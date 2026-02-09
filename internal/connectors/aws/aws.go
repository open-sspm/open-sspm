package aws

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/identitystore"
	identitystoretypes "github.com/aws/aws-sdk-go-v2/service/identitystore/types"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
)

const defaultHTTPTimeout = 120 * time.Second

// AssignmentSource describes how an entitlement was granted.
type AssignmentSource string

const (
	AssignmentSourceDirect AssignmentSource = "direct"
	AssignmentSourceGroup  AssignmentSource = "group"
)

// User is a normalized IAM Identity Center user.
type User struct {
	ID          string
	Email       string
	DisplayName string
	RawJSON     []byte
}

// EntitlementFact represents a single account + permission set assignment for a user.
type EntitlementFact struct {
	AccountID         string
	PermissionSetName string
	PermissionSetArn  string
	AssignmentSource  AssignmentSource
	GroupID           string
}

// Options configure the AWS IAM Identity Center connector.
type Options struct {
	Region          string
	InstanceArn     string
	IdentityStoreID string
	AuthType        string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
}

type Client struct {
	region          string
	instanceArn     string
	identityStoreID string

	ssoadmin      ssoAdminAPI
	identitystore identityStoreAPI
}

type ssoAdminAPI interface {
	ListInstances(context.Context, *ssoadmin.ListInstancesInput, ...func(*ssoadmin.Options)) (*ssoadmin.ListInstancesOutput, error)
	ListPermissionSets(context.Context, *ssoadmin.ListPermissionSetsInput, ...func(*ssoadmin.Options)) (*ssoadmin.ListPermissionSetsOutput, error)
	DescribePermissionSet(context.Context, *ssoadmin.DescribePermissionSetInput, ...func(*ssoadmin.Options)) (*ssoadmin.DescribePermissionSetOutput, error)
	ListAccountsForProvisionedPermissionSet(context.Context, *ssoadmin.ListAccountsForProvisionedPermissionSetInput, ...func(*ssoadmin.Options)) (*ssoadmin.ListAccountsForProvisionedPermissionSetOutput, error)
	ListAccountAssignments(context.Context, *ssoadmin.ListAccountAssignmentsInput, ...func(*ssoadmin.Options)) (*ssoadmin.ListAccountAssignmentsOutput, error)
}

type identityStoreAPI interface {
	ListUsers(context.Context, *identitystore.ListUsersInput, ...func(*identitystore.Options)) (*identitystore.ListUsersOutput, error)
	ListGroupMemberships(context.Context, *identitystore.ListGroupMembershipsInput, ...func(*identitystore.Options)) (*identitystore.ListGroupMembershipsOutput, error)
}

func New(ctx context.Context, opts Options) (*Client, error) {
	region := strings.TrimSpace(opts.Region)
	if region == "" {
		return nil, errors.New("aws identity center region is required")
	}

	authType := strings.ToLower(strings.TrimSpace(opts.AuthType))
	switch authType {
	case "", "default_chain":
		authType = "default_chain"
	case "access_key":
		accessKeyID := strings.TrimSpace(opts.AccessKeyID)
		secretAccessKey := strings.TrimSpace(opts.SecretAccessKey)
		if accessKeyID == "" || secretAccessKey == "" {
			return nil, errors.New("aws access key id and secret access key are required")
		}
	default:
		return nil, errors.New("unsupported aws credential auth type")
	}

	loadOpts := []func(*config.LoadOptions) error{
		config.WithRegion(region),
		config.WithHTTPClient(&http.Client{Timeout: defaultHTTPTimeout}),
	}
	if authType == "access_key" {
		loadOpts = append(loadOpts, config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(
			strings.TrimSpace(opts.AccessKeyID),
			strings.TrimSpace(opts.SecretAccessKey),
			strings.TrimSpace(opts.SessionToken),
		)))
	}

	cfg, err := config.LoadDefaultConfig(ctx, loadOpts...)
	if err != nil {
		return nil, err
	}
	return NewWithConfig(cfg, opts)
}

func NewWithConfig(cfg aws.Config, opts Options) (*Client, error) {
	return NewWithClients(opts, ssoadmin.NewFromConfig(cfg), identitystore.NewFromConfig(cfg))
}

func NewWithClients(opts Options, sso ssoAdminAPI, identity identityStoreAPI) (*Client, error) {
	region := strings.TrimSpace(opts.Region)
	if region == "" {
		return nil, errors.New("aws identity center region is required")
	}
	return &Client{
		region:          region,
		instanceArn:     strings.TrimSpace(opts.InstanceArn),
		identityStoreID: strings.TrimSpace(opts.IdentityStoreID),
		ssoadmin:        sso,
		identitystore:   identity,
	}, nil
}

func (c *Client) ListUsers(ctx context.Context) ([]User, error) {
	if err := c.ensureIdentityStore(ctx); err != nil {
		return nil, err
	}

	var out []User
	var token *string
	for {
		resp, err := c.identitystore.ListUsers(ctx, &identitystore.ListUsersInput{
			IdentityStoreId: aws.String(c.identityStoreID),
			NextToken:       token,
		})
		if err != nil {
			return nil, err
		}

		for _, u := range resp.Users {
			userID := strings.TrimSpace(aws.ToString(u.UserId))
			email := firstNonEmptyEmail(u.Emails)
			display := strings.TrimSpace(aws.ToString(u.DisplayName))
			if display == "" {
				display = strings.TrimSpace(aws.ToString(u.UserName))
			}
			if display == "" {
				display = userID
			}
			out = append(out, User{
				ID:          userID,
				Email:       email,
				DisplayName: display,
				RawJSON:     marshalJSON(u),
			})
		}

		if resp.NextToken == nil || aws.ToString(resp.NextToken) == "" {
			break
		}
		token = resp.NextToken
	}
	return out, nil
}

func (c *Client) ListUserEntitlements(ctx context.Context) (map[string][]EntitlementFact, error) {
	if err := c.ensureSSOAdmin(ctx); err != nil {
		return nil, err
	}
	if err := c.ensureIdentityStore(ctx); err != nil {
		return nil, err
	}

	permissionSets, err := c.listPermissionSets(ctx)
	if err != nil {
		return nil, err
	}

	entitlements := make(map[string][]EntitlementFact)
	groupCache := make(map[string][]string)

	for _, ps := range permissionSets {
		accounts, err := c.listAccountsForPermissionSet(ctx, ps.Arn)
		if err != nil {
			return nil, err
		}
		for _, accountID := range accounts {
			assignments, err := c.listAccountAssignments(ctx, accountID, ps.Arn)
			if err != nil {
				return nil, err
			}
			for _, assignment := range assignments {
				principalID := strings.TrimSpace(aws.ToString(assignment.PrincipalId))
				if principalID == "" {
					continue
				}
				switch assignment.PrincipalType {
				case ssoadmintypes.PrincipalTypeUser:
					entitlements[principalID] = append(entitlements[principalID], EntitlementFact{
						AccountID:         accountID,
						PermissionSetName: ps.Name,
						PermissionSetArn:  ps.Arn,
						AssignmentSource:  AssignmentSourceDirect,
					})
				case ssoadmintypes.PrincipalTypeGroup:
					groupID := principalID
					members, ok := groupCache[groupID]
					if !ok {
						members, err = c.listGroupMembers(ctx, groupID)
						if err != nil {
							return nil, err
						}
						groupCache[groupID] = members
					}
					for _, userID := range members {
						entitlements[userID] = append(entitlements[userID], EntitlementFact{
							AccountID:         accountID,
							PermissionSetName: ps.Name,
							PermissionSetArn:  ps.Arn,
							AssignmentSource:  AssignmentSourceGroup,
							GroupID:           groupID,
						})
					}
				}
			}
		}
	}

	return entitlements, nil
}

type permissionSet struct {
	Arn  string
	Name string
}

func (c *Client) listPermissionSets(ctx context.Context) ([]permissionSet, error) {
	var out []permissionSet
	var token *string
	for {
		resp, err := c.ssoadmin.ListPermissionSets(ctx, &ssoadmin.ListPermissionSetsInput{
			InstanceArn: aws.String(c.instanceArn),
			NextToken:   token,
		})
		if err != nil {
			return nil, err
		}
		for _, arn := range resp.PermissionSets {
			arn = strings.TrimSpace(arn)
			if arn == "" {
				continue
			}
			name, err := c.describePermissionSetName(ctx, arn)
			if err != nil {
				return nil, err
			}
			out = append(out, permissionSet{Arn: arn, Name: name})
		}
		if resp.NextToken == nil || aws.ToString(resp.NextToken) == "" {
			break
		}
		token = resp.NextToken
	}
	return out, nil
}

func (c *Client) describePermissionSetName(ctx context.Context, arn string) (string, error) {
	resp, err := c.ssoadmin.DescribePermissionSet(ctx, &ssoadmin.DescribePermissionSetInput{
		InstanceArn:      aws.String(c.instanceArn),
		PermissionSetArn: aws.String(arn),
	})
	if err != nil {
		return "", err
	}
	if resp.PermissionSet == nil {
		return "", nil
	}
	return strings.TrimSpace(aws.ToString(resp.PermissionSet.Name)), nil
}

func (c *Client) listAccountsForPermissionSet(ctx context.Context, permissionSetArn string) ([]string, error) {
	var out []string
	var token *string
	for {
		resp, err := c.ssoadmin.ListAccountsForProvisionedPermissionSet(ctx, &ssoadmin.ListAccountsForProvisionedPermissionSetInput{
			InstanceArn:      aws.String(c.instanceArn),
			PermissionSetArn: aws.String(permissionSetArn),
			NextToken:        token,
		})
		if err != nil {
			return nil, err
		}
		for _, accountID := range resp.AccountIds {
			accountID = strings.TrimSpace(accountID)
			if accountID == "" {
				continue
			}
			out = append(out, accountID)
		}
		if resp.NextToken == nil || aws.ToString(resp.NextToken) == "" {
			break
		}
		token = resp.NextToken
	}
	return out, nil
}

func (c *Client) listAccountAssignments(ctx context.Context, accountID, permissionSetArn string) ([]ssoadmintypes.AccountAssignment, error) {
	var out []ssoadmintypes.AccountAssignment
	var token *string
	for {
		resp, err := c.ssoadmin.ListAccountAssignments(ctx, &ssoadmin.ListAccountAssignmentsInput{
			InstanceArn:      aws.String(c.instanceArn),
			AccountId:        aws.String(accountID),
			PermissionSetArn: aws.String(permissionSetArn),
			NextToken:        token,
		})
		if err != nil {
			return nil, err
		}
		out = append(out, resp.AccountAssignments...)
		if resp.NextToken == nil || aws.ToString(resp.NextToken) == "" {
			break
		}
		token = resp.NextToken
	}
	return out, nil
}

func (c *Client) listGroupMembers(ctx context.Context, groupID string) ([]string, error) {
	var out []string
	var token *string
	for {
		resp, err := c.identitystore.ListGroupMemberships(ctx, &identitystore.ListGroupMembershipsInput{
			IdentityStoreId: aws.String(c.identityStoreID),
			GroupId:         aws.String(groupID),
			NextToken:       token,
		})
		if err != nil {
			return nil, err
		}
		for _, membership := range resp.GroupMemberships {
			if membership.MemberId == nil {
				continue
			}
			var userID string
			switch member := membership.MemberId.(type) {
			case *identitystoretypes.MemberIdMemberUserId:
				userID = strings.TrimSpace(member.Value)
			default:
				continue
			}
			if userID == "" {
				continue
			}
			out = append(out, userID)
		}
		if resp.NextToken == nil || aws.ToString(resp.NextToken) == "" {
			break
		}
		token = resp.NextToken
	}
	return out, nil
}

func (c *Client) ensureIdentityStore(ctx context.Context) error {
	if c.identitystore == nil {
		return errors.New("aws identitystore client is required")
	}
	if strings.TrimSpace(c.identityStoreID) != "" {
		return nil
	}
	return c.resolveInstance(ctx)
}

func (c *Client) ensureSSOAdmin(ctx context.Context) error {
	if c.ssoadmin == nil {
		return errors.New("aws ssoadmin client is required")
	}
	if strings.TrimSpace(c.instanceArn) != "" && strings.TrimSpace(c.identityStoreID) != "" {
		return nil
	}
	return c.resolveInstance(ctx)
}

func (c *Client) resolveInstance(ctx context.Context) error {
	if c.ssoadmin == nil {
		return errors.New("aws ssoadmin client is required to discover identity center instance")
	}
	instances, err := c.listInstances(ctx)
	if err != nil {
		return err
	}
	if len(instances) == 0 {
		return errors.New("no aws identity center instances found")
	}

	if c.instanceArn != "" {
		for _, inst := range instances {
			if aws.ToString(inst.InstanceArn) == c.instanceArn {
				if c.identityStoreID == "" {
					c.identityStoreID = aws.ToString(inst.IdentityStoreId)
				}
				break
			}
		}
		if c.identityStoreID == "" {
			return fmt.Errorf("aws identity center instance %s not found", c.instanceArn)
		}
		return nil
	}

	if c.identityStoreID != "" {
		for _, inst := range instances {
			if aws.ToString(inst.IdentityStoreId) == c.identityStoreID {
				c.instanceArn = aws.ToString(inst.InstanceArn)
				break
			}
		}
		if c.instanceArn == "" {
			return fmt.Errorf("aws identity center identity store %s not found", c.identityStoreID)
		}
		return nil
	}

	if len(instances) > 1 {
		return errors.New("multiple aws identity center instances found; set instance ARN and identity store ID")
	}
	inst := instances[0]
	c.instanceArn = aws.ToString(inst.InstanceArn)
	c.identityStoreID = aws.ToString(inst.IdentityStoreId)
	if c.instanceArn == "" || c.identityStoreID == "" {
		return errors.New("aws identity center instance metadata missing InstanceArn or IdentityStoreId")
	}
	return nil
}

func (c *Client) listInstances(ctx context.Context) ([]ssoadmintypes.InstanceMetadata, error) {
	var out []ssoadmintypes.InstanceMetadata
	var token *string
	for {
		resp, err := c.ssoadmin.ListInstances(ctx, &ssoadmin.ListInstancesInput{NextToken: token})
		if err != nil {
			return nil, fmt.Errorf("list aws identity center instances: %w", err)
		}
		out = append(out, resp.Instances...)
		if resp.NextToken == nil || aws.ToString(resp.NextToken) == "" {
			break
		}
		token = resp.NextToken
	}
	return out, nil
}

func firstNonEmptyEmail(emails []identitystoretypes.Email) string {
	for _, email := range emails {
		value := strings.TrimSpace(aws.ToString(email.Value))
		if value != "" {
			return value
		}
	}
	return ""
}

func marshalJSON(v any) []byte {
	b, err := json.Marshal(v)
	if err != nil {
		return []byte("{}")
	}
	return b
}
