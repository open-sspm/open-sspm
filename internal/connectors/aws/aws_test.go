package aws

import (
	"context"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/identitystore"
	identitystoretypes "github.com/aws/aws-sdk-go-v2/service/identitystore/types"
	"github.com/aws/aws-sdk-go-v2/service/ssoadmin"
	ssoadmintypes "github.com/aws/aws-sdk-go-v2/service/ssoadmin/types"
)

type fakeIdentityStore struct {
	usersPages       [][]identitystoretypes.User
	groupsPages      [][]identitystoretypes.Group
	listUsersCalls   int
	listGroupsCalls  int
	groupMemberships map[string][]string
}

func (f *fakeIdentityStore) ListUsers(ctx context.Context, in *identitystore.ListUsersInput, optFns ...func(*identitystore.Options)) (*identitystore.ListUsersOutput, error) {
	_ = ctx
	_ = in
	_ = optFns
	idx := f.listUsersCalls
	f.listUsersCalls++
	if idx >= len(f.usersPages) {
		return &identitystore.ListUsersOutput{Users: []identitystoretypes.User{}}, nil
	}
	out := &identitystore.ListUsersOutput{Users: f.usersPages[idx]}
	if idx < len(f.usersPages)-1 {
		out.NextToken = aws.String("next")
	}
	return out, nil
}

func (f *fakeIdentityStore) ListGroupMemberships(ctx context.Context, in *identitystore.ListGroupMembershipsInput, optFns ...func(*identitystore.Options)) (*identitystore.ListGroupMembershipsOutput, error) {
	_ = ctx
	_ = optFns
	groupID := aws.ToString(in.GroupId)
	members := f.groupMemberships[groupID]
	out := &identitystore.ListGroupMembershipsOutput{}
	for _, userID := range members {
		out.GroupMemberships = append(out.GroupMemberships, identitystoretypes.GroupMembership{
			GroupId: in.GroupId,
			MemberId: &identitystoretypes.MemberIdMemberUserId{
				Value: userID,
			},
		})
	}
	return out, nil
}

func (f *fakeIdentityStore) ListGroups(ctx context.Context, in *identitystore.ListGroupsInput, optFns ...func(*identitystore.Options)) (*identitystore.ListGroupsOutput, error) {
	_ = ctx
	_ = in
	_ = optFns
	idx := f.listGroupsCalls
	f.listGroupsCalls++
	if idx >= len(f.groupsPages) {
		return &identitystore.ListGroupsOutput{Groups: []identitystoretypes.Group{}}, nil
	}
	out := &identitystore.ListGroupsOutput{Groups: f.groupsPages[idx]}
	if idx < len(f.groupsPages)-1 {
		out.NextToken = aws.String("next")
	}
	return out, nil
}

type fakeSSOAdmin struct {
	permissionSets           []string
	permissionSetNames       map[string]string
	accountsForPermissionSet map[string][]string
	assignments              map[string][]ssoadmintypes.AccountAssignment
}

func (f *fakeSSOAdmin) ListInstances(ctx context.Context, in *ssoadmin.ListInstancesInput, optFns ...func(*ssoadmin.Options)) (*ssoadmin.ListInstancesOutput, error) {
	_ = ctx
	_ = in
	_ = optFns
	return &ssoadmin.ListInstancesOutput{}, nil
}

func (f *fakeSSOAdmin) ListPermissionSets(ctx context.Context, in *ssoadmin.ListPermissionSetsInput, optFns ...func(*ssoadmin.Options)) (*ssoadmin.ListPermissionSetsOutput, error) {
	_ = ctx
	_ = in
	_ = optFns
	return &ssoadmin.ListPermissionSetsOutput{PermissionSets: f.permissionSets}, nil
}

func (f *fakeSSOAdmin) DescribePermissionSet(ctx context.Context, in *ssoadmin.DescribePermissionSetInput, optFns ...func(*ssoadmin.Options)) (*ssoadmin.DescribePermissionSetOutput, error) {
	_ = ctx
	_ = optFns
	name := f.permissionSetNames[aws.ToString(in.PermissionSetArn)]
	return &ssoadmin.DescribePermissionSetOutput{
		PermissionSet: &ssoadmintypes.PermissionSet{
			Name: aws.String(name),
		},
	}, nil
}

func (f *fakeSSOAdmin) ListAccountsForProvisionedPermissionSet(ctx context.Context, in *ssoadmin.ListAccountsForProvisionedPermissionSetInput, optFns ...func(*ssoadmin.Options)) (*ssoadmin.ListAccountsForProvisionedPermissionSetOutput, error) {
	_ = ctx
	_ = optFns
	accounts := f.accountsForPermissionSet[aws.ToString(in.PermissionSetArn)]
	return &ssoadmin.ListAccountsForProvisionedPermissionSetOutput{AccountIds: accounts}, nil
}

func (f *fakeSSOAdmin) ListAccountAssignments(ctx context.Context, in *ssoadmin.ListAccountAssignmentsInput, optFns ...func(*ssoadmin.Options)) (*ssoadmin.ListAccountAssignmentsOutput, error) {
	_ = ctx
	_ = optFns
	key := aws.ToString(in.AccountId) + "|" + aws.ToString(in.PermissionSetArn)
	assignments := f.assignments[key]
	return &ssoadmin.ListAccountAssignmentsOutput{AccountAssignments: assignments}, nil
}

func TestListUsersPagination(t *testing.T) {
	identity := &fakeIdentityStore{
		usersPages: [][]identitystoretypes.User{
			{
				{
					UserId:      aws.String("u1"),
					UserName:    aws.String("user1"),
					DisplayName: aws.String(""),
					Emails: []identitystoretypes.Email{
						{Value: aws.String("user1@example.com")},
					},
				},
			},
			{
				{
					UserId:      aws.String("u2"),
					UserName:    aws.String("user2"),
					DisplayName: aws.String("User Two"),
				},
			},
		},
	}

	client := &Client{
		identityStoreID: "d-123",
		identitystore:   identity,
	}

	users, err := client.ListUsers(context.Background())
	if err != nil {
		t.Fatalf("ListUsers error: %v", err)
	}
	if len(users) != 2 {
		t.Fatalf("expected 2 users, got %d", len(users))
	}
	if users[0].Email != "user1@example.com" {
		t.Fatalf("expected first email to be user1@example.com, got %q", users[0].Email)
	}
	if users[0].DisplayName != "user1" {
		t.Fatalf("expected first display name user1, got %q", users[0].DisplayName)
	}
	if users[1].DisplayName != "User Two" {
		t.Fatalf("expected second display name User Two, got %q", users[1].DisplayName)
	}
}

func TestListUserEntitlementsGroupExpansion(t *testing.T) {
	sso := &fakeSSOAdmin{
		permissionSets: []string{"ps-1", "ps-2"},
		permissionSetNames: map[string]string{
			"ps-1": "Admin",
			"ps-2": "ReadOnly",
		},
		accountsForPermissionSet: map[string][]string{
			"ps-1": {"111111111111"},
			"ps-2": {"222222222222"},
		},
		assignments: map[string][]ssoadmintypes.AccountAssignment{
			"111111111111|ps-1": {
				{
					PrincipalId:   aws.String("u1"),
					PrincipalType: ssoadmintypes.PrincipalTypeUser,
				},
				{
					PrincipalId:   aws.String("g1"),
					PrincipalType: ssoadmintypes.PrincipalTypeGroup,
				},
			},
			"222222222222|ps-2": {
				{
					PrincipalId:   aws.String("g1"),
					PrincipalType: ssoadmintypes.PrincipalTypeGroup,
				},
			},
		},
	}

	identity := &fakeIdentityStore{
		groupMemberships: map[string][]string{
			"g1": {"u1", "u2"},
		},
	}

	client := &Client{
		instanceArn:     "arn:aws:sso:::instance/ssoins-123",
		identityStoreID: "d-123",
		ssoadmin:        sso,
		identitystore:   identity,
	}

	entitlements, err := client.ListUserEntitlements(context.Background())
	if err != nil {
		t.Fatalf("ListUserEntitlements error: %v", err)
	}

	got := make(map[string]struct{})
	for userID, facts := range entitlements {
		for _, fact := range facts {
			key := userID + "|" + fact.AccountID + "|" + fact.PermissionSetName + "|" + string(fact.AssignmentSource) + "|" + fact.GroupID
			got[key] = struct{}{}
		}
	}

	want := []string{
		"u1|111111111111|Admin|direct|",
		"u1|111111111111|Admin|group|g1",
		"u1|222222222222|ReadOnly|group|g1",
		"u2|111111111111|Admin|group|g1",
		"u2|222222222222|ReadOnly|group|g1",
	}
	for _, key := range want {
		if _, ok := got[key]; !ok {
			t.Fatalf("missing entitlement %q", key)
		}
	}
	if len(got) != len(want) {
		t.Fatalf("expected %d entitlements, got %d", len(want), len(got))
	}
}

func TestListGroupsPagination(t *testing.T) {
	identity := &fakeIdentityStore{
		groupsPages: [][]identitystoretypes.Group{
			{
				{
					GroupId:     aws.String("g1"),
					DisplayName: aws.String("Admins"),
				},
			},
			{
				{
					GroupId: aws.String("g2"),
				},
			},
		},
	}

	client := &Client{
		identityStoreID: "d-123",
		identitystore:   identity,
	}

	groups, err := client.ListGroups(context.Background())
	if err != nil {
		t.Fatalf("ListGroups error: %v", err)
	}
	if len(groups) != 2 {
		t.Fatalf("expected 2 groups, got %d", len(groups))
	}
	if groups[0].ID != "g1" || groups[0].DisplayName != "Admins" {
		t.Fatalf("unexpected groups[0]=%+v", groups[0])
	}
	if groups[1].ID != "g2" || groups[1].DisplayName != "g2" {
		t.Fatalf("unexpected groups[1]=%+v", groups[1])
	}
}
