package handlers

import (
	"context"
	"errors"
	"net/http"
	"strconv"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/labstack/echo/v4"
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/authn"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
	"github.com/open-sspm/open-sspm/internal/http/views"
)

type settingsUsersPageOptions struct {
	openAdd    bool
	openEdit   bool
	openDelete bool
	addForm    viewmodels.SettingsUsersForm
	editUserID int64
	editRole   string
	alert      *viewmodels.SettingsUsersAlert
}

func (h *Handlers) HandleSettingsUsers(c echo.Context) error {
	if c.Request().Method != http.MethodGet {
		return c.NoContent(http.StatusMethodNotAllowed)
	}

	open := strings.ToLower(strings.TrimSpace(c.QueryParam("open")))
	id, _ := parseInt64(c.QueryParam("id"))

	opts := settingsUsersPageOptions{
		openAdd:    open == "add",
		openEdit:   open == "edit",
		openDelete: open == "delete",
		editUserID: id,
	}

	return h.renderSettingsUsersPage(c, opts)
}

func (h *Handlers) HandleSettingsUsersCreate(c echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}

	email := auth.NormalizeEmail(c.FormValue("email"))
	role := strings.ToLower(strings.TrimSpace(c.FormValue("role")))
	password := c.FormValue("password")
	confirm := c.FormValue("confirm_password")

	form := viewmodels.SettingsUsersForm{
		Email: email,
		Role:  role,
	}

	if form.Email == "" {
		return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
			openAdd: true,
			addForm: form,
			alert: &viewmodels.SettingsUsersAlert{
				Title:       "Email required",
				Message:     "Provide an email address for the user.",
				Destructive: true,
			},
		})
	}

	switch form.Role {
	case auth.RoleAdmin, auth.RoleViewer:
	default:
		return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
			openAdd: true,
			addForm: form,
			alert: &viewmodels.SettingsUsersAlert{
				Title:       "Invalid group",
				Message:     "Group must be admin or viewer.",
				Destructive: true,
			},
		})
	}

	if strings.TrimSpace(password) == "" {
		return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
			openAdd: true,
			addForm: form,
			alert: &viewmodels.SettingsUsersAlert{
				Title:       "Password required",
				Message:     "Provide a password for the user.",
				Destructive: true,
			},
		})
	}

	if password != confirm {
		return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
			openAdd: true,
			addForm: form,
			alert: &viewmodels.SettingsUsersAlert{
				Title:       "Passwords do not match",
				Message:     "Confirm the password to continue.",
				Destructive: true,
			},
		})
	}

	if len(password) < 8 {
		return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
			openAdd: true,
			addForm: form,
			alert: &viewmodels.SettingsUsersAlert{
				Title:       "Password too short",
				Message:     "Use at least 8 characters.",
				Destructive: true,
			},
		})
	}

	hash, err := auth.HashPassword(password)
	if err != nil {
		return h.RenderError(c, err)
	}

	_, err = h.Q.CreateAuthUser(c.Request().Context(), gen.CreateAuthUserParams{
		Email:        form.Email,
		PasswordHash: hash,
		Role:         form.Role,
		IsActive:     true,
	})
	if err != nil {
		if isUniqueViolation(err) {
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openAdd: true,
				addForm: form,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "User already exists",
					Message:     "A user with that email address already exists.",
					Destructive: true,
				},
			})
		}
		return h.RenderError(c, err)
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "User created",
		Description: form.Email,
	})

	return c.Redirect(http.StatusSeeOther, "/settings/users")
}

func (h *Handlers) HandleSettingsUserUpdate(c echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}

	userID, ok := parseInt64(c.Param("id"))
	if !ok || userID <= 0 {
		return RenderNotFound(c)
	}

	principal, ok := authn.PrincipalFromContext(c)
	if !ok {
		return c.NoContent(http.StatusForbidden)
	}

	ctx := c.Request().Context()

	user, err := h.Q.GetAuthUser(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	role := strings.ToLower(strings.TrimSpace(c.FormValue("role")))
	password := c.FormValue("password")
	confirm := c.FormValue("confirm_password")

	if role != "" {
		switch role {
		case auth.RoleAdmin, auth.RoleViewer:
		default:
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openEdit:   true,
				editUserID: userID,
				editRole:   role,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "Invalid group",
					Message:     "Group must be admin or viewer.",
					Destructive: true,
				},
			})
		}
	}

	changeRole := role != "" && strings.ToLower(strings.TrimSpace(user.Role)) != role
	if changeRole {
		if principal.UserID == user.ID {
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openEdit:   true,
				editUserID: userID,
				editRole:   role,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "Group change not allowed",
					Message:     "You cannot change your own group.",
					Destructive: true,
				},
			})
		}

		if h.Pool == nil {
			return h.RenderError(c, errors.New("database pool not configured"))
		}

		tx, err := h.Pool.Begin(ctx)
		if err != nil {
			return h.RenderError(c, err)
		}
		defer tx.Rollback(ctx)

		qtx := h.Q.WithTx(tx)

		currentUser, err := qtx.GetAuthUser(ctx, userID)
		if err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				return RenderNotFound(c)
			}
			return h.RenderError(c, err)
		}

		adminIDs, err := qtx.ListActiveAuthAdminsForUpdate(ctx)
		if err != nil {
			return h.RenderError(c, err)
		}

		if currentUser.IsActive && strings.ToLower(strings.TrimSpace(currentUser.Role)) == auth.RoleAdmin && len(adminIDs) == 1 && role != auth.RoleAdmin {
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openEdit:   true,
				editUserID: userID,
				editRole:   role,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "Group change not allowed",
					Message:     "You cannot downgrade the last active admin.",
					Destructive: true,
				},
			})
		}

		if err := qtx.UpdateAuthUserRole(ctx, gen.UpdateAuthUserRoleParams{ID: userID, Role: role}); err != nil {
			return h.RenderError(c, err)
		}

		if err := tx.Commit(ctx); err != nil {
			return h.RenderError(c, err)
		}
	}

	changePassword := strings.TrimSpace(password) != "" || strings.TrimSpace(confirm) != ""
	if changePassword {
		if strings.TrimSpace(password) == "" {
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openEdit:   true,
				editUserID: userID,
				editRole:   role,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "Password required",
					Message:     "Provide a new password or leave both fields blank.",
					Destructive: true,
				},
			})
		}
		if password != confirm {
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openEdit:   true,
				editUserID: userID,
				editRole:   role,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "Passwords do not match",
					Message:     "Confirm the new password to continue.",
					Destructive: true,
				},
			})
		}
		if len(password) < 8 {
			return h.renderSettingsUsersPage(c, settingsUsersPageOptions{
				openEdit:   true,
				editUserID: userID,
				editRole:   role,
				alert: &viewmodels.SettingsUsersAlert{
					Title:       "Password too short",
					Message:     "Use at least 8 characters.",
					Destructive: true,
				},
			})
		}

		hash, err := auth.HashPassword(password)
		if err != nil {
			return h.RenderError(c, err)
		}
		if err := h.Q.UpdateAuthUserPasswordHash(ctx, gen.UpdateAuthUserPasswordHashParams{ID: userID, PasswordHash: hash}); err != nil {
			return h.RenderError(c, err)
		}
	}

	if !changeRole && !changePassword {
		setFlashToast(c, viewmodels.ToastViewData{
			Category: "info",
			Title:    "No changes",
		})
		return c.Redirect(http.StatusSeeOther, "/settings/users")
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "User updated",
		Description: strings.TrimSpace(user.Email),
	})
	return c.Redirect(http.StatusSeeOther, "/settings/users")
}

func (h *Handlers) HandleSettingsUserDelete(c echo.Context) error {
	if c.Request().Method != http.MethodPost {
		return c.NoContent(http.StatusMethodNotAllowed)
	}

	userID, ok := parseInt64(c.Param("id"))
	if !ok || userID <= 0 {
		return RenderNotFound(c)
	}

	principal, ok := authn.PrincipalFromContext(c)
	if !ok {
		return c.NoContent(http.StatusForbidden)
	}

	ctx := c.Request().Context()

	user, err := h.Q.GetAuthUser(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	if principal.UserID == user.ID {
		setFlashToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Delete not allowed",
			Description: "You cannot delete your own user.",
		})
		return c.Redirect(http.StatusSeeOther, "/settings/users")
	}

	if h.Pool == nil {
		return h.RenderError(c, errors.New("database pool not configured"))
	}

	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	defer tx.Rollback(ctx)

	qtx := h.Q.WithTx(tx)

	currentUser, err := qtx.GetAuthUser(ctx, userID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return RenderNotFound(c)
		}
		return h.RenderError(c, err)
	}

	adminIDs, err := qtx.ListActiveAuthAdminsForUpdate(ctx)
	if err != nil {
		return h.RenderError(c, err)
	}
	if currentUser.IsActive && strings.ToLower(strings.TrimSpace(currentUser.Role)) == auth.RoleAdmin && len(adminIDs) == 1 {
		setFlashToast(c, viewmodels.ToastViewData{
			Category:    "error",
			Title:       "Delete not allowed",
			Description: "You cannot delete the last active admin.",
		})
		return c.Redirect(http.StatusSeeOther, "/settings/users")
	}

	if err := qtx.DeleteAuthUser(ctx, userID); err != nil {
		return h.RenderError(c, err)
	}

	if err := tx.Commit(ctx); err != nil {
		return h.RenderError(c, err)
	}

	setFlashToast(c, viewmodels.ToastViewData{
		Category:    "success",
		Title:       "User deleted",
		Description: strings.TrimSpace(currentUser.Email),
	})
	return c.Redirect(http.StatusSeeOther, "/settings/users")
}

func (h *Handlers) renderSettingsUsersPage(c echo.Context, opts settingsUsersPageOptions) error {
	data, err := h.buildSettingsUsersViewData(c.Request().Context(), c, opts)
	if err != nil {
		return h.RenderError(c, err)
	}
	return h.RenderComponent(c, views.SettingsUsersPage(data))
}

func (h *Handlers) buildSettingsUsersViewData(ctx context.Context, c echo.Context, opts settingsUsersPageOptions) (viewmodels.SettingsUsersViewData, error) {
	layout, _, err := h.LayoutData(ctx, c, "Users")
	if err != nil {
		return viewmodels.SettingsUsersViewData{}, err
	}

	principal, _ := authn.PrincipalFromContext(c)

	adminCount, err := h.Q.CountAuthAdmins(ctx)
	if err != nil {
		return viewmodels.SettingsUsersViewData{}, err
	}

	rows, err := h.Q.ListAuthUsers(ctx)
	if err != nil {
		return viewmodels.SettingsUsersViewData{}, err
	}

	users := make([]viewmodels.SettingsUsersUserItem, 0, len(rows))
	for _, row := range rows {
		role := strings.ToLower(strings.TrimSpace(row.Role))
		isSelf := principal.UserID == row.ID
		isLastAdmin := row.IsActive && role == auth.RoleAdmin && adminCount == 1

		users = append(users, viewmodels.SettingsUsersUserItem{
			ID:          row.ID,
			Email:       strings.TrimSpace(row.Email),
			Role:        role,
			IsActive:    row.IsActive,
			IsSelf:      isSelf,
			IsLastAdmin: isLastAdmin,
			CanEditRole: !isSelf && !isLastAdmin,
			CanDelete:   !isSelf && !isLastAdmin,
		})
	}

	opts.addForm.Email = strings.TrimSpace(opts.addForm.Email)
	opts.addForm.Role = strings.ToLower(strings.TrimSpace(opts.addForm.Role))
	if opts.addForm.Role == "" {
		opts.addForm.Role = auth.RoleViewer
	}

	data := viewmodels.SettingsUsersViewData{
		Layout:     layout,
		Users:      users,
		HasUsers:   len(users) > 0,
		OpenAdd:    opts.openAdd,
		OpenEdit:   false,
		OpenDelete: false,
		Form:       opts.addForm,
		Alert:      opts.alert,
	}

	if opts.openEdit {
		if opts.editUserID <= 0 {
			if data.Alert == nil {
				data.Alert = &viewmodels.SettingsUsersAlert{
					Title:       "Invalid user",
					Message:     "Select a valid user to edit.",
					Destructive: true,
				}
			}
		} else if user, err := h.Q.GetAuthUser(ctx, opts.editUserID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if data.Alert == nil {
					data.Alert = &viewmodels.SettingsUsersAlert{
						Title:       "User not found",
						Message:     "That user no longer exists.",
						Destructive: true,
					}
				}
			} else {
				return viewmodels.SettingsUsersViewData{}, err
			}
		} else {
			role := strings.ToLower(strings.TrimSpace(user.Role))
			if opts.editRole != "" {
				role = strings.ToLower(strings.TrimSpace(opts.editRole))
			}

			roleDisabled := false
			roleDisabledReason := ""
			if principal.UserID == user.ID {
				roleDisabled = true
				roleDisabledReason = "You cannot change your own group."
			} else if user.IsActive && strings.ToLower(strings.TrimSpace(user.Role)) == auth.RoleAdmin && adminCount == 1 {
				roleDisabled = true
				roleDisabledReason = "You cannot change the group for the last active admin."
			}

			data.EditForm = viewmodels.SettingsUsersEditForm{
				ID:                 user.ID,
				Email:              strings.TrimSpace(user.Email),
				Role:               role,
				RoleDisabled:       roleDisabled,
				RoleDisabledReason: roleDisabledReason,
			}
			data.OpenEdit = true
		}
	}

	if opts.openDelete {
		userID := opts.editUserID
		if userID <= 0 {
			if data.Alert == nil {
				data.Alert = &viewmodels.SettingsUsersAlert{
					Title:       "Invalid user",
					Message:     "Select a valid user to delete.",
					Destructive: true,
				}
			}
		} else if user, err := h.Q.GetAuthUser(ctx, userID); err != nil {
			if errors.Is(err, pgx.ErrNoRows) {
				if data.Alert == nil {
					data.Alert = &viewmodels.SettingsUsersAlert{
						Title:       "User not found",
						Message:     "That user no longer exists.",
						Destructive: true,
					}
				}
			} else {
				return viewmodels.SettingsUsersViewData{}, err
			}
		} else {
			canDelete := true
			if principal.UserID == user.ID {
				canDelete = false
				if data.Alert == nil {
					data.Alert = &viewmodels.SettingsUsersAlert{
						Title:       "Delete not allowed",
						Message:     "You cannot delete your own user.",
						Destructive: true,
					}
				}
			} else if user.IsActive && strings.ToLower(strings.TrimSpace(user.Role)) == auth.RoleAdmin && adminCount == 1 {
				canDelete = false
				if data.Alert == nil {
					data.Alert = &viewmodels.SettingsUsersAlert{
						Title:       "Delete not allowed",
						Message:     "You cannot delete the last active admin.",
						Destructive: true,
					}
				}
			}

			if canDelete {
				data.Delete = viewmodels.SettingsUsersDeleteViewData{
					ID:    user.ID,
					Email: strings.TrimSpace(user.Email),
				}
				data.OpenDelete = true
			}
		}
	}

	return data, nil
}

func isUniqueViolation(err error) bool {
	var pgErr *pgconn.PgError
	return errors.As(err, &pgErr) && pgErr.Code == "23505"
}

func parseInt64(raw string) (int64, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return 0, false
	}
	n, err := strconv.ParseInt(raw, 10, 64)
	if err != nil {
		return 0, false
	}
	return n, true
}
