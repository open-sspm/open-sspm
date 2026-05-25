package handlers

import (
	"context"
	"errors"
	"net/http"

	"github.com/labstack/echo/v5"
	"github.com/open-sspm/open-sspm/internal/db/gen"
	"github.com/open-sspm/open-sspm/internal/http/viewmodels"
)

func destructiveAlert(title, message string) *viewmodels.AlertViewData {
	return &viewmodels.AlertViewData{
		Title:       title,
		Message:     message,
		Destructive: true,
	}
}

func redirectWithFlash(c *echo.Context, path string, toast viewmodels.ToastViewData) error {
	setResponseToast(c, toast)
	if isHX(c) {
		if isHXBoosted(c) {
			setHXLocation(c, path)
			return c.NoContent(http.StatusOK)
		}
		setHXRedirect(c, path)
		return c.NoContent(http.StatusOK)
	}
	return c.Redirect(http.StatusSeeOther, path)
}

func (h *Handlers) WithTx(ctx context.Context, fn func(*gen.Queries) error) error {
	if h.Pool == nil {
		return errors.New("database pool not configured")
	}
	if h.Q == nil {
		return errors.New("database queries not configured")
	}
	tx, err := h.Pool.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		_ = tx.Rollback(ctx)
	}()

	if err := fn(h.Q.WithTx(tx)); err != nil {
		return err
	}
	return tx.Commit(ctx)
}
