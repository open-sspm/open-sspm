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
	setFlashToast(c, toast)
	return c.Redirect(http.StatusSeeOther, path)
}

func (h *Handlers) WithTx(ctx context.Context, fn func(*gen.Queries) error) error {
	if h.Pool == nil {
		return errors.New("database pool not configured")
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
