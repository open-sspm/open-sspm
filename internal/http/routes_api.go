package httpapp

import (
	"github.com/open-sspm/open-sspm/internal/auth"
	"github.com/open-sspm/open-sspm/internal/http/authn"
)

func (es *EchoServer) registerAPIRoutes() {
	api := es.e.Group("/api")
	api.Use(es.browserMiddleware()...)
	api.Use(authn.RequireAuth(es.h.Sessions, es.h.Q))
	api.Use(authn.RequireRole(auth.RoleAdmin))

	// PHASE-TWO-DELETE: these API-prefixed admin endpoints are still HTMX/browser fragment endpoints; replace with explicit machine/API handlers or move paths in Phase Two.
	api.GET("/identity-resolution/candidates", es.h.HandleIdentityResolutionCandidates)
	api.GET("/identity-resolution/candidates/:id", es.h.HandleIdentityResolutionCandidateDetail)
	api.GET("/identities/:id/emails", es.h.HandleIdentityEmails)
	api.GET("/identities/:id/anchors", es.h.HandleIdentityAnchors)
	api.POST("/identity-resolution/candidates/:id/accept", es.h.HandleIdentityResolutionCandidateAccept)
	api.POST("/identity-resolution/candidates/:id/reject", es.h.HandleIdentityResolutionCandidateReject)
	api.POST("/identity-resolution/candidates/:id/mark-service", es.h.HandleIdentityResolutionCandidateMarkService)
	api.POST("/identity-resolution/candidates/:id/mark-shared", es.h.HandleIdentityResolutionCandidateMarkShared)
	api.POST("/identities/:id/emails", es.h.HandleIdentityEmailUpsert)
	api.POST("/identities/:id/anchors", es.h.HandleIdentityAnchorUpsert)
}
