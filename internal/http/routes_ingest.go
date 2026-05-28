package httpapp

import "github.com/labstack/echo/v5/middleware"

func (es *EchoServer) registerIngestRoutes() {
	ingest := es.e.Group("/ingest")
	ingest.Use(middleware.BodyLimit(1 << 20))

	ingest.GET("/okta/events", es.h.HandleOktaEventHookVerify)
	ingest.POST("/okta/events", es.h.HandleOktaEventHookPost)
	ingest.POST("/okta/eventbridge", es.h.HandleOktaEventBridgePost)
}
