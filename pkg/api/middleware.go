package api

import (
	"context"
	"strings"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
	"replicated-clob/pkg/replica"
)

func requestIDMiddleware(c *fiber.Ctx) error {
	requestID := strings.TrimSpace(c.Get(replica.RequestIDHeader))
	if requestID == "" {
		requestID = uuid.NewString()
	}

	ctx := context.WithValue(c.UserContext(), replica.RequestIDContextKey, requestID)
	c.SetUserContext(ctx)
	c.Set(replica.RequestIDHeader, requestID)

	return c.Next()
}
