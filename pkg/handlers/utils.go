package handlers

import (
	"github.com/gofiber/fiber/v2"
)

func jsonResponse(c *fiber.Ctx, status int, payload interface{}) error {
	return c.Status(status).JSON(payload)
}

func badRequest(c *fiber.Ctx, err error) error {
	return jsonResponse(c, fiber.StatusBadRequest, fiber.Map{
		"error": err.Error(),
	})
}

func notFound(c *fiber.Ctx, err error) error {
	return jsonResponse(c, fiber.StatusNotFound, fiber.Map{
		"error": err.Error(),
	})
}

func internalServerError(c *fiber.Ctx) error {
	return jsonResponse(c, fiber.StatusInternalServerError, fiber.Map{
		"error": "Something went wrong",
	})
}

func temporaryRedirect(c *fiber.Ctx, leader string) error {
	response := fiber.Map{
		"error": "not leader",
	}
	if leader != "" {
		response["leader"] = leader
	}
	return jsonResponse(c, fiber.StatusTemporaryRedirect, response)
}

func temporaryUnavailable(c *fiber.Ctx, err error) error {
	return jsonResponse(c, fiber.StatusServiceUnavailable, fiber.Map{
		"error": err.Error(),
	})
}

func conflict(c *fiber.Ctx, required int64, applied int64) error {
	return jsonResponse(c, fiber.StatusConflict, fiber.Map{
		"error":    "read not yet consistent",
		"required": required,
		"applied":  applied,
	})
}

func success(c *fiber.Ctx) error {
	return jsonResponse(c, fiber.StatusOK, fiber.Map{
		"message": "Success",
	})
}
