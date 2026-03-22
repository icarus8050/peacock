package handler

import (
	"github.com/gofiber/fiber/v2"
)

func registerHealth(app *fiber.App) {
	app.Get("/health", liveness)
	app.Get("/ready", readiness)
}

func liveness(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ok"})
}

func readiness(c *fiber.Ctx) error {
	return c.JSON(fiber.Map{"status": "ready"})
}
