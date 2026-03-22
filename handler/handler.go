package handler

import (
	"github.com/gofiber/fiber/v2"
)

func Register(app *fiber.App) {
	registerHealth(app)
}
