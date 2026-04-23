package handler

import (
	"github.com/gofiber/fiber/v2"

	"peacock/kv"
)

func Register(app *fiber.App, store *kv.Store) {
	registerHealth(app)
	registerKV(app, store)
}
