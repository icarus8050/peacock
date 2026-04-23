package handler

import (
	"errors"

	"github.com/gofiber/fiber/v2"

	"peacock/kv"
)

func registerKV(app *fiber.App, store *kv.Store) {
	app.Get("/kv/:key", getKV(store))
	app.Put("/kv/:key", putKV(store))
	app.Delete("/kv/:key", deleteKV(store))
}

func getKV(store *kv.Store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		value, err := store.Get(key)
		if errors.Is(err, kv.ErrNotFound) {
			return fiber.ErrNotFound
		}
		if err != nil {
			return err
		}
		c.Set(fiber.HeaderContentType, fiber.MIMEOctetStream)
		return c.Send(value)
	}
}

func putKV(store *kv.Store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		if err := store.Put(key, c.Body()); err != nil {
			return err
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}

func deleteKV(store *kv.Store) fiber.Handler {
	return func(c *fiber.Ctx) error {
		key := c.Params("key")
		if err := store.Delete(key); err != nil {
			return err
		}
		return c.SendStatus(fiber.StatusNoContent)
	}
}
