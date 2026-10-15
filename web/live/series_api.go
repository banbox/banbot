package live

import (
	"github.com/banbox/banbot/web/base"
	"github.com/gofiber/fiber/v2"
)

// Dashboard data reads use the same authenticated account scope as bot state.
func (h *apiHandlers) regApiSeries(api fiber.Router) {
	routes := api.Group("/kline", func(c *fiber.Ctx) error {
		return wrapAccount(c, func(_ string) error { return c.Next() })
	})
	if h.runtime() {
		base.RegApiSeriesWithRuntimeDeps(routes, *h.deps.DataDeps())
	} else {
		base.RegApiSeries(routes)
	}
}
