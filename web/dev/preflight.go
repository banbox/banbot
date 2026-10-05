package dev

import (
	"encoding/json"
	"os"
	"slices"
	"sort"

	"github.com/banbox/banbot/config"
	"github.com/banbox/banbot/factor/runner"
	"github.com/banbox/banbot/orm/ormu"
	"github.com/banbox/banbot/web/base"
	"github.com/gofiber/fiber/v2"
)

type StrategyInspection struct {
	Engine    string                   `json:"engine"`
	Name      string                   `json:"name"`
	ID        string                   `json:"id"`
	Account   string                   `json:"account"`
	TimeFrame string                   `json:"timeframe,omitempty"`
	Resolved  map[string]ResolvedValue `json:"resolved,omitempty"`
}

type ResolvedValue struct {
	Value  any                `json:"value"`
	Origin config.FieldOrigin `json:"origin"`
}

type PreflightIssue struct {
	Code    string `json:"code"`
	Message string `json:"message"`
}

// Static checks never claim that data, PIT evidence or a live binding is ready.
type BacktestInspection struct {
	Version         int                           `json:"version"`
	Engines         []string                      `json:"engines"`
	ExecutionMode   string                        `json:"execution_mode"`
	DataChecked     bool                          `json:"data_checked"`
	Strategies      []StrategyInspection          `json:"strategies"`
	Warnings        []PreflightIssue              `json:"warnings"`
	Origins         map[string]config.FieldOrigin `json:"origins"`
	EffectiveConfig string                        `json:"effective_config"`
}

func getStrategyCatalog(c *fiber.Ctx) error {
	definitions, portfolios := runner.DefinitionCatalog()
	return c.JSON(fiber.Map{"data": fiber.Map{
		"version": 1, "engines": []string{config.EngineTimeSeries, config.EngineFactor},
		"definitions": definitions, "portfolio_builders": portfolios,
		"backtest_modes": []runner.Mode{runner.Weights, runner.Events}, "mixed_mode": runner.Events,
		"research_tasks": false, "live_binding_required": true,
	}})
}

func (s *DevServer) handleBacktestPreflight(c *fiber.Ctx) error {
	args := new(struct {
		Configs map[string]string `json:"configs" validate:"required"`
		Paths   []string          `json:"paths"`
	})
	if err := base.VerifyArg(c, args, base.ArgBody); err != nil {
		return err
	}
	if s.inspectBacktest == nil {
		return c.Status(fiber.StatusServiceUnavailable).JSON(fiber.Map{"msg": "static preflight is not configured", "errors": []PreflightIssue{{"unavailable", "static preflight is not configured"}}})
	}
	failure := func(err error) error {
		return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"msg": err.Error(), "errors": []PreflightIssue{{"invalid_config", err.Error()}}})
	}
	if len(args.Paths) == 0 {
		for path := range args.Configs {
			args.Paths = append(args.Paths, path)
		}
		sort.Strings(args.Paths)
	}
	dir, paths, err := s.prepareBacktestConfigFiles(args.Configs, args.Paths)
	if err != nil {
		return failure(err)
	}
	defer os.RemoveAll(dir)
	spec, specErr := config.LoadRunSpec(&config.CmdArgs{Configs: paths, NoDefault: true, DataDir: s.DataDir()}, false)
	if specErr != nil {
		return failure(specErr)
	}
	inspection, err := s.inspectBacktest(spec)
	if err != nil {
		return failure(err)
	}
	if slices.Contains(spec.Engines(), config.EngineTimeSeries) {
		if err := validateWebTimeSeriesBacktest(backtestTaskConfig(spec)); err != nil {
			return failure(err)
		}
	}
	inspection.Origins = spec.Origins()
	remap := func(origin config.FieldOrigin) config.FieldOrigin {
		for index, path := range paths {
			if origin.Source == path {
				origin.Source = args.Paths[index]
				break
			}
		}
		return origin
	}
	for field, origin := range inspection.Origins {
		inspection.Origins[field] = remap(origin)
	}
	for _, strategy := range inspection.Strategies {
		for field, value := range strategy.Resolved {
			value.Origin = remap(value.Origin)
			strategy.Resolved[field] = value
		}
	}
	return c.JSON(fiber.Map{"data": inspection})
}

// Projection only: parse task YAML without loading files, defaults or resources.
func appendBacktestMetadata(task *ormu.Task, values map[string]any) {
	// ToMap merges structured Info but older scheduler failures are plain text.
	if task.Info != "" && !json.Valid([]byte(task.Info)) {
		values["info"] = task.Info
	}
	u, err := config.ParseUnifiedYAML([]byte(task.Config), "task")
	if err != nil {
		return
	}
	var engines []string
	for _, policy := range u.RunPolicy {
		engines = append(engines, policy.Engine)
	}
	slices.Sort(engines)
	values["engines"] = slices.Compact(engines)
	if slices.Contains(engines, config.EngineFactor) {
		mode, _ := u.Execution["mode"].(string)
		if mode == "" {
			mode = string(runner.Events)
		}
		values["executionMode"] = mode
	}
}
