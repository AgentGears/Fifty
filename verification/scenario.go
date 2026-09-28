package verification

import (
	"context"
	"fmt"
)

type Scenario struct {
	ID       string
	Required bool
	Run      func(context.Context) error
}

type ScenarioResult struct {
	ID      string
	Passed  bool
	Skipped bool
	Error   string
}

type Report struct{ Results []ScenarioResult }

func (r Report) Passed() bool {
	if len(r.Results) == 0 {
		return false
	}
	executed := false
	for _, result := range r.Results {
		if !result.Passed {
			return false
		}
		if !result.Skipped {
			executed = true
		}
	}
	return executed
}

func RunScenarios(ctx context.Context, scenarios []Scenario) Report {
	report := Report{Results: make([]ScenarioResult, len(scenarios))}
	seen := make(map[string]struct{}, len(scenarios))
	invalid := false

	// Preflight the pack before any runner executes so malformed metadata cannot
	// partially mutate shared fixtures.
	for i, scenario := range scenarios {
		report.Results[i].ID = scenario.ID
		switch {
		case scenario.ID == "":
			report.Results[i].Error = "scenario id is required"
			invalid = true
		default:
			if _, exists := seen[scenario.ID]; exists {
				report.Results[i].Error = "scenario id is duplicated"
				invalid = true
			} else {
				seen[scenario.ID] = struct{}{}
			}
		}
	}
	if invalid {
		return report
	}

	for i, scenario := range scenarios {
		result := &report.Results[i]
		if err := ctx.Err(); err != nil {
			result.Error = err.Error()
			continue
		}
		if scenario.Run == nil {
			result.Skipped = true
			if scenario.Required {
				result.Error = "required scenario has no runner"
			} else {
				result.Passed = true
			}
			continue
		}
		if err := runScenario(ctx, scenario.Run); err != nil {
			result.Error = err.Error()
			continue
		}
		if err := ctx.Err(); err != nil {
			result.Error = err.Error()
			continue
		}
		result.Passed = true
	}
	return report
}

func runScenario(ctx context.Context, run func(context.Context) error) (err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("scenario panic: %v", recovered)
		}
	}()
	return run(ctx)
}

func RequirePassed(report Report) error {
	if report.Passed() {
		return nil
	}
	return fmt.Errorf("verification report did not pass")
}
