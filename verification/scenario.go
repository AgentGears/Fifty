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
	for _, result := range r.Results {
		if !result.Passed {
			return false
		}
	}
	return true
}

func RunScenarios(ctx context.Context, scenarios []Scenario) Report {
	report := Report{Results: make([]ScenarioResult, 0, len(scenarios))}
	seen := make(map[string]struct{}, len(scenarios))
	for _, scenario := range scenarios {
		result := ScenarioResult{ID: scenario.ID}
		if scenario.ID == "" {
			result.Error = "scenario id is required"
			report.Results = append(report.Results, result)
			continue
		}
		if _, exists := seen[scenario.ID]; exists {
			result.Error = "scenario id is duplicated"
			report.Results = append(report.Results, result)
			continue
		}
		seen[scenario.ID] = struct{}{}
		if scenario.Run == nil {
			result.Skipped = true
			if scenario.Required {
				result.Error = "required scenario has no runner"
			} else {
				result.Passed = true
			}
			report.Results = append(report.Results, result)
			continue
		}
		if err := scenario.Run(ctx); err != nil {
			result.Error = err.Error()
		} else {
			result.Passed = true
		}
		report.Results = append(report.Results, result)
	}
	return report
}

func RequirePassed(report Report) error {
	if report.Passed() {
		return nil
	}
	return fmt.Errorf("verification report did not pass")
}
