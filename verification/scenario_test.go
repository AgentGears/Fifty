package verification

import (
	"context"
	"errors"
	"testing"
)

func TestRequiredSkippedScenarioCannotPass(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{{ID: "required", Required: true}})
	if report.Passed() {
		t.Fatal("required skipped scenario must not pass")
	}
	if err := RequirePassed(report); err == nil {
		t.Fatal("required skipped scenario must make qualification fail")
	}
}

func TestOptionalSkippedScenarioMayPass(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{{ID: "optional", Required: false}})
	if !report.Passed() {
		t.Fatalf("optional skipped scenario should not fail: %+v", report)
	}
}

func TestScenarioFailureCannotPass(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{{ID: "broken", Required: true, Run: func(context.Context) error { return errors.New("broken invariant") }}})
	if report.Passed() {
		t.Fatal("failing scenario must not pass")
	}
}

func TestEmptyScenarioSetCannotPass(t *testing.T) {
	report := RunScenarios(context.Background(), nil)
	if report.Passed() {
		t.Fatal("empty scenario set must not pass")
	}
}

func TestScenarioIdsMustBeUniqueAndNonEmpty(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{
		{ID: "", Run: func(context.Context) error { return nil }},
		{ID: "same", Run: func(context.Context) error { return nil }},
		{ID: "same", Run: func(context.Context) error { return nil }},
	})
	if report.Passed() {
		t.Fatal("invalid scenario identifiers must not pass")
	}
}
