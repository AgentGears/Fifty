package verification

import (
	"context"
	"errors"
	"strings"
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

func TestOptionalSkippedScenarioAloneCannotQualify(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{{ID: "optional", Required: false}})
	if report.Passed() {
		t.Fatalf("all-skipped evidence must not qualify: %+v", report)
	}
}

func TestOptionalSkippedScenarioMayAccompanyExecutedEvidence(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{
		{ID: "executed", Run: func(context.Context) error { return nil }},
		{ID: "optional"},
	})
	if !report.Passed() {
		t.Fatalf("executed evidence plus optional skip should pass: %+v", report)
	}
}

func TestScenarioFailureCannotPass(t *testing.T) {
	report := RunScenarios(context.Background(), []Scenario{{ID: "broken", Required: true, Run: func(context.Context) error { return errors.New("broken invariant") }}})
	if report.Passed() {
		t.Fatal("failing scenario must not pass")
	}
}

func TestScenarioPanicBecomesFailureAndLaterScenarioRuns(t *testing.T) {
	laterRan := false
	report := RunScenarios(context.Background(), []Scenario{
		{ID: "panic", Required: true, Run: func(context.Context) error { panic("boom") }},
		{ID: "later", Required: true, Run: func(context.Context) error { laterRan = true; return nil }},
	})
	if report.Passed() {
		t.Fatalf("panicking scenario must not pass: %+v", report)
	}
	if !strings.Contains(report.Results[0].Error, "scenario panic") {
		t.Fatalf("panic was not recorded as scenario failure: %+v", report.Results[0])
	}
	if !laterRan || !report.Results[1].Passed {
		t.Fatalf("later scenario did not execute after contained panic: %+v", report)
	}
}

func TestEmptyScenarioSetCannotPass(t *testing.T) {
	report := RunScenarios(context.Background(), nil)
	if report.Passed() {
		t.Fatal("empty scenario set must not pass")
	}
}

func TestScenarioIdsArePreflightedBeforeExecution(t *testing.T) {
	executed := 0
	report := RunScenarios(context.Background(), []Scenario{
		{ID: "same", Run: func(context.Context) error { executed++; return nil }},
		{ID: "same", Run: func(context.Context) error { executed++; return nil }},
	})
	if report.Passed() {
		t.Fatal("duplicate scenario identifiers must not pass")
	}
	if executed != 0 {
		t.Fatalf("invalid pack executed %d runners before rejection", executed)
	}
}

func TestScenarioIdCannotBeEmpty(t *testing.T) {
	executed := 0
	report := RunScenarios(context.Background(), []Scenario{{ID: "", Run: func(context.Context) error { executed++; return nil }}})
	if report.Passed() || executed != 0 {
		t.Fatalf("empty id pack result=%+v executed=%d", report, executed)
	}
}

func TestCancelledScenarioContextCannotPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	report := RunScenarios(ctx, []Scenario{{ID: "required", Required: true, Run: func(context.Context) error { return nil }}})
	if report.Passed() {
		t.Fatalf("cancelled context must not pass: %+v", report)
	}
}

func TestCancellationDuringRunnerCannotPass(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	report := RunScenarios(ctx, []Scenario{{ID: "required", Required: true, Run: func(context.Context) error { cancel(); return nil }}})
	if report.Passed() {
		t.Fatalf("runner canceled context but report passed: %+v", report)
	}
}
