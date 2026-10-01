package conversation

import (
	"context"
	"errors"
	"testing"
	"time"

	"fifty/kernel/identity"
	"fifty/persistence"
	"fifty/workspace"
)

type testClock struct{ value time.Time }

func (c *testClock) Now() time.Time { return c.value }

func newServices(t *testing.T, directory string, source *testClock) (*workspace.Service, *Service, workspace.Workspace, workspace.Principal, workspace.Principal) {
	t.Helper()
	store, err := persistence.Open(directory)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	workspaceService, err := workspace.NewService(store, identity.RandomGenerator{}, source)
	if err != nil {
		t.Fatalf("new workspace service: %v", err)
	}
	createdWorkspace, human, assistant, _, err := workspaceService.Initialize(context.Background())
	if err != nil {
		t.Fatalf("initialize workspace: %v", err)
	}
	conversationService, err := NewService(store, identity.RandomGenerator{}, source, workspaceService)
	if err != nil {
		t.Fatalf("new conversation service: %v", err)
	}
	return workspaceService, conversationService, createdWorkspace, human, assistant
}

func reopenConversation(t *testing.T, directory string, source *testClock) (*workspace.Service, *Service) {
	t.Helper()
	store, err := persistence.Open(directory)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	workspaceService, err := workspace.NewService(store, identity.RandomGenerator{}, source)
	if err != nil {
		t.Fatalf("new workspace service after restart: %v", err)
	}
	conversationService, err := NewService(store, identity.RandomGenerator{}, source, workspaceService)
	if err != nil {
		t.Fatalf("new conversation service after restart: %v", err)
	}
	return workspaceService, conversationService
}

func TestInteractionSurvivesRestartAndContextRegenerates(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &testClock{value: time.Date(2026, 10, 1, 7, 0, 0, 0, time.UTC)}
	_, service, root, human, assistant := newServices(t, dir, clock)

	first, err := service.Admit(ctx, root.ID, human.ID, DirectionUserToAssistant, "content://user/1", DataD1, nil)
	if err != nil {
		t.Fatalf("admit user interaction: %v", err)
	}
	clock.value = clock.value.Add(time.Second)
	second, err := service.Admit(ctx, root.ID, assistant.ID, DirectionAssistantToUser, "content://assistant/1", DataD0, []SemanticReference{{Kind: "interaction", ID: first.ID}})
	if err != nil {
		t.Fatalf("admit assistant interaction: %v", err)
	}

	_, restarted := reopenConversation(t, dir, clock)
	loaded, err := restarted.LoadInteraction(ctx, root.ID, second.ID)
	if err != nil {
		t.Fatalf("load interaction after restart: %v", err)
	}
	if loaded.ID != second.ID || loaded.ContentRef != second.ContentRef || loaded.AuthorPrincipalID != assistant.ID {
		t.Fatalf("interaction changed across restart: %+v", loaded)
	}

	view, err := restarted.ProjectContext(ctx, root.ID, ProjectionPolicy{MaxInteractions: 8})
	if err != nil {
		t.Fatalf("project context after restart: %v", err)
	}
	if view.Generation == "" || len(view.Interactions) != 2 {
		t.Fatalf("unexpected regenerated context: %+v", view)
	}
	view.Interactions[0].ContentRef = "tampered://projection"
	canonical, err := restarted.LoadInteraction(ctx, root.ID, first.ID)
	if err != nil {
		t.Fatalf("reload canonical interaction: %v", err)
	}
	if canonical.ContentRef != "content://user/1" {
		t.Fatal("mutating ContextView changed canonical Interaction")
	}
}

func TestContextIsBoundedAndD2FailsClosedWithoutAdmissionWorkflow(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &testClock{value: time.Date(2026, 10, 1, 8, 0, 0, 0, time.UTC)}
	_, service, root, human, _ := newServices(t, dir, clock)

	for i, ref := range []string{"content://1", "content://2", "content://3"} {
		clock.value = clock.value.Add(time.Second)
		if _, err := service.Admit(ctx, root.ID, human.ID, DirectionUserToAssistant, ref, DataD0, nil); err != nil {
			t.Fatalf("admit D0 interaction %d: %v", i, err)
		}
	}
	clock.value = clock.value.Add(time.Second)
	if _, err := service.Admit(ctx, root.ID, human.ID, DirectionUserToAssistant, "content://d2", DataD2, nil); !errors.Is(err, ErrD2AdmissionRequired) {
		t.Fatalf("D2 admission did not fail closed without an admission workflow: %v", err)
	}

	view, err := service.ProjectContext(ctx, root.ID, ProjectionPolicy{MaxInteractions: 2})
	if err != nil {
		t.Fatalf("project bounded context: %v", err)
	}
	if len(view.Interactions) != 2 || view.Interactions[0].ContentRef != "content://2" || view.Interactions[1].ContentRef != "content://3" {
		t.Fatalf("bounded projection is wrong: %+v", view.Interactions)
	}
}

func TestReferenceResolutionFailsClosedOnAmbiguity(t *testing.T) {
	firstID := mustID(t, "11111111111111111111111111111111")
	secondID := mustID(t, "22222222222222222222222222222222")
	candidates := []ReferenceCandidate{
		{Label: "current", Reference: SemanticReference{Kind: "interaction", ID: firstID}},
		{Label: "current", Reference: SemanticReference{Kind: "interaction", ID: secondID}},
	}
	if _, err := ResolveExactReference("current", candidates); !errors.Is(err, ErrReferenceAmbiguous) {
		t.Fatalf("ambiguous reference did not fail closed: %v", err)
	}
	if _, err := ResolveExactReference("curr", candidates); !errors.Is(err, ErrReferenceUnresolved) {
		t.Fatalf("non-exact reference was resolved by similarity: %v", err)
	}

	resolved, err := ResolveExactReference("only", []ReferenceCandidate{{Label: "only", Reference: SemanticReference{Kind: "interaction", ID: firstID}}})
	if err != nil || resolved.ID != firstID {
		t.Fatalf("exact reference did not resolve: ref=%+v err=%v", resolved, err)
	}
}

func TestAdmissionRejectsD3AndAuthorDirectionMismatch(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	clock := &testClock{value: time.Date(2026, 10, 1, 10, 0, 0, 0, time.UTC)}
	_, service, root, human, assistant := newServices(t, dir, clock)

	if _, err := service.Admit(ctx, root.ID, human.ID, DirectionUserToAssistant, "content://secret", DataD3, nil); !errors.Is(err, ErrD3NotAdmitted) {
		t.Fatalf("D3 entered ordinary conversation state: %v", err)
	}
	if _, err := service.Admit(ctx, root.ID, assistant.ID, DirectionUserToAssistant, "content://wrong-author", DataD0, nil); !errors.Is(err, ErrAuthorMismatch) {
		t.Fatalf("assistant admitted with user direction: %v", err)
	}
}

func mustID(t *testing.T, value string) identity.ID {
	t.Helper()
	id, err := identity.Parse(value)
	if err != nil {
		t.Fatalf("parse id %q: %v", value, err)
	}
	return id
}
