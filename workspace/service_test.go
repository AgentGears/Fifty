package workspace

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"fifty/kernel/identity"
	"fifty/persistence"
	"fifty/ports"
)

type fixedClock struct{ value time.Time }

type uncertainCommitStore struct {
	inner ports.Store
}

func (s uncertainCommitStore) WithinTransaction(ctx context.Context, fn func(ports.Transaction) error) error {
	err := s.inner.WithinTransaction(ctx, fn)
	if err != nil {
		return err
	}
	return ports.ErrCommitUncertain
}

func (c fixedClock) Now() time.Time { return c.value }

func newTestService(t *testing.T, store ports.Store) *Service {
	t.Helper()
	service, err := NewService(store, identity.RandomGenerator{}, fixedClock{value: time.Date(2026, 9, 28, 20, 30, 0, 0, time.UTC)})
	if err != nil {
		t.Fatalf("new service: %v", err)
	}
	return service
}

func TestWorkspaceAndPrincipalIdentitySurviveRestartAndSessionReplacement(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := persistence.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service := newTestService(t, store)
	createdWorkspace, createdHuman, createdAssistant, credentials, err := service.Initialize(ctx)
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if credentials.Access == "" || credentials.Recovery == "" || credentials.Access == credentials.Recovery {
		t.Fatal("credentials were not independently issued")
	}

	restartedStore, err := persistence.Open(dir)
	if err != nil {
		t.Fatalf("reopen store: %v", err)
	}
	replacementSession := newTestService(t, restartedStore)
	workspace, human, assistant, err := replacementSession.Load(ctx, createdWorkspace.ID)
	if err != nil {
		t.Fatalf("load after restart: %v", err)
	}
	if workspace.ID != createdWorkspace.ID || human.ID != createdHuman.ID || assistant.ID != createdAssistant.ID {
		t.Fatalf("canonical identities changed across restart: workspace=%s human=%s assistant=%s", workspace.ID, human.ID, assistant.ID)
	}
	if workspace.PrimaryAssistantPrincipalID != createdAssistant.ID {
		t.Fatal("primary assistant semantic identity changed across session replacement")
	}
	resolvedWorkspace, resolvedHuman, err := replacementSession.ResolveAccess(ctx, credentials.Access)
	if err != nil {
		t.Fatalf("resolve access: %v", err)
	}
	if resolvedWorkspace.ID != createdWorkspace.ID || resolvedHuman.ID != createdHuman.ID {
		t.Fatal("access credential did not resolve to the durable human principal and workspace")
	}
}

func TestRecoveryPreservesPrincipalAndRevokesPriorCredentials(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := persistence.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service := newTestService(t, store)
	workspace, human, _, credentials, err := service.Initialize(ctx)
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}

	restartedStore, err := persistence.Open(dir)
	if err != nil {
		t.Fatalf("reopen store for recovery: %v", err)
	}
	service = newTestService(t, restartedStore)
	recoveredWorkspace, recoveredHuman, replacement, err := service.Recover(ctx, credentials.Recovery)
	if err != nil {
		t.Fatalf("recover: %v", err)
	}
	if recoveredWorkspace.ID != workspace.ID || recoveredHuman.ID != human.ID {
		t.Fatal("recovery changed canonical workspace or human principal identity")
	}
	if recoveredWorkspace.Version != workspace.Version+1 || recoveredWorkspace.AccessGeneration != workspace.AccessGeneration+1 || recoveredWorkspace.RecoveryGeneration != workspace.RecoveryGeneration+1 {
		t.Fatalf("recovery generations did not advance: %+v", recoveredWorkspace)
	}
	if replacement.Access == credentials.Access || replacement.Recovery == credentials.Recovery {
		t.Fatal("recovery failed to rotate credentials")
	}
	if _, _, err := service.ResolveAccess(ctx, credentials.Access); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("old access credential was not revoked: %v", err)
	}
	if _, _, _, err := service.Recover(ctx, credentials.Recovery); !errors.Is(err, ErrRecoveryDenied) {
		t.Fatalf("old recovery credential was not revoked: %v", err)
	}
	resolvedWorkspace, resolvedHuman, err := service.ResolveAccess(ctx, replacement.Access)
	if err != nil {
		t.Fatalf("replacement access failed: %v", err)
	}
	if resolvedWorkspace.ID != workspace.ID || resolvedHuman.ID != human.ID {
		t.Fatal("replacement access resolved to a different canonical identity")
	}
	loadedWorkspace, _, assistant, err := service.Load(ctx, workspace.ID)
	if err != nil {
		t.Fatalf("load after recovery: %v", err)
	}
	if loadedWorkspace.PrimaryAssistantPrincipalID != assistant.ID {
		t.Fatal("recovery changed primary-assistant semantic identity")
	}
}

func TestRawCredentialValuesNeverEnterCanonicalPayload(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := persistence.Open(dir)
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service := newTestService(t, store)
	workspace, _, _, credentials, err := service.Initialize(ctx)
	if err != nil {
		t.Fatalf("initialize: %v", err)
	}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspace.ID, WorkspaceRecordKind, workspace.ID)
		if err != nil {
			return err
		}
		if bytes.Contains(record.Payload, []byte(credentials.Access)) || bytes.Contains(record.Payload, []byte(credentials.Recovery)) {
			t.Fatal("raw credential value was admitted to canonical workspace payload")
		}
		return nil
	}); err != nil {
		t.Fatalf("inspect canonical payload: %v", err)
	}
	snapshot, err := os.ReadFile(filepath.Join(dir, "snapshot.json"))
	if err != nil {
		t.Fatalf("read persisted snapshot: %v", err)
	}
	if bytes.Contains(snapshot, []byte(credentials.Access)) || bytes.Contains(snapshot, []byte(credentials.Recovery)) {
		t.Fatal("raw credential value was persisted in the canonical snapshot")
	}
}

func TestMalformedAndUnknownCredentialsFailClosed(t *testing.T) {
	ctx := context.Background()
	store, err := persistence.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service := newTestService(t, store)
	if _, _, err := service.ResolveAccess(ctx, "not-a-credential"); !errors.Is(err, ErrAccessDenied) {
		t.Fatalf("malformed access credential did not fail closed: %v", err)
	}
	if _, _, _, err := service.Recover(ctx, "not-a-credential"); !errors.Is(err, ErrRecoveryDenied) {
		t.Fatalf("malformed recovery credential did not fail closed: %v", err)
	}
}

func TestInitializeRetainsIssuedCredentialsWhenCommitOutcomeIsUncertain(t *testing.T) {
	ctx := context.Background()
	inner, err := persistence.Open(t.TempDir())
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	service := newTestService(t, uncertainCommitStore{inner: inner})
	workspace, human, assistant, credentials, err := service.Initialize(ctx)
	if !errors.Is(err, ports.ErrCommitUncertain) {
		t.Fatalf("expected commit uncertainty, got %v", err)
	}
	if workspace.ID.IsZero() || human.ID.IsZero() || assistant.ID.IsZero() {
		t.Fatal("uncertain initialization discarded canonical identity results")
	}
	if credentials.Access == "" || credentials.Recovery == "" {
		t.Fatal("uncertain initialization discarded issued credentials")
	}

	committed := newTestService(t, inner)
	resolvedWorkspace, resolvedHuman, resolveErr := committed.ResolveAccess(ctx, credentials.Access)
	if resolveErr != nil {
		t.Fatalf("issued access credential did not resolve after uncertain commit: %v", resolveErr)
	}
	if resolvedWorkspace.ID != workspace.ID || resolvedHuman.ID != human.ID {
		t.Fatal("uncertain initialization credentials do not identify committed canonical state")
	}
}

func TestCanonicalObjectsRejectSharedSemanticIdentifiers(t *testing.T) {
	workspaceID, err := identity.Parse("11111111111111111111111111111111")
	if err != nil {
		t.Fatalf("parse workspace id: %v", err)
	}
	assistantID, err := identity.Parse("22222222222222222222222222222222")
	if err != nil {
		t.Fatalf("parse assistant id: %v", err)
	}
	now := time.Date(2026, 9, 28, 20, 30, 0, 0, time.UTC)
	invalidWorkspace := Workspace{
		ID:                          workspaceID,
		HumanPrincipalID:            workspaceID,
		PrimaryAssistantPrincipalID: assistantID,
		State:                       WorkspaceActive,
		CreatedAt:                   now,
		RetentionPolicyRef:          InitialRetentionPolicyRef,
		Version:                     1,
		accessDigest:                tokenDigest(accessCredentialDomain, []byte("access")),
		recoveryDigest:              tokenDigest(recoveryCredentialDomain, []byte("recovery")),
		AccessGeneration:            1,
		RecoveryGeneration:          1,
	}
	if err := validateWorkspace(invalidWorkspace); !errors.Is(err, ErrInvalidCanonicalState) {
		t.Fatalf("workspace accepted an identifier shared with a principal: %v", err)
	}
	invalidPrincipal := Principal{
		ID:          workspaceID,
		WorkspaceID: workspaceID,
		Kind:        PrincipalHuman,
		State:       PrincipalActive,
		CreatedAt:   now,
		Version:     1,
	}
	if err := validatePrincipal(invalidPrincipal); !errors.Is(err, ErrInvalidCanonicalState) {
		t.Fatalf("principal accepted the workspace identifier as its own: %v", err)
	}
}
