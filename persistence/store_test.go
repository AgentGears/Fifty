package persistence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"fifty/kernel/identity"
	"fifty/ports"
)

func fixedID(t *testing.T, value string) identity.ID {
	t.Helper()
	id, err := identity.Parse(value)
	if err != nil {
		t.Fatalf("parse id: %v", err)
	}
	return id
}

func TestStoreRestartPreservesAcceptedState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	workspaceID := fixedID(t, "11111111111111111111111111111111")
	recordID := fixedID(t, "22222222222222222222222222222222")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: recordID, Payload: []byte("accepted")})
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	restarted, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := restarted.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		if err != nil {
			return err
		}
		if record.Version != 1 || string(record.Payload) != "accepted" {
			t.Fatalf("unexpected record after restart: version=%d payload=%q", record.Version, record.Payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("load after restart: %v", err)
	}
}

func TestPreparedJournalDoesNotBecomeAcceptedState(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	workspaceID := fixedID(t, "11111111111111111111111111111111")
	recordID := fixedID(t, "22222222222222222222222222222222")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: recordID, Payload: []byte("old")})
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}

	interrupted := errors.New("injected interruption")
	store.afterJournal = func() error { return interrupted }
	err = store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		if err != nil {
			return err
		}
		record.Payload = []byte("new")
		return tx.ReplaceIfVersion(ctx, record, record.Version)
	})
	if !errors.Is(err, interrupted) {
		t.Fatalf("expected injected interruption, got %v", err)
	}

	restarted, err := Open(dir)
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	if err := restarted.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		if err != nil {
			return err
		}
		if record.Version != 1 || string(record.Payload) != "old" {
			t.Fatalf("prepared state was incorrectly accepted: version=%d payload=%q", record.Version, record.Payload)
		}
		return nil
	}); err != nil {
		t.Fatalf("load after interrupted publication: %v", err)
	}
}

func TestCorruptSnapshotFailsClosed(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	workspaceID := fixedID(t, "11111111111111111111111111111111")
	recordID := fixedID(t, "22222222222222222222222222222222")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: recordID, Payload: []byte("accepted")})
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, snapshotFileName), []byte("not a valid snapshot"), 0o600); err != nil {
		t.Fatalf("corrupt snapshot: %v", err)
	}
	if _, err := Open(dir); !errors.Is(err, ErrCorruptState) {
		t.Fatalf("expected corrupt-state failure, got %v", err)
	}
}

func TestBackupRestorePreservesIdentityAndRejectsStaleBackup(t *testing.T) {
	ctx := context.Background()
	dir := t.TempDir()
	store, err := Open(dir)
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	workspaceID := fixedID(t, "11111111111111111111111111111111")
	recordID := fixedID(t, "22222222222222222222222222222222")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: recordID, Payload: []byte("generation-one")})
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	backup := filepath.Join(t.TempDir(), "backup.json")
	if err := store.Backup(ctx, backup); err != nil {
		t.Fatalf("backup: %v", err)
	}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		if err != nil {
			return err
		}
		record.Payload = []byte("generation-two")
		return tx.ReplaceIfVersion(ctx, record, record.Version)
	}); err != nil {
		t.Fatalf("replace: %v", err)
	}
	if err := store.Restore(ctx, backup); !errors.Is(err, ErrStaleBackup) {
		t.Fatalf("expected stale backup rejection, got %v", err)
	}

	restored, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open restore target: %v", err)
	}
	if err := restored.Restore(ctx, backup); err != nil {
		t.Fatalf("restore into empty store: %v", err)
	}
	if err := restored.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		if err != nil {
			return err
		}
		if record.WorkspaceID != workspaceID || record.ID != recordID || record.Version != 1 || string(record.Payload) != "generation-one" {
			t.Fatalf("restored record mismatch: %+v", record)
		}
		return nil
	}); err != nil {
		t.Fatalf("verify restored state: %v", err)
	}
}

func TestStaleRecordGenerationCannotOverwriteNewerState(t *testing.T) {
	ctx := context.Background()
	store, err := Open(t.TempDir())
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	workspaceID := fixedID(t, "11111111111111111111111111111111")
	recordID := fixedID(t, "22222222222222222222222222222222")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: recordID, Payload: []byte("one")})
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	var stale ports.Record
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		stale = record
		return err
	}); err != nil {
		t.Fatalf("load stale copy: %v", err)
	}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, "test", recordID)
		if err != nil {
			return err
		}
		record.Payload = []byte("two")
		return tx.ReplaceIfVersion(ctx, record, record.Version)
	}); err != nil {
		t.Fatalf("advance state: %v", err)
	}
	stale.Payload = []byte("stale")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.ReplaceIfVersion(ctx, stale, stale.Version)
	}); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("expected version conflict, got %v", err)
	}
}
