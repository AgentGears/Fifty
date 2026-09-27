package verification

import (
	"context"
	"errors"
	"testing"

	"fifty/ports"
)

func TestMemoryStoreRollsBackFailedTransaction(t *testing.T) {
	store := NewMemoryStore()
	record := ports.Record{WorkspaceID: "w1", Kind: "test", ID: "r1", Payload: []byte("before")}
	wantErr := errors.New("stop")
	err := store.WithinTransaction(context.Background(), func(tx ports.Transaction) error {
		if err := tx.Insert(context.Background(), record); err != nil {
			return err
		}
		return wantErr
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("transaction error: got %v want %v", err, wantErr)
	}
	if _, err := store.Read("w1", "test", "r1"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("rollback read: got %v", err)
	}
}

func TestMemoryStoreRejectsStaleVersion(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	original := ports.Record{WorkspaceID: "w1", Kind: "test", ID: "r1", Payload: []byte("one")}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, original) }); err != nil {
		t.Fatalf("insert: %v", err)
	}
	updated, err := store.Read("w1", "test", "r1")
	if err != nil {
		t.Fatalf("read before replace: %v", err)
	}
	updated.Payload = []byte("two")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.ReplaceIfVersion(ctx, updated, 1) }); err != nil {
		t.Fatalf("replace: %v", err)
	}
	stale := updated
	stale.Payload = []byte("stale")
	err = store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.ReplaceIfVersion(ctx, stale, 1) })
	if !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("stale replace: got %v want conflict", err)
	}
	got, err := store.Read("w1", "test", "r1")
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	if string(got.Payload) != "two" || got.Version != 2 {
		t.Fatalf("current record: payload=%q version=%d", got.Payload, got.Version)
	}
}

func TestMemoryStoreReportsUnavailable(t *testing.T) {
	store := NewMemoryStore()
	store.SetUnavailable(true)
	err := store.WithinTransaction(context.Background(), func(ports.Transaction) error { return nil })
	if !errors.Is(err, ports.ErrUnavailable) {
		t.Fatalf("got %v want unavailable", err)
	}
}

func TestMemoryStoreRejectsPreVersionedInsert(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := ports.Record{WorkspaceID: "w1", Kind: "test", ID: "r1", Version: 7}
	err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) })
	if !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("got %v want invalid", err)
	}
}

func TestMemoryStoreDoesNotCommitAfterContextCancellation(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	record := ports.Record{WorkspaceID: "w1", Kind: "test", ID: "r1"}
	err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		if err := tx.Insert(ctx, record); err != nil {
			return err
		}
		cancel()
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v want canceled", err)
	}
	if _, err := store.Read("w1", "test", "r1"); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("rollback read: got %v", err)
	}
}

func TestMemoryStoreScopesKeysByWorkspace(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	for _, workspaceID := range []string{"w1", "w2"} {
		record := ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: "shared", Payload: []byte(workspaceID)}
		if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
			t.Fatalf("insert %s: %v", workspaceID, err)
		}
	}
	for _, workspaceID := range []string{"w1", "w2"} {
		got, err := store.Read(workspaceID, "test", "shared")
		if err != nil {
			t.Fatalf("read %s: %v", workspaceID, err)
		}
		if string(got.Payload) != workspaceID {
			t.Fatalf("read %s payload=%q", workspaceID, got.Payload)
		}
	}
}
