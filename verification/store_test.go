package verification

import (
	"context"
	"errors"
	"math"
	"testing"

	"fifty/kernel/identity"
	"fifty/ports"
)

func testID(t *testing.T, value string) identity.ID {
	t.Helper()
	id, err := identity.Parse(value)
	if err != nil {
		t.Fatalf("parse test id %q: %v", value, err)
	}
	return id
}

func testRecord(t *testing.T, workspaceHex, idHex string, payload string) ports.Record {
	t.Helper()
	return ports.Record{
		WorkspaceID: testID(t, workspaceHex),
		Kind:        "test",
		ID:          testID(t, idHex),
		Payload:     []byte(payload),
	}
}

func TestMemoryStoreRollsBackFailedTransaction(t *testing.T) {
	store := NewMemoryStore()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "before")
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
	if _, err := store.Read(record.WorkspaceID, record.Kind, record.ID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("rollback read: got %v", err)
	}
}

func TestMemoryStoreRejectsStaleVersion(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	original := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "one")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, original) }); err != nil {
		t.Fatalf("insert: %v", err)
	}
	updated, err := store.Read(original.WorkspaceID, original.Kind, original.ID)
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
	got, err := store.Read(original.WorkspaceID, original.Kind, original.ID)
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
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "")
	record.Version = 7
	err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) })
	if !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("got %v want invalid", err)
	}
}

func TestMemoryStoreDoesNotCommitAfterContextCancellation(t *testing.T) {
	store := NewMemoryStore()
	ctx, cancel := context.WithCancel(context.Background())
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "")
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
	if _, err := store.Read(record.WorkspaceID, record.Kind, record.ID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("rollback read: got %v", err)
	}
}

func TestMemoryStoreScopesKeysByWorkspace(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	workspaces := []identity.ID{
		testID(t, "00000000000000000000000000000001"),
		testID(t, "00000000000000000000000000000002"),
	}
	shared := testID(t, "00000000000000000000000000000003")
	for i, workspaceID := range workspaces {
		record := ports.Record{WorkspaceID: workspaceID, Kind: "test", ID: shared, Payload: []byte{byte('1' + i)}}
		if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
			t.Fatalf("insert workspace %d: %v", i, err)
		}
	}
	for i, workspaceID := range workspaces {
		got, err := store.Read(workspaceID, "test", shared)
		if err != nil {
			t.Fatalf("read workspace %d: %v", i, err)
		}
		if len(got.Payload) != 1 || got.Payload[0] != byte('1'+i) {
			t.Fatalf("read workspace %d payload=%q", i, got.Payload)
		}
	}
}

func TestMemoryStoreRejectsZeroSemanticIdentifiers(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "")
	record.ID = identity.ID{}
	err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) })
	if !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("got %v want invalid", err)
	}
}

func TestMemoryStoreTransactionCannotEscape(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "before")
	var leaked ports.Transaction
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		leaked = tx
		return tx.Insert(ctx, record)
	}); err != nil {
		t.Fatalf("insert: %v", err)
	}
	current, err := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if err != nil {
		t.Fatalf("read: %v", err)
	}
	current.Payload = []byte("outside")
	if err := leaked.ReplaceIfVersion(ctx, current, current.Version); !errors.Is(err, ports.ErrTransactionClosed) {
		t.Fatalf("escaped transaction: got %v want closed", err)
	}
	got, _ := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if string(got.Payload) != "before" || got.Version != 1 {
		t.Fatalf("escaped transaction changed committed state: %+v", got)
	}
}

func TestMemoryStoreRejectsVersionOverflow(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "")
	k := recordKey{workspaceID: record.WorkspaceID, kind: record.Kind, id: record.ID}
	record.Version = math.MaxUint64
	store.records[k] = cloneRecord(record)
	err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.ReplaceIfVersion(ctx, record, math.MaxUint64)
	})
	if !errors.Is(err, ports.ErrVersionExhausted) {
		t.Fatalf("got %v want version exhausted", err)
	}
	got, _ := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if got.Version != math.MaxUint64 {
		t.Fatalf("version changed after exhaustion: %d", got.Version)
	}
}

func TestMemoryStoreDeleteAndStaleDelete(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
		t.Fatal(err)
	}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.DeleteIfVersion(ctx, record.WorkspaceID, record.Kind, record.ID, 0)
	}); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("stale delete: got %v want conflict", err)
	}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		return tx.DeleteIfVersion(ctx, record.WorkspaceID, record.Kind, record.ID, 1)
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	if _, err := store.Read(record.WorkspaceID, record.Kind, record.ID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("post-delete read: %v", err)
	}
}

func TestMemoryStoreLoadAndPayloadIsolation(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "original")
	inputPayload := record.Payload
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
		t.Fatal(err)
	}
	inputPayload[0] = 'X'

	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		loaded, err := tx.Load(ctx, record.WorkspaceID, record.Kind, record.ID)
		if err != nil {
			return err
		}
		loaded.Payload[0] = 'Y'
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	got, err := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "original" {
		t.Fatalf("payload alias leaked into store: %q", got.Payload)
	}
	got.Payload[0] = 'Z'
	again, _ := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if string(again.Payload) != "original" {
		t.Fatalf("read payload alias leaked into store: %q", again.Payload)
	}
}

func TestMemoryStoreRejectsDuplicateInsertAndNilTransaction(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
		t.Fatal(err)
	}
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); !errors.Is(err, ports.ErrConflict) {
		t.Fatalf("duplicate insert: got %v want conflict", err)
	}
	if err := store.WithinTransaction(ctx, nil); !errors.Is(err, ports.ErrInvalid) {
		t.Fatalf("nil transaction callback: got %v want invalid", err)
	}
}

func TestMemoryStoreScopesKeysByKind(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	workspaceID := testID(t, "00000000000000000000000000000001")
	id := testID(t, "00000000000000000000000000000002")
	for _, item := range []struct {
		kind    string
		payload string
	}{{"alpha", "one"}, {"beta", "two"}} {
		record := ports.Record{WorkspaceID: workspaceID, Kind: item.kind, ID: id, Payload: []byte(item.payload)}
		if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
			t.Fatalf("insert kind %q: %v", item.kind, err)
		}
	}
	for _, item := range []struct {
		kind    string
		payload string
	}{{"alpha", "one"}, {"beta", "two"}} {
		got, err := store.Read(workspaceID, item.kind, id)
		if err != nil || string(got.Payload) != item.payload {
			t.Fatalf("kind %q: record=%+v err=%v", item.kind, got, err)
		}
	}
}

func TestMemoryStoreClosesTransactionBeforePostCallbackUse(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "before")
	var leaked ports.Transaction
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		leaked = tx
		return tx.Insert(ctx, record)
	}); err != nil {
		t.Fatal(err)
	}

	const attempts = 64
	errs := make(chan error, attempts)
	for i := 0; i < attempts; i++ {
		go func() {
			current, _ := store.Read(record.WorkspaceID, record.Kind, record.ID)
			current.Payload = []byte("outside")
			errs <- leaked.ReplaceIfVersion(ctx, current, current.Version)
		}()
	}
	for i := 0; i < attempts; i++ {
		if err := <-errs; !errors.Is(err, ports.ErrTransactionClosed) {
			t.Fatalf("escaped transaction attempt %d: got %v want closed", i, err)
		}
	}
	got, _ := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if string(got.Payload) != "before" || got.Version != 1 {
		t.Fatalf("post-callback use changed committed state: %+v", got)
	}
}
