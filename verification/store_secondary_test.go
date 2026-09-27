package verification

import (
	"context"
	"errors"
	"runtime"
	"testing"
	"time"

	"fifty/ports"
)

func TestMemoryStoreAbortsWhenOperationIsInFlightAtCallbackReturn(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "before")

	var concrete *memoryTransaction
	var operationDone chan error

	err := store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		concrete = tx.(*memoryTransaction)
		concrete.recordsMu.Lock()
		operationDone = make(chan error, 1)
		go func() {
			operationDone <- tx.Insert(ctx, record)
		}()

		deadline := time.Now().Add(2 * time.Second)
		for {
			concrete.lifecycleMu.Lock()
			active := concrete.active
			concrete.lifecycleMu.Unlock()
			if active == 1 {
				return nil
			}
			if time.Now().After(deadline) {
				concrete.recordsMu.Unlock()
				return errors.New("operation did not become active")
			}
			runtime.Gosched()
		}
	})

	if !errors.Is(err, ports.ErrTransactionInFlight) {
		if concrete != nil {
			concrete.recordsMu.Unlock()
		}
		t.Fatalf("transaction result: got %v want in-flight error", err)
	}

	concrete.recordsMu.Unlock()
	if opErr := <-operationDone; !errors.Is(opErr, ports.ErrTransactionClosed) {
		t.Fatalf("in-flight operation after close: got %v want closed", opErr)
	}
	if _, err := store.Read(record.WorkspaceID, record.Kind, record.ID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("in-flight operation reached committed state: %v", err)
	}
}
