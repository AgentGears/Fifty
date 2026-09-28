package verification

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"fifty/ports"
)

func TestMemoryStoreReadDuringTransactionSeesCommittedState(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "before")
	if err := store.WithinTransaction(ctx, func(tx ports.Transaction) error { return tx.Insert(ctx, record) }); err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() {
		done <- store.WithinTransaction(ctx, func(tx ports.Transaction) error {
			current, err := tx.Load(ctx, record.WorkspaceID, record.Kind, record.ID)
			if err != nil {
				return err
			}
			current.Payload = []byte("after")
			if err := tx.ReplaceIfVersion(ctx, current, current.Version); err != nil {
				return err
			}

			visible, err := store.Read(record.WorkspaceID, record.Kind, record.ID)
			if err != nil {
				return err
			}
			if string(visible.Payload) != "before" || visible.Version != 1 {
				return errors.New("read observed uncommitted transaction state")
			}
			return nil
		})
	}()

	select {
	case err := <-done:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("read inside transaction callback deadlocked")
	}

	got, err := store.Read(record.WorkspaceID, record.Kind, record.ID)
	if err != nil {
		t.Fatal(err)
	}
	if string(got.Payload) != "after" || got.Version != 2 {
		t.Fatalf("post-commit state: %+v", got)
	}
}

func TestMemoryStoreSerializesTransactionCallbacks(t *testing.T) {
	store := NewMemoryStore()
	ctx := context.Background()
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	secondStarted := make(chan struct{})
	secondEntered := make(chan struct{})
	var wg sync.WaitGroup
	errs := make(chan error, 2)

	wg.Add(1)
	go func() {
		defer wg.Done()
		errs <- store.WithinTransaction(ctx, func(ports.Transaction) error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()
	<-firstEntered

	wg.Add(1)
	go func() {
		defer wg.Done()
		close(secondStarted)
		errs <- store.WithinTransaction(ctx, func(ports.Transaction) error {
			close(secondEntered)
			return nil
		})
	}()
	<-secondStarted

	select {
	case <-secondEntered:
		t.Fatal("second transaction callback entered before first callback released")
	case <-time.After(50 * time.Millisecond):
	}

	close(releaseFirst)
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-secondEntered:
	default:
		t.Fatal("second transaction callback never entered")
	}
}

func TestMemoryStoreCancellationWhileWaitingDoesNotEnterCallback(t *testing.T) {
	store := NewMemoryStore()
	firstEntered := make(chan struct{})
	releaseFirst := make(chan struct{})
	firstDone := make(chan error, 1)

	go func() {
		firstDone <- store.WithinTransaction(context.Background(), func(ports.Transaction) error {
			close(firstEntered)
			<-releaseFirst
			return nil
		})
	}()
	<-firstEntered

	ctx, cancel := context.WithCancel(context.Background())
	callbackEntered := make(chan struct{}, 1)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- store.WithinTransaction(ctx, func(ports.Transaction) error {
			callbackEntered <- struct{}{}
			return nil
		})
	}()
	cancel()
	close(releaseFirst)

	if err := <-firstDone; err != nil {
		t.Fatalf("first transaction: %v", err)
	}
	if err := <-secondDone; !errors.Is(err, context.Canceled) {
		t.Fatalf("waiting transaction: got %v want canceled", err)
	}
	select {
	case <-callbackEntered:
		t.Fatal("canceled waiting transaction entered callback")
	default:
	}
}
