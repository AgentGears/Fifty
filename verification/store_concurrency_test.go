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

type observedCancelContext struct {
	context.Context
	doneObserved chan struct{}
	once         sync.Once
}

func (c *observedCancelContext) Done() <-chan struct{} {
	c.once.Do(func() { close(c.doneObserved) })
	return c.Context.Done()
}

func TestMemoryStoreCancellationWhileWaitingReturnsPromptly(t *testing.T) {
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

	base, cancel := context.WithCancel(context.Background())
	ctx := &observedCancelContext{Context: base, doneObserved: make(chan struct{})}
	callbackEntered := make(chan struct{}, 1)
	secondDone := make(chan error, 1)
	go func() {
		secondDone <- store.WithinTransaction(ctx, func(ports.Transaction) error {
			callbackEntered <- struct{}{}
			return nil
		})
	}()

	select {
	case <-ctx.doneObserved:
	case <-time.After(2 * time.Second):
		t.Fatal("waiting transaction did not reach context-aware gate")
	}
	cancel()

	select {
	case err := <-secondDone:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("waiting transaction: got %v want canceled", err)
		}
	case <-time.After(250 * time.Millisecond):
		t.Fatal("canceled waiting transaction remained blocked behind active callback")
	}
	select {
	case <-callbackEntered:
		t.Fatal("canceled waiting transaction entered callback")
	default:
	}

	close(releaseFirst)
	if err := <-firstDone; err != nil {
		t.Fatalf("first transaction: %v", err)
	}
}

type observedErrContext struct {
	context.Context
	mu       sync.Mutex
	count    int
	target   int
	observed chan struct{}
	once     sync.Once
}

func (c *observedErrContext) Err() error {
	c.mu.Lock()
	c.count++
	count := c.count
	c.mu.Unlock()
	if count == c.target {
		c.once.Do(func() { close(c.observed) })
	}
	return c.Context.Err()
}

func TestMemoryStoreCancellationBeforePublicationDoesNotCommit(t *testing.T) {
	store := NewMemoryStore()
	record := testRecord(t, "00000000000000000000000000000001", "00000000000000000000000000000002", "before")

	base, cancel := context.WithCancel(context.Background())
	ctx := &observedErrContext{Context: base, target: 4, observed: make(chan struct{})}

	// Hold a committed-state read lock so the transaction can execute and
	// snapshot its working set but cannot publish until this test releases it.
	store.mu.RLock()
	done := make(chan error, 1)
	go func() {
		done <- store.WithinTransaction(ctx, func(tx ports.Transaction) error {
			return tx.Insert(context.Background(), record)
		})
	}()

	select {
	case <-ctx.observed:
	case <-time.After(2 * time.Second):
		store.mu.RUnlock()
		t.Fatal("transaction did not reach pre-publication cancellation check")
	}
	cancel()
	store.mu.RUnlock()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("transaction result: got %v want canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("canceled transaction did not return after publication lock released")
	}
	if _, err := store.Read(record.WorkspaceID, record.Kind, record.ID); !errors.Is(err, ports.ErrNotFound) {
		t.Fatalf("canceled transaction published working state: %v", err)
	}
}
