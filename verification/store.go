package verification

import (
	"context"
	"math"
	"sync"

	"fifty/kernel/identity"
	"fifty/ports"
)

type recordKey struct {
	workspaceID identity.ID
	kind        string
	id          identity.ID
}

type MemoryStore struct {
	mu          sync.RWMutex
	records     map[recordKey]ports.Record
	unavailable bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{records: make(map[recordKey]ports.Record)} }

func (s *MemoryStore) SetUnavailable(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = value
}

func (s *MemoryStore) WithinTransaction(ctx context.Context, fn func(ports.Transaction) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if fn == nil {
		return ports.ErrInvalid
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return ports.ErrUnavailable
	}

	tx := &memoryTransaction{records: cloneRecords(s.records)}
	var callbackErr error
	var activeAtClose int
	func() {
		defer func() { activeAtClose = tx.close() }()
		callbackErr = fn(tx)
	}()
	if callbackErr != nil {
		return callbackErr
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	if activeAtClose != 0 {
		return ports.ErrTransactionInFlight
	}

	s.records = tx.snapshot()
	return nil
}

func (s *MemoryStore) Read(workspaceID identity.ID, kind string, id identity.ID) (ports.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unavailable {
		return ports.Record{}, ports.ErrUnavailable
	}
	if err := validateKey(workspaceID, kind, id); err != nil {
		return ports.Record{}, err
	}
	record, ok := s.records[recordKey{workspaceID: workspaceID, kind: kind, id: id}]
	if !ok {
		return ports.Record{}, ports.ErrNotFound
	}
	return cloneRecord(record), nil
}

type memoryTransaction struct {
	lifecycleMu sync.Mutex
	closed      bool
	active      int

	recordsMu sync.Mutex
	records   map[recordKey]ports.Record
}

func (tx *memoryTransaction) Load(ctx context.Context, workspaceID identity.ID, kind string, id identity.ID) (ports.Record, error) {
	if err := tx.begin(ctx); err != nil {
		return ports.Record{}, err
	}
	defer tx.end()

	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	if err := tx.ensureOpenAfterBegin(ctx); err != nil {
		return ports.Record{}, err
	}
	if err := validateKey(workspaceID, kind, id); err != nil {
		return ports.Record{}, err
	}
	record, ok := tx.records[recordKey{workspaceID: workspaceID, kind: kind, id: id}]
	if !ok {
		return ports.Record{}, ports.ErrNotFound
	}
	return cloneRecord(record), nil
}

func (tx *memoryTransaction) Insert(ctx context.Context, record ports.Record) error {
	if err := tx.begin(ctx); err != nil {
		return err
	}
	defer tx.end()

	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	if err := tx.ensureOpenAfterBegin(ctx); err != nil {
		return err
	}
	if err := validateRecord(record); err != nil {
		return err
	}
	if record.Version != 0 {
		return ports.ErrInvalid
	}
	k := recordKey{workspaceID: record.WorkspaceID, kind: record.Kind, id: record.ID}
	if _, exists := tx.records[k]; exists {
		return ports.ErrConflict
	}
	record.Version = 1
	tx.records[k] = cloneRecord(record)
	return nil
}

func (tx *memoryTransaction) ReplaceIfVersion(ctx context.Context, record ports.Record, expected uint64) error {
	if err := tx.begin(ctx); err != nil {
		return err
	}
	defer tx.end()

	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	if err := tx.ensureOpenAfterBegin(ctx); err != nil {
		return err
	}
	if err := validateRecord(record); err != nil {
		return err
	}
	k := recordKey{workspaceID: record.WorkspaceID, kind: record.Kind, id: record.ID}
	current, exists := tx.records[k]
	if !exists {
		return ports.ErrNotFound
	}
	if current.Version != expected {
		return ports.ErrConflict
	}
	if record.Version != expected {
		return ports.ErrInvalid
	}
	if expected == math.MaxUint64 {
		return ports.ErrVersionExhausted
	}
	record.Version = expected + 1
	tx.records[k] = cloneRecord(record)
	return nil
}

func (tx *memoryTransaction) DeleteIfVersion(ctx context.Context, workspaceID identity.ID, kind string, id identity.ID, expected uint64) error {
	if err := tx.begin(ctx); err != nil {
		return err
	}
	defer tx.end()

	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	if err := tx.ensureOpenAfterBegin(ctx); err != nil {
		return err
	}
	if err := validateKey(workspaceID, kind, id); err != nil {
		return err
	}
	k := recordKey{workspaceID: workspaceID, kind: kind, id: id}
	current, exists := tx.records[k]
	if !exists {
		return ports.ErrNotFound
	}
	if current.Version != expected {
		return ports.ErrConflict
	}
	delete(tx.records, k)
	return nil
}

func (tx *memoryTransaction) begin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.lifecycleMu.Lock()
	defer tx.lifecycleMu.Unlock()
	if tx.closed {
		return ports.ErrTransactionClosed
	}
	tx.active++
	return nil
}

func (tx *memoryTransaction) end() {
	tx.lifecycleMu.Lock()
	defer tx.lifecycleMu.Unlock()
	tx.active--
}

func (tx *memoryTransaction) ensureOpenAfterBegin(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx.lifecycleMu.Lock()
	defer tx.lifecycleMu.Unlock()
	if tx.closed {
		return ports.ErrTransactionClosed
	}
	return nil
}

func (tx *memoryTransaction) close() int {
	tx.lifecycleMu.Lock()
	defer tx.lifecycleMu.Unlock()
	tx.closed = true
	return tx.active
}

func (tx *memoryTransaction) snapshot() map[recordKey]ports.Record {
	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	return cloneRecords(tx.records)
}

func validateRecord(record ports.Record) error {
	return validateKey(record.WorkspaceID, record.Kind, record.ID)
}

func validateKey(workspaceID identity.ID, kind string, id identity.ID) error {
	if workspaceID.IsZero() || id.IsZero() || kind == "" {
		return ports.ErrInvalid
	}
	return nil
}

func cloneRecords(source map[recordKey]ports.Record) map[recordKey]ports.Record {
	result := make(map[recordKey]ports.Record, len(source))
	for k, record := range source {
		result[k] = cloneRecord(record)
	}
	return result
}

func cloneRecord(record ports.Record) ports.Record {
	record.Payload = append([]byte(nil), record.Payload...)
	return record
}
