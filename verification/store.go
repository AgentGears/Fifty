package verification

import (
	"context"
	"sync"

	"fifty/ports"
)

type MemoryStore struct {
	mu          sync.RWMutex
	records     map[string]ports.Record
	unavailable bool
}

func NewMemoryStore() *MemoryStore { return &MemoryStore{records: make(map[string]ports.Record)} }

func (s *MemoryStore) SetUnavailable(value bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.unavailable = value
}

func (s *MemoryStore) WithinTransaction(ctx context.Context, fn func(ports.Transaction) error) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.unavailable {
		return ports.ErrUnavailable
	}
	working := cloneRecords(s.records)
	tx := &memoryTransaction{records: working}
	if err := fn(tx); err != nil {
		return err
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	s.records = working
	return nil
}

func (s *MemoryStore) Read(workspaceID, kind, id string) (ports.Record, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.unavailable {
		return ports.Record{}, ports.ErrUnavailable
	}
	record, ok := s.records[key(workspaceID, kind, id)]
	if !ok {
		return ports.Record{}, ports.ErrNotFound
	}
	return cloneRecord(record), nil
}

type memoryTransaction struct{ records map[string]ports.Record }

func (tx *memoryTransaction) Load(ctx context.Context, workspaceID, kind, id string) (ports.Record, error) {
	if err := ctx.Err(); err != nil {
		return ports.Record{}, err
	}
	record, ok := tx.records[key(workspaceID, kind, id)]
	if !ok {
		return ports.Record{}, ports.ErrNotFound
	}
	return cloneRecord(record), nil
}

func (tx *memoryTransaction) Insert(ctx context.Context, record ports.Record) error {
	if err := validateRecord(ctx, record); err != nil {
		return err
	}
	k := key(record.WorkspaceID, record.Kind, record.ID)
	if _, exists := tx.records[k]; exists {
		return ports.ErrConflict
	}
	if record.Version != 0 {
		return ports.ErrInvalid
	}
	record.Version = 1
	tx.records[k] = cloneRecord(record)
	return nil
}

func (tx *memoryTransaction) ReplaceIfVersion(ctx context.Context, record ports.Record, expected uint64) error {
	if err := validateRecord(ctx, record); err != nil {
		return err
	}
	k := key(record.WorkspaceID, record.Kind, record.ID)
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
	record.Version = expected + 1
	tx.records[k] = cloneRecord(record)
	return nil
}

func (tx *memoryTransaction) DeleteIfVersion(ctx context.Context, workspaceID, kind, id string, expected uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	k := key(workspaceID, kind, id)
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

func validateRecord(ctx context.Context, record ports.Record) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if record.WorkspaceID == "" || record.Kind == "" || record.ID == "" {
		return ports.ErrInvalid
	}
	return nil
}

func key(workspaceID, kind, id string) string { return workspaceID + "\x00" + kind + "\x00" + id }

func cloneRecords(source map[string]ports.Record) map[string]ports.Record {
	result := make(map[string]ports.Record, len(source))
	for k, record := range source {
		result[k] = cloneRecord(record)
	}
	return result
}

func cloneRecord(record ports.Record) ports.Record {
	record.Payload = append([]byte(nil), record.Payload...)
	return record
}
