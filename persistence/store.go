package persistence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"sort"
	"sync"

	"fifty/kernel/identity"
	"fifty/ports"
)

const (
	stateFormatVersion = 1
	snapshotFileName   = "snapshot.json"
	journalFileName    = "journal.json"
)

var (
	ErrCorruptState = errors.New("canonical persistence state is corrupt")
	ErrStaleBackup  = errors.New("canonical backup is stale")
)

type recordKey struct {
	workspaceID identity.ID
	kind        string
	id          identity.ID
}

type Store struct {
	dir          string
	gate         chan struct{}
	records      map[recordKey]ports.Record
	generation   uint64
	afterJournal func() error
}

var _ ports.Store = (*Store)(nil)

func Open(directory string) (*Store, error) {
	if directory == "" {
		return nil, ports.ErrInvalid
	}
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return nil, errors.Join(ports.ErrUnavailable, err)
	}
	store := &Store{
		dir:     directory,
		gate:    make(chan struct{}, 1),
		records: make(map[recordKey]ports.Record),
	}
	if err := store.recover(); err != nil {
		return nil, err
	}
	return store, nil
}

func (s *Store) WithinTransaction(ctx context.Context, fn func(ports.Transaction) error) error {
	if fn == nil {
		return ports.ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()

	tx := &fileTransaction{records: cloneRecords(s.records)}
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
	if !tx.isDirty() {
		return nil
	}
	if s.generation == math.MaxUint64 {
		return ports.ErrVersionExhausted
	}

	next := tx.snapshot()
	nextGeneration := s.generation + 1
	if err := s.persistState(next, nextGeneration); err != nil {
		return err
	}
	s.records = next
	s.generation = nextGeneration
	return nil
}

func (s *Store) Backup(ctx context.Context, target string) error {
	if target == "" {
		return ports.ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()
	data, err := encodeEnvelope(s.records, s.generation)
	if err != nil {
		return err
	}
	if err := writeAtomicFile(target, data); err != nil {
		return errors.Join(ports.ErrUnavailable, err)
	}
	return nil
}

func (s *Store) Restore(ctx context.Context, source string) error {
	if source == "" {
		return ports.ErrInvalid
	}
	if err := s.acquire(ctx); err != nil {
		return err
	}
	defer s.release()

	data, err := os.ReadFile(source)
	if err != nil {
		return errors.Join(ports.ErrUnavailable, err)
	}
	records, generation, err := decodeEnvelope(data)
	if err != nil {
		return err
	}
	if generation < s.generation {
		return ErrStaleBackup
	}
	if generation == s.generation {
		if equalRecords(records, s.records) {
			return nil
		}
		return ErrStaleBackup
	}
	if err := s.persistState(records, generation); err != nil {
		return err
	}
	s.records = cloneRecords(records)
	s.generation = generation
	return nil
}

func (s *Store) acquire(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	select {
	case s.gate <- struct{}{}:
		if err := ctx.Err(); err != nil {
			<-s.gate
			return err
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) release() { <-s.gate }

func (s *Store) recover() error {
	snapshotPath := filepath.Join(s.dir, snapshotFileName)
	journalPath := filepath.Join(s.dir, journalFileName)

	records := make(map[recordKey]ports.Record)
	var generation uint64
	if data, err := os.ReadFile(snapshotPath); err == nil {
		decoded, decodedGeneration, decodeErr := decodeEnvelope(data)
		if decodeErr != nil {
			return decodeErr
		}
		records = decoded
		generation = decodedGeneration
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ports.ErrUnavailable, err)
	}

	if data, err := os.ReadFile(journalPath); err == nil {
		if _, _, decodeErr := decodeEnvelope(data); decodeErr != nil {
			return decodeErr
		}
		// The journal is preparation evidence only. The atomically published
		// snapshot is the acceptance boundary, so an unmatched prepared journal
		// is never promoted after restart.
		if removeErr := os.Remove(journalPath); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
			return errors.Join(ports.ErrUnavailable, removeErr)
		}
		if err := syncDirectory(s.dir); err != nil {
			return errors.Join(ports.ErrUnavailable, err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return errors.Join(ports.ErrUnavailable, err)
	}

	s.records = records
	s.generation = generation
	return nil
}

func (s *Store) persistState(records map[recordKey]ports.Record, generation uint64) error {
	data, err := encodeEnvelope(records, generation)
	if err != nil {
		return err
	}
	journalPath := filepath.Join(s.dir, journalFileName)
	if err := writeAtomicFile(journalPath, data); err != nil {
		return errors.Join(ports.ErrUnavailable, err)
	}
	if s.afterJournal != nil {
		if err := s.afterJournal(); err != nil {
			return err
		}
	}
	snapshotPath := filepath.Join(s.dir, snapshotFileName)
	if err := writeAtomicFile(snapshotPath, data); err != nil {
		return errors.Join(ports.ErrUnavailable, err)
	}
	// Snapshot publication is the durable acceptance boundary. Journal cleanup
	// is recovery housekeeping and cannot turn an accepted commit into failure.
	_ = os.Remove(journalPath)
	_ = syncDirectory(s.dir)
	return nil
}

type fileTransaction struct {
	lifecycleMu sync.Mutex
	closed      bool
	active      int
	recordsMu   sync.Mutex
	records     map[recordKey]ports.Record
	dirty       bool
}

func (tx *fileTransaction) Load(ctx context.Context, workspaceID identity.ID, kind string, id identity.ID) (ports.Record, error) {
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

func (tx *fileTransaction) Insert(ctx context.Context, record ports.Record) error {
	if err := tx.begin(ctx); err != nil {
		return err
	}
	defer tx.end()
	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	if err := tx.ensureOpenAfterBegin(ctx); err != nil {
		return err
	}
	if err := validateRecord(record); err != nil || record.Version != 0 {
		return ports.ErrInvalid
	}
	key := recordKey{workspaceID: record.WorkspaceID, kind: record.Kind, id: record.ID}
	if _, exists := tx.records[key]; exists {
		return ports.ErrConflict
	}
	record.Version = 1
	tx.records[key] = cloneRecord(record)
	tx.dirty = true
	return nil
}

func (tx *fileTransaction) ReplaceIfVersion(ctx context.Context, record ports.Record, expected uint64) error {
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
	key := recordKey{workspaceID: record.WorkspaceID, kind: record.Kind, id: record.ID}
	current, exists := tx.records[key]
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
	tx.records[key] = cloneRecord(record)
	tx.dirty = true
	return nil
}

func (tx *fileTransaction) DeleteIfVersion(ctx context.Context, workspaceID identity.ID, kind string, id identity.ID, expected uint64) error {
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
	key := recordKey{workspaceID: workspaceID, kind: kind, id: id}
	current, exists := tx.records[key]
	if !exists {
		return ports.ErrNotFound
	}
	if current.Version != expected {
		return ports.ErrConflict
	}
	delete(tx.records, key)
	tx.dirty = true
	return nil
}

func (tx *fileTransaction) begin(ctx context.Context) error {
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

func (tx *fileTransaction) end() {
	tx.lifecycleMu.Lock()
	defer tx.lifecycleMu.Unlock()
	tx.active--
}

func (tx *fileTransaction) ensureOpenAfterBegin(ctx context.Context) error {
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

func (tx *fileTransaction) close() int {
	tx.lifecycleMu.Lock()
	defer tx.lifecycleMu.Unlock()
	tx.closed = true
	return tx.active
}

func (tx *fileTransaction) snapshot() map[recordKey]ports.Record {
	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	return cloneRecords(tx.records)
}

func (tx *fileTransaction) isDirty() bool {
	tx.recordsMu.Lock()
	defer tx.recordsMu.Unlock()
	return tx.dirty
}

type envelopeWire struct {
	Format   uint64          `json:"format"`
	State    json.RawMessage `json:"state"`
	Checksum string          `json:"checksum"`
}

type stateWire struct {
	Generation uint64       `json:"generation"`
	Records    []recordWire `json:"records"`
}

type recordWire struct {
	WorkspaceID string `json:"workspace_id"`
	Kind        string `json:"kind"`
	ID          string `json:"id"`
	Version     uint64 `json:"version"`
	Payload     []byte `json:"payload"`
}

func encodeEnvelope(records map[recordKey]ports.Record, generation uint64) ([]byte, error) {
	keys := make([]recordKey, 0, len(records))
	for key := range records {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		leftWorkspace := keys[i].workspaceID.String()
		rightWorkspace := keys[j].workspaceID.String()
		if leftWorkspace != rightWorkspace {
			return leftWorkspace < rightWorkspace
		}
		if keys[i].kind != keys[j].kind {
			return keys[i].kind < keys[j].kind
		}
		return keys[i].id.String() < keys[j].id.String()
	})
	state := stateWire{Generation: generation, Records: make([]recordWire, 0, len(keys))}
	for _, key := range keys {
		record := records[key]
		if err := validateRecord(record); err != nil || record.Version == 0 {
			return nil, ErrCorruptState
		}
		state.Records = append(state.Records, recordWire{
			WorkspaceID: record.WorkspaceID.String(),
			Kind:        record.Kind,
			ID:          record.ID.String(),
			Version:     record.Version,
			Payload:     append([]byte(nil), record.Payload...),
		})
	}
	stateBytes, err := json.Marshal(state)
	if err != nil {
		return nil, errors.Join(ErrCorruptState, err)
	}
	checksum := sha256.Sum256(stateBytes)
	return json.Marshal(envelopeWire{
		Format:   stateFormatVersion,
		State:    stateBytes,
		Checksum: hex.EncodeToString(checksum[:]),
	})
}

func decodeEnvelope(data []byte) (map[recordKey]ports.Record, uint64, error) {
	var envelope envelopeWire
	if err := json.Unmarshal(data, &envelope); err != nil || envelope.Format != stateFormatVersion || len(envelope.State) == 0 {
		return nil, 0, ErrCorruptState
	}
	expected, err := hex.DecodeString(envelope.Checksum)
	if err != nil || len(expected) != sha256.Size {
		return nil, 0, ErrCorruptState
	}
	actual := sha256.Sum256(envelope.State)
	if !bytes.Equal(expected, actual[:]) {
		return nil, 0, ErrCorruptState
	}
	var state stateWire
	if err := json.Unmarshal(envelope.State, &state); err != nil {
		return nil, 0, ErrCorruptState
	}
	records := make(map[recordKey]ports.Record, len(state.Records))
	for _, wire := range state.Records {
		workspaceID, err := identity.Parse(wire.WorkspaceID)
		if err != nil {
			return nil, 0, ErrCorruptState
		}
		id, err := identity.Parse(wire.ID)
		if err != nil || wire.Kind == "" || wire.Version == 0 {
			return nil, 0, ErrCorruptState
		}
		key := recordKey{workspaceID: workspaceID, kind: wire.Kind, id: id}
		if _, exists := records[key]; exists {
			return nil, 0, ErrCorruptState
		}
		records[key] = ports.Record{
			WorkspaceID: workspaceID,
			Kind:        wire.Kind,
			ID:          id,
			Version:     wire.Version,
			Payload:     append([]byte(nil), wire.Payload...),
		}
	}
	return records, state.Generation, nil
}

func writeAtomicFile(path string, data []byte) error {
	directory := filepath.Dir(path)
	if err := os.MkdirAll(directory, 0o700); err != nil {
		return err
	}
	temporary, err := os.CreateTemp(directory, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	temporaryPath := temporary.Name()
	cleanup := func() {
		_ = temporary.Close()
		_ = os.Remove(temporaryPath)
	}
	if err := temporary.Chmod(0o600); err != nil {
		cleanup()
		return err
	}
	if _, err := temporary.Write(data); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Sync(); err != nil {
		cleanup()
		return err
	}
	if err := temporary.Close(); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	if err := os.Rename(temporaryPath, path); err != nil {
		_ = os.Remove(temporaryPath)
		return err
	}
	return syncDirectory(directory)
}

func syncDirectory(directory string) error {
	handle, err := os.Open(directory)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
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
	for key, record := range source {
		result[key] = cloneRecord(record)
	}
	return result
}

func cloneRecord(record ports.Record) ports.Record {
	record.Payload = append([]byte(nil), record.Payload...)
	return record
}

func equalRecords(left map[recordKey]ports.Record, right map[recordKey]ports.Record) bool {
	if len(left) != len(right) {
		return false
	}
	for key, leftRecord := range left {
		rightRecord, ok := right[key]
		if !ok || leftRecord.WorkspaceID != rightRecord.WorkspaceID || leftRecord.Kind != rightRecord.Kind || leftRecord.ID != rightRecord.ID || leftRecord.Version != rightRecord.Version || !bytes.Equal(leftRecord.Payload, rightRecord.Payload) {
			return false
		}
	}
	return true
}
