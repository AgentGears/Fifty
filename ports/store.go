package ports

import (
	"context"
	"errors"
)

var (
	ErrNotFound    = errors.New("canonical record not found")
	ErrConflict    = errors.New("canonical record version conflict")
	ErrUnavailable = errors.New("canonical store unavailable")
	ErrInvalid     = errors.New("invalid canonical record")
)

type Record struct {
	WorkspaceID string
	Kind        string
	ID          string
	Version     uint64
	Payload     []byte
}

type Store interface {
	WithinTransaction(context.Context, func(Transaction) error) error
}

type Transaction interface {
	Load(context.Context, string, string, string) (Record, error)
	Insert(context.Context, Record) error
	ReplaceIfVersion(context.Context, Record, uint64) error
	DeleteIfVersion(context.Context, string, string, string, uint64) error
}
