package ports

import (
	"context"
	"errors"

	"fifty/kernel/identity"
)

var (
	ErrNotFound            = errors.New("canonical record not found")
	ErrConflict            = errors.New("canonical record version conflict")
	ErrUnavailable         = errors.New("canonical store unavailable")
	ErrInvalid             = errors.New("invalid canonical record")
	ErrVersionExhausted    = errors.New("canonical record version exhausted")
	ErrTransactionClosed   = errors.New("canonical transaction is closed")
	ErrTransactionInFlight = errors.New("canonical transaction has an in-flight operation at close")
)

type Record struct {
	WorkspaceID identity.ID
	Kind        string
	ID          identity.ID
	Version     uint64
	Payload     []byte
}

type Store interface {
	WithinTransaction(context.Context, func(Transaction) error) error
}

type Transaction interface {
	Load(context.Context, identity.ID, string, identity.ID) (Record, error)
	Insert(context.Context, Record) error
	ReplaceIfVersion(context.Context, Record, uint64) error
	DeleteIfVersion(context.Context, identity.ID, string, identity.ID, uint64) error
}
