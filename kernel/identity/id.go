package identity

import (
	"crypto/rand"
	"encoding/hex"
	"errors"
)

const Size = 16

var ErrInvalid = errors.New("invalid semantic identifier")

type ID [Size]byte

type Generator interface {
	New() (ID, error)
}

type RandomGenerator struct{}

func (RandomGenerator) New() (ID, error) {
	for {
		var id ID
		if _, err := rand.Read(id[:]); err != nil {
			return ID{}, err
		}
		if !id.IsZero() {
			return id, nil
		}
	}
}

func Parse(value string) (ID, error) {
	if len(value) != Size*2 {
		return ID{}, ErrInvalid
	}
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != Size {
		return ID{}, ErrInvalid
	}
	var id ID
	copy(id[:], decoded)
	if id.IsZero() {
		return ID{}, ErrInvalid
	}
	return id, nil
}

func (id ID) String() string { return hex.EncodeToString(id[:]) }
func (id ID) IsZero() bool   { return id == ID{} }
