package verification

import (
	"errors"
	"sync"
	"time"

	"fifty/kernel/identity"
)

type ControlledClock struct {
	mu  sync.RWMutex
	now time.Time
}

func NewControlledClock(start time.Time) *ControlledClock { return &ControlledClock{now: start.UTC()} }
func (c *ControlledClock) Now() time.Time                 { c.mu.RLock(); defer c.mu.RUnlock(); return c.now }
func (c *ControlledClock) Set(value time.Time)            { c.mu.Lock(); defer c.mu.Unlock(); c.now = value.UTC() }
func (c *ControlledClock) Advance(delta time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.now = c.now.Add(delta)
}

var ErrIdentifierSequenceExhausted = errors.New("controlled identifier sequence exhausted")

type ControlledIdentifiers struct {
	mu    sync.Mutex
	items []identity.ID
	next  int
}

func NewControlledIdentifiers(values ...identity.ID) *ControlledIdentifiers {
	return &ControlledIdentifiers{items: append([]identity.ID(nil), values...)}
}

func (g *ControlledIdentifiers) New() (identity.ID, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.next >= len(g.items) {
		return identity.ID{}, ErrIdentifierSequenceExhausted
	}
	value := g.items[g.next]
	g.next++
	return value, nil
}
