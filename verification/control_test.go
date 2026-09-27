package verification

import (
	"errors"
	"testing"
	"time"

	"fifty/kernel/identity"
)

func TestControlledClock(t *testing.T) {
	start := time.Date(2026, 9, 27, 19, 0, 0, 0, time.UTC)
	clock := NewControlledClock(start)
	clock.Advance(90 * time.Second)
	want := start.Add(90 * time.Second)
	if got := clock.Now(); !got.Equal(want) {
		t.Fatalf("now: got %v want %v", got, want)
	}
}

func TestControlledIdentifiers(t *testing.T) {
	first, _ := identity.Parse("00000000000000000000000000000001")
	second, _ := identity.Parse("00000000000000000000000000000002")
	ids := NewControlledIdentifiers(first, second)
	got, err := ids.New()
	if err != nil || got != first {
		t.Fatalf("first: got %v err %v", got, err)
	}
	got, err = ids.New()
	if err != nil || got != second {
		t.Fatalf("second: got %v err %v", got, err)
	}
	if _, err = ids.New(); !errors.Is(err, ErrIdentifierSequenceExhausted) {
		t.Fatalf("exhaustion: got %v", err)
	}
}
