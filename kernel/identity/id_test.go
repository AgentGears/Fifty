package identity

import "testing"

func TestParseRoundTrip(t *testing.T) {
	original := "00112233445566778899aabbccddeeff"
	id, err := Parse(original)
	if err != nil {
		t.Fatalf("parse: %v", err)
	}
	if got := id.String(); got != original {
		t.Fatalf("round trip: got %q want %q", got, original)
	}
}

func TestParseRejectsMalformedOrZeroValue(t *testing.T) {
	for _, value := range []string{"", "0011", "not-an-identifier", "00112233445566778899aabbccddeefg", "00000000000000000000000000000000"} {
		if _, err := Parse(value); err == nil {
			t.Fatalf("expected %q to be rejected", value)
		}
	}
}

func TestRandomGeneratorProducesNonZeroIdentifier(t *testing.T) {
	id, err := (RandomGenerator{}).New()
	if err != nil {
		t.Fatalf("new id: %v", err)
	}
	if id.IsZero() {
		t.Fatal("random identifier must not be zero")
	}
}
