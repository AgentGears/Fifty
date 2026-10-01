package conversation

import (
	"bytes"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fifty/kernel/identity"
	"fifty/ports"
)

const historyRecordKind = "conversation-history"

var (
	ErrInvalidCanonicalState = errors.New("invalid conversation canonical state")
	ErrD2AdmissionRequired   = errors.New("D2 data requires explicit admission")
	ErrD3NotAdmitted         = errors.New("D3 data is not admitted to ordinary conversation state")
	ErrAuthorMismatch        = errors.New("interaction author does not match direction or workspace actor")
	ErrReferenceUnresolved   = errors.New("semantic reference could not be resolved exactly")
	ErrReferenceAmbiguous    = errors.New("semantic reference is ambiguous")
)

type Direction string

const (
	DirectionUserToAssistant Direction = "USER_TO_ASSISTANT"
	DirectionAssistantToUser Direction = "ASSISTANT_TO_USER"
)

type DataClassification string

const (
	DataD0 DataClassification = "D0"
	DataD1 DataClassification = "D1"
	DataD2 DataClassification = "D2"
	DataD3 DataClassification = "D3"
)

type DeletionState string

const (
	InteractionRecorded DeletionState = "RECORDED"
	InteractionDeleted  DeletionState = "DELETED"
)

type SemanticReference struct {
	Kind string
	ID   identity.ID
}

type Interaction struct {
	ID                 identity.ID
	WorkspaceID        identity.ID
	AuthorPrincipalID  identity.ID
	Direction          Direction
	AdmittedAt         time.Time
	ContentRef         string
	DataClassification DataClassification
	SemanticRefs       []SemanticReference
	DeletionState      DeletionState
	Version            uint64
}

type ContextInteraction struct {
	ID                 identity.ID
	AuthorPrincipalID  identity.ID
	Direction          Direction
	AdmittedAt         time.Time
	ContentRef         string
	DataClassification DataClassification
	SemanticRefs       []SemanticReference
	Version            uint64
}

type ContextView struct {
	WorkspaceID  identity.ID
	Generation   string
	Interactions []ContextInteraction
}

type ProjectionPolicy struct {
	MaxInteractions int
}

type ReferenceCandidate struct {
	Label     string
	Reference SemanticReference
}

type historyState struct {
	WorkspaceID  identity.ID
	Generation   uint64
	Interactions []Interaction
}

type historyWire struct {
	WorkspaceID  string            `json:"workspace_id"`
	Generation   uint64            `json:"generation"`
	Interactions []interactionWire `json:"interactions"`
}

type interactionWire struct {
	InteractionID      string                  `json:"interaction_id"`
	WorkspaceID        string                  `json:"workspace_id"`
	AuthorPrincipalID  string                  `json:"author_principal_id"`
	Direction          Direction               `json:"direction"`
	AdmittedAt         string                  `json:"admitted_at"`
	ContentRef         string                  `json:"content_ref"`
	DataClassification DataClassification      `json:"data_classification"`
	SemanticRefs       []semanticReferenceWire `json:"semantic_refs"`
	DeletionState      DeletionState           `json:"deletion_state"`
	Version            uint64                  `json:"version"`
}

type semanticReferenceWire struct {
	Kind string `json:"kind"`
	ID   string `json:"id"`
}

func ResolveExactReference(label string, candidates []ReferenceCandidate) (SemanticReference, error) {
	if label == "" {
		return SemanticReference{}, ErrReferenceUnresolved
	}
	matches := make(map[string]SemanticReference)
	for _, candidate := range candidates {
		if candidate.Label == "" || validateSemanticReference(candidate.Reference) != nil {
			return SemanticReference{}, ErrInvalidCanonicalState
		}
		if candidate.Label != label {
			continue
		}
		key := candidate.Reference.Kind + "\x00" + candidate.Reference.ID.String()
		matches[key] = candidate.Reference
	}
	if len(matches) == 0 {
		return SemanticReference{}, ErrReferenceUnresolved
	}
	if len(matches) > 1 {
		return SemanticReference{}, ErrReferenceAmbiguous
	}
	for _, match := range matches {
		return match, nil
	}
	return SemanticReference{}, ErrReferenceUnresolved
}

func encodeHistory(state historyState) ([]byte, error) {
	if err := validateHistory(state); err != nil {
		return nil, err
	}
	wire := historyWire{
		WorkspaceID:  state.WorkspaceID.String(),
		Generation:   state.Generation,
		Interactions: make([]interactionWire, 0, len(state.Interactions)),
	}
	for _, interaction := range state.Interactions {
		entry := interactionWire{
			InteractionID:      interaction.ID.String(),
			WorkspaceID:        interaction.WorkspaceID.String(),
			AuthorPrincipalID:  interaction.AuthorPrincipalID.String(),
			Direction:          interaction.Direction,
			AdmittedAt:         interaction.AdmittedAt.UTC().Format(time.RFC3339Nano),
			ContentRef:         interaction.ContentRef,
			DataClassification: interaction.DataClassification,
			DeletionState:      interaction.DeletionState,
			Version:            interaction.Version,
			SemanticRefs:       make([]semanticReferenceWire, 0, len(interaction.SemanticRefs)),
		}
		for _, reference := range interaction.SemanticRefs {
			entry.SemanticRefs = append(entry.SemanticRefs, semanticReferenceWire{Kind: reference.Kind, ID: reference.ID.String()})
		}
		wire.Interactions = append(wire.Interactions, entry)
	}
	return json.Marshal(wire)
}

func decodeHistory(record ports.Record) (historyState, error) {
	if record.Kind != historyRecordKind || record.WorkspaceID.IsZero() || record.ID != record.WorkspaceID || record.Version == 0 {
		return historyState{}, ErrInvalidCanonicalState
	}
	var wire historyWire
	if err := decodeStrictJSON(record.Payload, &wire); err != nil {
		return historyState{}, ErrInvalidCanonicalState
	}
	workspaceID, err := identity.Parse(wire.WorkspaceID)
	if err != nil || workspaceID != record.WorkspaceID || wire.Generation != record.Version {
		return historyState{}, ErrInvalidCanonicalState
	}
	state := historyState{WorkspaceID: workspaceID, Generation: wire.Generation, Interactions: make([]Interaction, 0, len(wire.Interactions))}
	for _, entry := range wire.Interactions {
		interactionID, err := identity.Parse(entry.InteractionID)
		if err != nil {
			return historyState{}, ErrInvalidCanonicalState
		}
		entryWorkspaceID, err := identity.Parse(entry.WorkspaceID)
		if err != nil || entryWorkspaceID != workspaceID {
			return historyState{}, ErrInvalidCanonicalState
		}
		authorID, err := identity.Parse(entry.AuthorPrincipalID)
		if err != nil {
			return historyState{}, ErrInvalidCanonicalState
		}
		admittedAt, err := time.Parse(time.RFC3339Nano, entry.AdmittedAt)
		if err != nil {
			return historyState{}, ErrInvalidCanonicalState
		}
		interaction := Interaction{
			ID:                 interactionID,
			WorkspaceID:        entryWorkspaceID,
			AuthorPrincipalID:  authorID,
			Direction:          entry.Direction,
			AdmittedAt:         admittedAt.UTC(),
			ContentRef:         entry.ContentRef,
			DataClassification: entry.DataClassification,
			DeletionState:      entry.DeletionState,
			Version:            entry.Version,
			SemanticRefs:       make([]SemanticReference, 0, len(entry.SemanticRefs)),
		}
		for _, referenceWire := range entry.SemanticRefs {
			referenceID, err := identity.Parse(referenceWire.ID)
			if err != nil {
				return historyState{}, ErrInvalidCanonicalState
			}
			interaction.SemanticRefs = append(interaction.SemanticRefs, SemanticReference{Kind: referenceWire.Kind, ID: referenceID})
		}
		state.Interactions = append(state.Interactions, interaction)
	}
	if err := validateHistory(state); err != nil {
		return historyState{}, err
	}
	return state, nil
}

func validateHistory(state historyState) error {
	if state.WorkspaceID.IsZero() || state.Generation == 0 {
		return ErrInvalidCanonicalState
	}
	seen := make(map[identity.ID]struct{}, len(state.Interactions))
	for _, interaction := range state.Interactions {
		if interaction.WorkspaceID != state.WorkspaceID || validateInteraction(interaction) != nil {
			return ErrInvalidCanonicalState
		}
		if _, exists := seen[interaction.ID]; exists {
			return ErrInvalidCanonicalState
		}
		seen[interaction.ID] = struct{}{}
	}
	return nil
}

func validateInteraction(value Interaction) error {
	if value.ID.IsZero() || value.WorkspaceID.IsZero() || value.AuthorPrincipalID.IsZero() ||
		value.ID == value.WorkspaceID || value.AuthorPrincipalID == value.WorkspaceID || value.Version == 0 || value.AdmittedAt.IsZero() {
		return ErrInvalidCanonicalState
	}
	switch value.Direction {
	case DirectionUserToAssistant, DirectionAssistantToUser:
	default:
		return ErrInvalidCanonicalState
	}
	switch value.DataClassification {
	case DataD0, DataD1, DataD2:
	case DataD3:
		return ErrD3NotAdmitted
	default:
		return ErrInvalidCanonicalState
	}
	switch value.DeletionState {
	case InteractionRecorded:
		if value.ContentRef == "" {
			return ErrInvalidCanonicalState
		}
	case InteractionDeleted:
		if value.ContentRef != "" || len(value.SemanticRefs) != 0 {
			return ErrInvalidCanonicalState
		}
	default:
		return ErrInvalidCanonicalState
	}
	seen := make(map[string]struct{}, len(value.SemanticRefs))
	for _, reference := range value.SemanticRefs {
		if err := validateSemanticReference(reference); err != nil {
			return err
		}
		key := reference.Kind + "\x00" + reference.ID.String()
		if _, exists := seen[key]; exists {
			return ErrInvalidCanonicalState
		}
		seen[key] = struct{}{}
	}
	return nil
}

func validateSemanticReference(value SemanticReference) error {
	if value.Kind == "" || value.ID.IsZero() {
		return ErrInvalidCanonicalState
	}
	return nil
}

func cloneInteraction(value Interaction) Interaction {
	value.SemanticRefs = append([]SemanticReference(nil), value.SemanticRefs...)
	return value
}

func decodeStrictJSON(data []byte, target interface{}) error {
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra interface{}
	if err := decoder.Decode(&extra); !errors.Is(err, io.EOF) {
		return ErrInvalidCanonicalState
	}
	return nil
}
