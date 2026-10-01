package conversation

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sort"

	"fifty/kernel/clock"
	"fifty/kernel/identity"
	"fifty/ports"
	"fifty/workspace"
)

const maxProjectionInteractions = 256

type Service struct {
	store      ports.Store
	ids        identity.Generator
	clock      clock.Clock
	workspaces *workspace.Service
}

func NewService(store ports.Store, ids identity.Generator, source clock.Clock, workspaces *workspace.Service) (*Service, error) {
	if store == nil || ids == nil || source == nil || workspaces == nil {
		return nil, ports.ErrInvalid
	}
	return &Service{store: store, ids: ids, clock: source, workspaces: workspaces}, nil
}

func (s *Service) Admit(
	ctx context.Context,
	workspaceID identity.ID,
	authorPrincipalID identity.ID,
	direction Direction,
	contentRef string,
	classification DataClassification,
	semanticRefs []SemanticReference,
) (Interaction, error) {
	if workspaceID.IsZero() || authorPrincipalID.IsZero() || contentRef == "" {
		return Interaction{}, ports.ErrInvalid
	}
	if classification == DataD3 {
		return Interaction{}, ErrD3NotAdmitted
	}
	if classification == DataD2 {
		return Interaction{}, ErrD2AdmissionRequired
	}
	workspaceState, human, assistant, err := s.workspaces.Load(ctx, workspaceID)
	if err != nil {
		return Interaction{}, err
	}
	if workspaceState.State != workspace.WorkspaceActive {
		return Interaction{}, ErrAuthorMismatch
	}
	if err := validateAuthor(authorPrincipalID, direction, human, assistant); err != nil {
		return Interaction{}, err
	}
	for _, reference := range semanticRefs {
		if err := validateSemanticReference(reference); err != nil {
			return Interaction{}, err
		}
	}

	var admitted Interaction
	err = s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		state, record, exists, err := loadHistory(ctx, tx, workspaceID)
		if err != nil {
			return err
		}
		id, err := s.newDistinctInteractionID(state, workspaceID, human.ID, assistant.ID)
		if err != nil {
			return err
		}
		admitted = Interaction{
			ID:                 id,
			WorkspaceID:        workspaceID,
			AuthorPrincipalID:  authorPrincipalID,
			Direction:          direction,
			AdmittedAt:         s.clock.Now().UTC(),
			ContentRef:         contentRef,
			DataClassification: classification,
			SemanticRefs:       append([]SemanticReference(nil), semanticRefs...),
			DeletionState:      InteractionRecorded,
			Version:            1,
		}
		if err := validateInteraction(admitted); err != nil {
			return err
		}
		state.WorkspaceID = workspaceID
		if exists {
			if record.Version == math.MaxUint64 {
				return ports.ErrVersionExhausted
			}
			state.Generation = record.Version + 1
		} else {
			state.Generation = 1
		}
		state.Interactions = append(state.Interactions, cloneInteraction(admitted))
		payload, err := encodeHistory(state)
		if err != nil {
			return err
		}
		if !exists {
			return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: historyRecordKind, ID: workspaceID, Payload: payload})
		}
		return tx.ReplaceIfVersion(ctx, ports.Record{
			WorkspaceID: workspaceID,
			Kind:        historyRecordKind,
			ID:          workspaceID,
			Version:     record.Version,
			Payload:     payload,
		}, record.Version)
	})
	if err != nil {
		if errors.Is(err, ports.ErrCommitUncertain) && !admitted.ID.IsZero() {
			return cloneInteraction(admitted), err
		}
		return Interaction{}, err
	}
	return cloneInteraction(admitted), nil
}

func (s *Service) LoadInteraction(ctx context.Context, workspaceID, interactionID identity.ID) (Interaction, error) {
	if workspaceID.IsZero() || interactionID.IsZero() {
		return Interaction{}, ports.ErrInvalid
	}
	var result Interaction
	err := s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		state, _, exists, err := loadHistory(ctx, tx, workspaceID)
		if err != nil {
			return err
		}
		if !exists {
			return ports.ErrNotFound
		}
		index := findInteraction(state, interactionID)
		if index < 0 {
			return ports.ErrNotFound
		}
		result = cloneInteraction(state.Interactions[index])
		return nil
	})
	if err != nil {
		return Interaction{}, err
	}
	return result, nil
}

func (s *Service) ProjectContext(ctx context.Context, workspaceID identity.ID, policy ProjectionPolicy) (ContextView, error) {
	if workspaceID.IsZero() || policy.MaxInteractions <= 0 || policy.MaxInteractions > maxProjectionInteractions {
		return ContextView{}, ports.ErrInvalid
	}
	workspaceState, _, _, err := s.workspaces.Load(ctx, workspaceID)
	if err != nil {
		return ContextView{}, err
	}
	if workspaceState.State != workspace.WorkspaceActive {
		return ContextView{}, ports.ErrUnavailable
	}

	var state historyState
	var exists bool
	err = s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		var loadErr error
		state, _, exists, loadErr = loadHistory(ctx, tx, workspaceID)
		return loadErr
	})
	if err != nil {
		return ContextView{}, err
	}
	if !exists {
		return ContextView{WorkspaceID: workspaceID, Generation: "0", Interactions: []ContextInteraction{}}, nil
	}

	eligible := make([]Interaction, 0, len(state.Interactions))
	for _, interaction := range state.Interactions {
		if interaction.DeletionState != InteractionRecorded {
			continue
		}
		if interaction.DataClassification == DataD2 {
			continue
		}
		eligible = append(eligible, cloneInteraction(interaction))
	}
	sort.Slice(eligible, func(i, j int) bool {
		if eligible[i].AdmittedAt.Equal(eligible[j].AdmittedAt) {
			return eligible[i].ID.String() < eligible[j].ID.String()
		}
		return eligible[i].AdmittedAt.Before(eligible[j].AdmittedAt)
	})
	if len(eligible) > policy.MaxInteractions {
		eligible = eligible[len(eligible)-policy.MaxInteractions:]
	}
	view := ContextView{
		WorkspaceID:  workspaceID,
		Generation:   contextGeneration(state.Generation, policy, eligible),
		Interactions: make([]ContextInteraction, 0, len(eligible)),
	}
	for _, interaction := range eligible {
		view.Interactions = append(view.Interactions, ContextInteraction{
			ID:                 interaction.ID,
			AuthorPrincipalID:  interaction.AuthorPrincipalID,
			Direction:          interaction.Direction,
			AdmittedAt:         interaction.AdmittedAt,
			ContentRef:         interaction.ContentRef,
			DataClassification: interaction.DataClassification,
			SemanticRefs:       append([]SemanticReference(nil), interaction.SemanticRefs...),
			Version:            interaction.Version,
		})
	}
	return view, nil
}

func loadHistory(ctx context.Context, tx ports.Transaction, workspaceID identity.ID) (historyState, ports.Record, bool, error) {
	record, err := tx.Load(ctx, workspaceID, historyRecordKind, workspaceID)
	if errors.Is(err, ports.ErrNotFound) {
		return historyState{WorkspaceID: workspaceID}, ports.Record{}, false, nil
	}
	if err != nil {
		return historyState{}, ports.Record{}, false, err
	}
	state, err := decodeHistory(record)
	if err != nil {
		return historyState{}, ports.Record{}, false, err
	}
	return state, record, true, nil
}

func findInteraction(state historyState, interactionID identity.ID) int {
	for index := range state.Interactions {
		if state.Interactions[index].ID == interactionID {
			return index
		}
	}
	return -1
}

func (s *Service) newDistinctInteractionID(state historyState, reserved ...identity.ID) (identity.ID, error) {
	const maxAttempts = 8
	for attempt := 0; attempt < maxAttempts; attempt++ {
		id, err := s.ids.New()
		if err != nil {
			return identity.ID{}, err
		}
		if id.IsZero() {
			continue
		}
		duplicate := false
		for _, current := range reserved {
			if id == current {
				duplicate = true
				break
			}
		}
		if duplicate {
			continue
		}
		if findInteraction(state, id) < 0 {
			return id, nil
		}
	}
	return identity.ID{}, ports.ErrInvalid
}

func validateAuthor(author identity.ID, direction Direction, human, assistant workspace.Principal) error {
	switch direction {
	case DirectionUserToAssistant:
		if human.State != workspace.PrincipalActive || author != human.ID {
			return ErrAuthorMismatch
		}
	case DirectionAssistantToUser:
		if assistant.State != workspace.PrincipalActive || author != assistant.ID {
			return ErrAuthorMismatch
		}
	default:
		return ports.ErrInvalid
	}
	return nil
}

func contextGeneration(generation uint64, policy ProjectionPolicy, interactions []Interaction) string {
	hash := sha256.New()
	_, _ = fmt.Fprintf(hash, "history=%d|max=%d|", generation, policy.MaxInteractions)
	for _, interaction := range interactions {
		_, _ = fmt.Fprintf(hash, "%s:%d|", interaction.ID.String(), interaction.Version)
	}
	sum := hash.Sum(nil)
	return hex.EncodeToString(sum[:16])
}
