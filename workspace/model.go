package workspace

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"time"

	"fifty/kernel/identity"
	"fifty/ports"
)

const (
	WorkspaceRecordKind = "workspace"
	PrincipalRecordKind = "principal"
)

var ErrInvalidCanonicalState = errors.New("invalid workspace canonical state")

type WorkspaceState string

const (
	WorkspaceActive    WorkspaceState = "ACTIVE"
	WorkspaceSuspended WorkspaceState = "SUSPENDED"
	WorkspaceDeleting  WorkspaceState = "DELETING"
	WorkspaceDeleted   WorkspaceState = "DELETED"
)

type PrincipalKind string

const (
	PrincipalHuman            PrincipalKind = "HUMAN"
	PrincipalPrimaryAssistant PrincipalKind = "PRIMARY_ASSISTANT"
	PrincipalSystemControl    PrincipalKind = "SYSTEM_CONTROL"
)

type PrincipalState string

const (
	PrincipalActive    PrincipalState = "ACTIVE"
	PrincipalSuspended PrincipalState = "SUSPENDED"
	PrincipalRetired   PrincipalState = "RETIRED"
)

type digest [32]byte

type Workspace struct {
	ID                          identity.ID
	HumanPrincipalID            identity.ID
	PrimaryAssistantPrincipalID identity.ID
	State                       WorkspaceState
	CreatedAt                   time.Time
	RetentionPolicyRef          string
	Version                     uint64
	accessDigest                digest
	recoveryDigest              digest
	AccessGeneration            uint64
	RecoveryGeneration          uint64
}

type Principal struct {
	ID          identity.ID
	WorkspaceID identity.ID
	Kind        PrincipalKind
	State       PrincipalState
	CreatedAt   time.Time
	RetiredAt   *time.Time
	Version     uint64
}

type workspaceWire struct {
	WorkspaceID                  string         `json:"workspace_id"`
	HumanPrincipalID             string         `json:"human_principal_id"`
	PrimaryAssistantPrincipalID  string         `json:"primary_assistant_principal_id"`
	State                        WorkspaceState `json:"state"`
	CreatedAt                    string         `json:"created_at"`
	RetentionPolicyRef           string         `json:"retention_policy_ref"`
	Version                      uint64         `json:"version"`
	AccessCredentialDigest       string         `json:"access_credential_digest"`
	RecoveryCredentialDigest     string         `json:"recovery_credential_digest"`
	AccessCredentialGeneration   uint64         `json:"access_credential_generation"`
	RecoveryCredentialGeneration uint64         `json:"recovery_credential_generation"`
}

type principalWire struct {
	PrincipalID string         `json:"principal_id"`
	WorkspaceID string         `json:"workspace_id"`
	Kind        PrincipalKind  `json:"kind"`
	State       PrincipalState `json:"state"`
	CreatedAt   string         `json:"created_at"`
	RetiredAt   *string        `json:"retired_at,omitempty"`
	Version     uint64         `json:"version"`
}

func encodeWorkspace(value Workspace) ([]byte, error) {
	if err := validateWorkspace(value); err != nil {
		return nil, err
	}
	wire := workspaceWire{
		WorkspaceID:                  value.ID.String(),
		HumanPrincipalID:             value.HumanPrincipalID.String(),
		PrimaryAssistantPrincipalID:  value.PrimaryAssistantPrincipalID.String(),
		State:                        value.State,
		CreatedAt:                    value.CreatedAt.UTC().Format(time.RFC3339Nano),
		RetentionPolicyRef:           value.RetentionPolicyRef,
		Version:                      value.Version,
		AccessCredentialDigest:       hex.EncodeToString(value.accessDigest[:]),
		RecoveryCredentialDigest:     hex.EncodeToString(value.recoveryDigest[:]),
		AccessCredentialGeneration:   value.AccessGeneration,
		RecoveryCredentialGeneration: value.RecoveryGeneration,
	}
	return json.Marshal(wire)
}

func decodeWorkspace(record ports.Record) (Workspace, error) {
	if record.Kind != WorkspaceRecordKind || record.WorkspaceID.IsZero() || record.ID != record.WorkspaceID || record.Version == 0 {
		return Workspace{}, ErrInvalidCanonicalState
	}
	var wire workspaceWire
	if err := decodeStrictJSON(record.Payload, &wire); err != nil {
		return Workspace{}, ErrInvalidCanonicalState
	}
	id, err := identity.Parse(wire.WorkspaceID)
	if err != nil || id != record.ID {
		return Workspace{}, ErrInvalidCanonicalState
	}
	humanID, err := identity.Parse(wire.HumanPrincipalID)
	if err != nil {
		return Workspace{}, ErrInvalidCanonicalState
	}
	assistantID, err := identity.Parse(wire.PrimaryAssistantPrincipalID)
	if err != nil {
		return Workspace{}, ErrInvalidCanonicalState
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil {
		return Workspace{}, ErrInvalidCanonicalState
	}
	accessDigest, err := parseDigest(wire.AccessCredentialDigest)
	if err != nil {
		return Workspace{}, err
	}
	recoveryDigest, err := parseDigest(wire.RecoveryCredentialDigest)
	if err != nil {
		return Workspace{}, err
	}
	value := Workspace{
		ID:                          id,
		HumanPrincipalID:            humanID,
		PrimaryAssistantPrincipalID: assistantID,
		State:                       wire.State,
		CreatedAt:                   createdAt.UTC(),
		RetentionPolicyRef:          wire.RetentionPolicyRef,
		Version:                     wire.Version,
		accessDigest:                accessDigest,
		recoveryDigest:              recoveryDigest,
		AccessGeneration:            wire.AccessCredentialGeneration,
		RecoveryGeneration:          wire.RecoveryCredentialGeneration,
	}
	if value.Version != record.Version || validateWorkspace(value) != nil {
		return Workspace{}, ErrInvalidCanonicalState
	}
	return value, nil
}

func encodePrincipal(value Principal) ([]byte, error) {
	if err := validatePrincipal(value); err != nil {
		return nil, err
	}
	var retiredAt *string
	if value.RetiredAt != nil {
		formatted := value.RetiredAt.UTC().Format(time.RFC3339Nano)
		retiredAt = &formatted
	}
	wire := principalWire{
		PrincipalID: value.ID.String(),
		WorkspaceID: value.WorkspaceID.String(),
		Kind:        value.Kind,
		State:       value.State,
		CreatedAt:   value.CreatedAt.UTC().Format(time.RFC3339Nano),
		RetiredAt:   retiredAt,
		Version:     value.Version,
	}
	return json.Marshal(wire)
}

func decodePrincipal(record ports.Record) (Principal, error) {
	if record.Kind != PrincipalRecordKind || record.WorkspaceID.IsZero() || record.ID.IsZero() || record.Version == 0 {
		return Principal{}, ErrInvalidCanonicalState
	}
	var wire principalWire
	if err := decodeStrictJSON(record.Payload, &wire); err != nil {
		return Principal{}, ErrInvalidCanonicalState
	}
	id, err := identity.Parse(wire.PrincipalID)
	if err != nil || id != record.ID {
		return Principal{}, ErrInvalidCanonicalState
	}
	workspaceID, err := identity.Parse(wire.WorkspaceID)
	if err != nil || workspaceID != record.WorkspaceID {
		return Principal{}, ErrInvalidCanonicalState
	}
	createdAt, err := time.Parse(time.RFC3339Nano, wire.CreatedAt)
	if err != nil {
		return Principal{}, ErrInvalidCanonicalState
	}
	var retiredAt *time.Time
	if wire.RetiredAt != nil {
		parsed, err := time.Parse(time.RFC3339Nano, *wire.RetiredAt)
		if err != nil {
			return Principal{}, ErrInvalidCanonicalState
		}
		parsed = parsed.UTC()
		retiredAt = &parsed
	}
	value := Principal{
		ID:          id,
		WorkspaceID: workspaceID,
		Kind:        wire.Kind,
		State:       wire.State,
		CreatedAt:   createdAt.UTC(),
		RetiredAt:   retiredAt,
		Version:     wire.Version,
	}
	if value.Version != record.Version || validatePrincipal(value) != nil {
		return Principal{}, ErrInvalidCanonicalState
	}
	return value, nil
}

func validateWorkspace(value Workspace) error {
	if value.ID.IsZero() || value.HumanPrincipalID.IsZero() || value.PrimaryAssistantPrincipalID.IsZero() ||
		value.ID == value.HumanPrincipalID || value.ID == value.PrimaryAssistantPrincipalID ||
		value.HumanPrincipalID == value.PrimaryAssistantPrincipalID {
		return ErrInvalidCanonicalState
	}
	if value.CreatedAt.IsZero() || value.Version == 0 || value.RetentionPolicyRef == "" || value.AccessGeneration == 0 || value.RecoveryGeneration == 0 {
		return ErrInvalidCanonicalState
	}
	switch value.State {
	case WorkspaceActive, WorkspaceSuspended, WorkspaceDeleting, WorkspaceDeleted:
	default:
		return ErrInvalidCanonicalState
	}
	if value.accessDigest == (digest{}) || value.recoveryDigest == (digest{}) {
		return ErrInvalidCanonicalState
	}
	return nil
}

func validatePrincipal(value Principal) error {
	if value.ID.IsZero() || value.WorkspaceID.IsZero() || value.ID == value.WorkspaceID || value.CreatedAt.IsZero() || value.Version == 0 {
		return ErrInvalidCanonicalState
	}
	switch value.Kind {
	case PrincipalHuman, PrincipalPrimaryAssistant, PrincipalSystemControl:
	default:
		return ErrInvalidCanonicalState
	}
	switch value.State {
	case PrincipalActive, PrincipalSuspended:
		if value.RetiredAt != nil {
			return ErrInvalidCanonicalState
		}
	case PrincipalRetired:
		if value.RetiredAt == nil || value.RetiredAt.Before(value.CreatedAt) {
			return ErrInvalidCanonicalState
		}
	default:
		return ErrInvalidCanonicalState
	}
	return nil
}

func parseDigest(value string) (digest, error) {
	decoded, err := hex.DecodeString(value)
	if err != nil || len(decoded) != 32 {
		return digest{}, ErrInvalidCanonicalState
	}
	var result digest
	copy(result[:], decoded)
	if result == (digest{}) {
		return digest{}, ErrInvalidCanonicalState
	}
	return result, nil
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
