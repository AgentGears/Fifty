package workspace

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/base64"
	"errors"
	"io"
	"math"

	"fifty/kernel/clock"
	"fifty/kernel/identity"
	"fifty/ports"
)

const (
	credentialSecretSize      = 32
	InitialRetentionPolicyRef = "workspace-initial"
	accessCredentialDomain    = "access"
	recoveryCredentialDomain  = "recovery"
)

var (
	ErrAccessDenied      = errors.New("workspace access denied")
	ErrRecoveryDenied    = errors.New("workspace recovery denied")
	ErrCredentialEntropy = errors.New("credential entropy unavailable")
)

type Credentials struct {
	Access   string
	Recovery string
}

type Service struct {
	store   ports.Store
	ids     identity.Generator
	clock   clock.Clock
	entropy io.Reader
}

func NewService(store ports.Store, ids identity.Generator, source clock.Clock) (*Service, error) {
	if store == nil || ids == nil || source == nil {
		return nil, ports.ErrInvalid
	}
	return &Service{store: store, ids: ids, clock: source, entropy: rand.Reader}, nil
}

func (s *Service) Initialize(ctx context.Context) (Workspace, Principal, Principal, Credentials, error) {
	workspaceID, err := s.ids.New()
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}
	humanID, err := s.ids.New()
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}
	assistantID, err := s.ids.New()
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}
	if workspaceID.IsZero() || humanID.IsZero() || assistantID.IsZero() || humanID == assistantID {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, ports.ErrInvalid
	}

	accessToken, accessDigest, err := s.issueToken(workspaceID, accessCredentialDomain)
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}
	recoveryToken, recoveryDigest, err := s.issueToken(workspaceID, recoveryCredentialDomain)
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}

	now := s.clock.Now().UTC()
	workspace := Workspace{
		ID:                          workspaceID,
		HumanPrincipalID:            humanID,
		PrimaryAssistantPrincipalID: assistantID,
		State:                       WorkspaceActive,
		CreatedAt:                   now,
		RetentionPolicyRef:          InitialRetentionPolicyRef,
		Version:                     1,
		accessDigest:                accessDigest,
		recoveryDigest:              recoveryDigest,
		AccessGeneration:            1,
		RecoveryGeneration:          1,
	}
	human := Principal{
		ID:          humanID,
		WorkspaceID: workspaceID,
		Kind:        PrincipalHuman,
		State:       PrincipalActive,
		CreatedAt:   now,
		Version:     1,
	}
	assistant := Principal{
		ID:          assistantID,
		WorkspaceID: workspaceID,
		Kind:        PrincipalPrimaryAssistant,
		State:       PrincipalActive,
		CreatedAt:   now,
		Version:     1,
	}

	workspacePayload, err := encodeWorkspace(workspace)
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}
	humanPayload, err := encodePrincipal(human)
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}
	assistantPayload, err := encodePrincipal(assistant)
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}

	err = s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		if err := tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: WorkspaceRecordKind, ID: workspaceID, Payload: workspacePayload}); err != nil {
			return err
		}
		if err := tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: PrincipalRecordKind, ID: humanID, Payload: humanPayload}); err != nil {
			return err
		}
		return tx.Insert(ctx, ports.Record{WorkspaceID: workspaceID, Kind: PrincipalRecordKind, ID: assistantID, Payload: assistantPayload})
	})
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, Credentials{}, err
	}

	return workspace, human, assistant, Credentials{Access: accessToken, Recovery: recoveryToken}, nil
}

func (s *Service) Load(ctx context.Context, workspaceID identity.ID) (Workspace, Principal, Principal, error) {
	if workspaceID.IsZero() {
		return Workspace{}, Principal{}, Principal{}, ports.ErrInvalid
	}
	var workspace Workspace
	var human Principal
	var assistant Principal
	err := s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		workspaceRecord, err := tx.Load(ctx, workspaceID, WorkspaceRecordKind, workspaceID)
		if err != nil {
			return err
		}
		workspace, err = decodeWorkspace(workspaceRecord)
		if err != nil {
			return err
		}
		humanRecord, err := tx.Load(ctx, workspaceID, PrincipalRecordKind, workspace.HumanPrincipalID)
		if err != nil {
			return err
		}
		human, err = decodePrincipal(humanRecord)
		if err != nil {
			return err
		}
		assistantRecord, err := tx.Load(ctx, workspaceID, PrincipalRecordKind, workspace.PrimaryAssistantPrincipalID)
		if err != nil {
			return err
		}
		assistant, err = decodePrincipal(assistantRecord)
		if err != nil {
			return err
		}
		return validateWorkspacePrincipals(workspace, human, assistant)
	})
	if err != nil {
		return Workspace{}, Principal{}, Principal{}, err
	}
	return workspace, human, assistant, nil
}

func (s *Service) ResolveAccess(ctx context.Context, token string) (Workspace, Principal, error) {
	workspaceID, raw, err := parseToken(token)
	if err != nil {
		return Workspace{}, Principal{}, ErrAccessDenied
	}
	presented := tokenDigest(accessCredentialDomain, raw)
	var workspace Workspace
	var human Principal
	err = s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, WorkspaceRecordKind, workspaceID)
		if err != nil {
			if errors.Is(err, ports.ErrNotFound) || errors.Is(err, ports.ErrInvalid) {
				return ErrAccessDenied
			}
			return err
		}
		workspace, err = decodeWorkspace(record)
		if err != nil {
			return err
		}
		if workspace.State != WorkspaceActive || !equalDigest(workspace.accessDigest, presented) {
			return ErrAccessDenied
		}
		humanRecord, err := tx.Load(ctx, workspaceID, PrincipalRecordKind, workspace.HumanPrincipalID)
		if err != nil {
			return err
		}
		human, err = decodePrincipal(humanRecord)
		if err != nil {
			return err
		}
		if human.Kind != PrincipalHuman || human.State != PrincipalActive || human.WorkspaceID != workspace.ID {
			return ErrAccessDenied
		}
		return nil
	})
	if err != nil {
		return Workspace{}, Principal{}, err
	}
	return workspace, human, nil
}

func (s *Service) Recover(ctx context.Context, token string) (Workspace, Principal, Credentials, error) {
	workspaceID, raw, err := parseToken(token)
	if err != nil {
		return Workspace{}, Principal{}, Credentials{}, ErrRecoveryDenied
	}
	presented := tokenDigest(recoveryCredentialDomain, raw)
	var workspace Workspace
	var human Principal
	var nextCredentials Credentials
	err = s.store.WithinTransaction(ctx, func(tx ports.Transaction) error {
		record, err := tx.Load(ctx, workspaceID, WorkspaceRecordKind, workspaceID)
		if err != nil {
			if errors.Is(err, ports.ErrNotFound) || errors.Is(err, ports.ErrInvalid) {
				return ErrRecoveryDenied
			}
			return err
		}
		workspace, err = decodeWorkspace(record)
		if err != nil {
			return err
		}
		if workspace.State != WorkspaceActive || !equalDigest(workspace.recoveryDigest, presented) {
			return ErrRecoveryDenied
		}
		if record.Version == math.MaxUint64 {
			return ports.ErrVersionExhausted
		}

		accessToken, accessDigest, err := s.issueToken(workspaceID, accessCredentialDomain)
		if err != nil {
			return err
		}
		recoveryToken, recoveryDigest, err := s.issueToken(workspaceID, recoveryCredentialDomain)
		if err != nil {
			return err
		}
		workspace.accessDigest = accessDigest
		workspace.recoveryDigest = recoveryDigest
		workspace.AccessGeneration++
		workspace.RecoveryGeneration++
		workspace.Version = record.Version + 1
		payload, err := encodeWorkspace(workspace)
		if err != nil {
			return err
		}
		if err := tx.ReplaceIfVersion(ctx, ports.Record{
			WorkspaceID: workspaceID,
			Kind:        WorkspaceRecordKind,
			ID:          workspaceID,
			Version:     record.Version,
			Payload:     payload,
		}, record.Version); err != nil {
			return err
		}

		humanRecord, err := tx.Load(ctx, workspaceID, PrincipalRecordKind, workspace.HumanPrincipalID)
		if err != nil {
			return err
		}
		human, err = decodePrincipal(humanRecord)
		if err != nil {
			return err
		}
		if human.Kind != PrincipalHuman || human.State != PrincipalActive || human.WorkspaceID != workspace.ID {
			return ErrRecoveryDenied
		}
		nextCredentials = Credentials{Access: accessToken, Recovery: recoveryToken}
		return nil
	})
	if err != nil {
		return Workspace{}, Principal{}, Credentials{}, err
	}
	return workspace, human, nextCredentials, nil
}

func validateWorkspacePrincipals(workspace Workspace, human Principal, assistant Principal) error {
	if human.ID != workspace.HumanPrincipalID || human.WorkspaceID != workspace.ID || human.Kind != PrincipalHuman {
		return ErrInvalidCanonicalState
	}
	if assistant.ID != workspace.PrimaryAssistantPrincipalID || assistant.WorkspaceID != workspace.ID || assistant.Kind != PrincipalPrimaryAssistant {
		return ErrInvalidCanonicalState
	}
	return nil
}

func (s *Service) issueToken(workspaceID identity.ID, domain string) (string, digest, error) {
	if workspaceID.IsZero() || s.entropy == nil {
		return "", digest{}, ports.ErrInvalid
	}
	raw := make([]byte, identity.Size+credentialSecretSize)
	copy(raw[:identity.Size], workspaceID[:])
	if _, err := io.ReadFull(s.entropy, raw[identity.Size:]); err != nil {
		return "", digest{}, errors.Join(ErrCredentialEntropy, err)
	}
	allZero := true
	for _, value := range raw[identity.Size:] {
		if value != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return "", digest{}, ErrCredentialEntropy
	}
	return base64.RawURLEncoding.EncodeToString(raw), tokenDigest(domain, raw), nil
}

func parseToken(value string) (identity.ID, []byte, error) {
	raw, err := base64.RawURLEncoding.DecodeString(value)
	if err != nil || len(raw) != identity.Size+credentialSecretSize {
		return identity.ID{}, nil, ErrAccessDenied
	}
	var workspaceID identity.ID
	copy(workspaceID[:], raw[:identity.Size])
	if workspaceID.IsZero() {
		return identity.ID{}, nil, ErrAccessDenied
	}
	allZero := true
	for _, value := range raw[identity.Size:] {
		if value != 0 {
			allZero = false
			break
		}
	}
	if allZero {
		return identity.ID{}, nil, ErrAccessDenied
	}
	return workspaceID, raw, nil
}

func tokenDigest(domain string, raw []byte) digest {
	hash := sha256.New()
	_, _ = hash.Write([]byte(domain))
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(raw)
	var result digest
	copy(result[:], hash.Sum(nil))
	return result
}

func equalDigest(left digest, right digest) bool {
	return subtle.ConstantTimeCompare(left[:], right[:]) == 1
}
