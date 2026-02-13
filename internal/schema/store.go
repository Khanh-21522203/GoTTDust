package schema

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"time"

	"GoTTDust/internal/common"
	redisadapter "GoTTDust/internal/storage/redis"
)

// Store is the PostgreSQL-backed schema store with L1/L2/L3 caching.
type Store struct {
	db    *sql.DB
	redis *redisadapter.Adapter
	l1    *common.LRUCache // In-process LRU cache (1000 entries, 5min TTL)
}

// NewStore creates a new PostgreSQL-backed schema store.
func NewStore(db *sql.DB, redis *redisadapter.Adapter) *Store {
	return &Store{
		db:    db,
		redis: redis,
		l1:    common.NewLRUCache(1000, 5*time.Minute),
	}
}

// CreateSchema creates a new schema in the database.
func (s *Store) CreateSchema(ctx context.Context, name, namespace, description, createdBy string) (*common.Schema, error) {
	schema := &common.Schema{}
	err := s.db.QueryRowContext(ctx,
		`INSERT INTO schemas (name, namespace, description, created_by)
		 VALUES ($1, $2, $3, $4)
		 RETURNING schema_id, name, namespace, description, created_at, updated_at, created_by`,
		name, namespace, description, createdBy,
	).Scan(&schema.SchemaID, &schema.Name, &schema.Namespace, &schema.Description,
		&schema.CreatedAt, &schema.UpdatedAt, &schema.CreatedBy)
	if err != nil {
		return nil, fmt.Errorf("create schema: %w", err)
	}
	return schema, nil
}

// CreateVersion creates a new schema version with compatibility check.
func (s *Store) CreateVersion(ctx context.Context, schemaID string, definition json.RawMessage, compatMode, createdBy string) (*common.SchemaVersion, error) {
	// Compute fingerprint
	hash := sha256.Sum256(definition)
	fingerprint := "sha256:" + hex.EncodeToString(hash[:])

	// Check if same fingerprint already exists
	var existingID string
	err := s.db.QueryRowContext(ctx,
		`SELECT version_id FROM schema_versions WHERE schema_id = $1 AND fingerprint = $2`,
		schemaID, fingerprint,
	).Scan(&existingID)
	if err == nil {
		// Idempotent: return existing version
		return s.getVersionByID(ctx, existingID)
	}

	// Get latest version number
	var latestVersion int
	err = s.db.QueryRowContext(ctx,
		`SELECT COALESCE(MAX(version), 0) FROM schema_versions WHERE schema_id = $1`,
		schemaID,
	).Scan(&latestVersion)
	if err != nil {
		return nil, fmt.Errorf("get latest version: %w", err)
	}

	newVersion := latestVersion + 1

	// Backward compatibility check (MVP)
	if compatMode == common.CompatBackward && latestVersion > 0 {
		if err := s.checkBackwardCompatibility(ctx, schemaID, latestVersion, definition); err != nil {
			return nil, fmt.Errorf("%w: %v", common.ErrValidation, err)
		}
	}

	// Validate the JSON Schema itself
	defStr := string(definition)
	if _, err := NewValidator(defStr); err != nil {
		return nil, fmt.Errorf("%w: invalid JSON schema: %v", common.ErrValidation, err)
	}

	// Insert
	sv := &common.SchemaVersion{}
	err = s.db.QueryRowContext(ctx,
		`INSERT INTO schema_versions (schema_id, version, definition, fingerprint, compatibility_mode, created_by)
		 VALUES ($1, $2, $3, $4, $5, $6)
		 RETURNING version_id, schema_id, version, definition, fingerprint, compatibility_mode, status, created_at, created_by`,
		schemaID, newVersion, definition, fingerprint, compatMode, createdBy,
	).Scan(&sv.VersionID, &sv.SchemaID, &sv.Version, &sv.Definition, &sv.Fingerprint,
		&sv.CompatibilityMode, &sv.Status, &sv.CreatedAt, &sv.CreatedBy)
	if err != nil {
		return nil, fmt.Errorf("create schema version: %w", err)
	}

	// Invalidate caches
	s.invalidateCache(ctx, schemaID, newVersion)

	return sv, nil
}

// GetSchema returns a schema by ID.
func (s *Store) GetSchema(ctx context.Context, schemaID string) (*common.Schema, error) {
	schema := &common.Schema{}
	err := s.db.QueryRowContext(ctx,
		`SELECT schema_id, name, namespace, description, created_at, updated_at, created_by
		 FROM schemas WHERE schema_id = $1`,
		schemaID,
	).Scan(&schema.SchemaID, &schema.Name, &schema.Namespace, &schema.Description,
		&schema.CreatedAt, &schema.UpdatedAt, &schema.CreatedBy)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: schema %s", common.ErrNotFound, schemaID)
	}
	if err != nil {
		return nil, fmt.Errorf("get schema: %w", err)
	}
	return schema, nil
}

// ListSchemas returns all schemas.
func (s *Store) ListSchemas(ctx context.Context) ([]*common.Schema, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT schema_id, name, namespace, description, created_at, updated_at, created_by
		 FROM schemas ORDER BY created_at DESC`)
	if err != nil {
		return nil, fmt.Errorf("list schemas: %w", err)
	}
	_ = rows.Close()

	var schemas []*common.Schema
	for rows.Next() {
		sc := &common.Schema{}
		if err := rows.Scan(&sc.SchemaID, &sc.Name, &sc.Namespace, &sc.Description,
			&sc.CreatedAt, &sc.UpdatedAt, &sc.CreatedBy); err != nil {
			return nil, fmt.Errorf("scan schema: %w", err)
		}
		schemas = append(schemas, sc)
	}
	return schemas, nil
}

// GetVersion returns a specific schema version, using L1→L2→L3 cache hierarchy.
func (s *Store) GetVersion(ctx context.Context, schemaID string, version int) (*common.SchemaVersion, error) {
	cacheKey := fmt.Sprintf("%s:%d", schemaID, version)

	// L1: In-process cache
	if val, ok := s.l1.Get(cacheKey); ok {
		return val.(*common.SchemaVersion), nil
	}

	// L2: Redis cache
	if s.redis != nil {
		redisKey := fmt.Sprintf("ttdust:schema:%s:%d", schemaID, version)
		if data, ok := s.redis.Get(ctx, redisKey); ok {
			sv := &common.SchemaVersion{}
			if err := json.Unmarshal(data, sv); err == nil {
				s.l1.Set(cacheKey, sv)
				return sv, nil
			}
		}
	}

	// L3: PostgreSQL
	sv := &common.SchemaVersion{}
	err := s.db.QueryRowContext(ctx,
		`SELECT version_id, schema_id, version, definition, fingerprint, compatibility_mode, status, created_at, created_by
		 FROM schema_versions WHERE schema_id = $1 AND version = $2`,
		schemaID, version,
	).Scan(&sv.VersionID, &sv.SchemaID, &sv.Version, &sv.Definition, &sv.Fingerprint,
		&sv.CompatibilityMode, &sv.Status, &sv.CreatedAt, &sv.CreatedBy)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: schema %s version %d", common.ErrNotFound, schemaID, version)
	}
	if err != nil {
		return nil, fmt.Errorf("get schema version: %w", err)
	}

	// Populate caches
	s.l1.Set(cacheKey, sv)
	if s.redis != nil {
		redisKey := fmt.Sprintf("ttdust:schema:%s:%d", schemaID, version)
		if data, err := json.Marshal(sv); err == nil {
			s.redis.Set(ctx, redisKey, data, 1*time.Hour)
		}
	}

	return sv, nil
}

// ListVersions returns all versions for a schema.
func (s *Store) ListVersions(ctx context.Context, schemaID string) ([]*common.SchemaVersion, error) {
	rows, err := s.db.QueryContext(ctx,
		`SELECT version_id, schema_id, version, definition, fingerprint, compatibility_mode, status, created_at, created_by
		 FROM schema_versions WHERE schema_id = $1 ORDER BY version ASC`,
		schemaID,
	)
	if err != nil {
		return nil, fmt.Errorf("list versions: %w", err)
	}
	_ = rows.Close()

	var versions []*common.SchemaVersion
	for rows.Next() {
		sv := &common.SchemaVersion{}
		if err := rows.Scan(&sv.VersionID, &sv.SchemaID, &sv.Version, &sv.Definition, &sv.Fingerprint,
			&sv.CompatibilityMode, &sv.Status, &sv.CreatedAt, &sv.CreatedBy); err != nil {
			return nil, fmt.Errorf("scan version: %w", err)
		}
		versions = append(versions, sv)
	}
	return versions, nil
}

// GetLatestVersion returns the latest active version for a schema.
func (s *Store) GetLatestVersion(ctx context.Context, schemaID string) (*common.SchemaVersion, error) {
	sv := &common.SchemaVersion{}
	err := s.db.QueryRowContext(ctx,
		`SELECT version_id, schema_id, version, definition, fingerprint, compatibility_mode, status, created_at, created_by
		 FROM schema_versions WHERE schema_id = $1 AND status = 'active' ORDER BY version DESC LIMIT 1`,
		schemaID,
	).Scan(&sv.VersionID, &sv.SchemaID, &sv.Version, &sv.Definition, &sv.Fingerprint,
		&sv.CompatibilityMode, &sv.Status, &sv.CreatedAt, &sv.CreatedBy)
	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%w: no active version for schema %s", common.ErrNotFound, schemaID)
	}
	if err != nil {
		return nil, fmt.Errorf("get latest version: %w", err)
	}
	return sv, nil
}

// ValidatePayload validates a payload against a schema version (with caching).
func (s *Store) ValidatePayload(ctx context.Context, schemaID string, version int, payload []byte) error {
	sv, err := s.GetVersion(ctx, schemaID, version)
	if err != nil {
		return err
	}

	validator, err := NewValidator(string(sv.Definition))
	if err != nil {
		return fmt.Errorf("compile schema: %w", err)
	}

	return validator.Validate(payload)
}

// --- internal helpers ---

func (s *Store) getVersionByID(ctx context.Context, versionID string) (*common.SchemaVersion, error) {
	sv := &common.SchemaVersion{}
	err := s.db.QueryRowContext(ctx,
		`SELECT version_id, schema_id, version, definition, fingerprint, compatibility_mode, status, created_at, created_by
		 FROM schema_versions WHERE version_id = $1`,
		versionID,
	).Scan(&sv.VersionID, &sv.SchemaID, &sv.Version, &sv.Definition, &sv.Fingerprint,
		&sv.CompatibilityMode, &sv.Status, &sv.CreatedAt, &sv.CreatedBy)
	if err != nil {
		return nil, fmt.Errorf("get version by id: %w", err)
	}
	return sv, nil
}

func (s *Store) invalidateCache(ctx context.Context, schemaID string, version int) {
	cacheKey := fmt.Sprintf("%s:%d", schemaID, version)
	s.l1.Delete(cacheKey)

	if s.redis != nil {
		redisKey := fmt.Sprintf("ttdust:schema:%s:%d", schemaID, version)
		s.redis.Delete(ctx, redisKey)
	}
}

func (s *Store) checkBackwardCompatibility(ctx context.Context, schemaID string, latestVersion int, newDef json.RawMessage) error {
	oldSV, err := s.GetVersion(ctx, schemaID, latestVersion)
	if err != nil {
		return fmt.Errorf("get old version for compat check: %w", err)
	}

	var oldSchema, newSchema map[string]interface{}
	if err := json.Unmarshal(oldSV.Definition, &oldSchema); err != nil {
		return fmt.Errorf("parse old schema: %w", err)
	}
	if err := json.Unmarshal(newDef, &newSchema); err != nil {
		return fmt.Errorf("parse new schema: %w", err)
	}

	// BACKWARD: new schema must be able to read old data
	// All required fields in old must exist in new
	oldRequired := getStringSlice(oldSchema, "required")
	newProps := getObjectKeys(newSchema, "properties")

	for _, req := range oldRequired {
		found := false
		for _, prop := range newProps {
			if req == prop {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("backward incompatible: required field %q removed from properties", req)
		}
	}

	// New required fields not allowed
	newRequired := getStringSlice(newSchema, "required")
	for _, req := range newRequired {
		found := false
		for _, oldReq := range oldRequired {
			if req == oldReq {
				found = true
				break
			}
		}
		if !found {
			return fmt.Errorf("backward incompatible: new required field %q added", req)
		}
	}

	return nil
}

func getStringSlice(schema map[string]interface{}, key string) []string {
	val, ok := schema[key]
	if !ok {
		return nil
	}
	arr, ok := val.([]interface{})
	if !ok {
		return nil
	}
	result := make([]string, 0, len(arr))
	for _, v := range arr {
		if s, ok := v.(string); ok {
			result = append(result, s)
		}
	}
	return result
}

func getObjectKeys(schema map[string]interface{}, key string) []string {
	val, ok := schema[key]
	if !ok {
		return nil
	}
	obj, ok := val.(map[string]interface{})
	if !ok {
		return nil
	}
	keys := make([]string, 0, len(obj))
	for k := range obj {
		keys = append(keys, k)
	}
	return keys
}
