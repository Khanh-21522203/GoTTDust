package schema

import (
	"fmt"
	"sync"

	"GoTTDust/internal/common"
)

// SchemaDefinition holds a registered schema with its metadata.
type SchemaDefinition struct {
	ID        common.SchemaID
	Name      string
	Namespace string
	Version   int
	RawSchema string // JSON Schema (draft-07) as string
}

// Registry manages schema definitions and versions.
type Registry struct {
	mu      sync.RWMutex
	schemas map[common.SchemaID]*SchemaDefinition
	// name+namespace -> list of versions
	byName     map[string][]*SchemaDefinition
	validators map[common.SchemaID]*Validator
}

// NewRegistry creates a new in-memory schema registry.
func NewRegistry() *Registry {
	return &Registry{
		schemas:    make(map[common.SchemaID]*SchemaDefinition),
		byName:     make(map[string][]*SchemaDefinition),
		validators: make(map[common.SchemaID]*Validator),
	}
}

// Register registers a new schema definition.
// Returns the assigned schema ID and version.
func (r *Registry) Register(name, namespace, rawSchema string) (*SchemaDefinition, error) {
	r.mu.Lock()
	defer r.mu.Unlock()

	// Validate the schema itself is valid JSON Schema
	validator, err := NewValidator(rawSchema)
	if err != nil {
		return nil, fmt.Errorf("%w: invalid JSON schema: %v", common.ErrValidation, err)
	}

	key := nameKey(name, namespace)
	versions := r.byName[key]

	// Check for duplicate raw schema content in latest version
	if len(versions) > 0 {
		latest := versions[len(versions)-1]
		if latest.RawSchema == rawSchema {
			return latest, nil // Idempotent: same schema, return existing
		}
	}

	version := len(versions) + 1
	id := common.SchemaID(fmt.Sprintf("%s.%s.v%d", namespace, name, version))

	def := &SchemaDefinition{
		ID:        id,
		Name:      name,
		Namespace: namespace,
		Version:   version,
		RawSchema: rawSchema,
	}

	r.schemas[id] = def
	r.byName[key] = append(r.byName[key], def)
	r.validators[id] = validator

	return def, nil
}

// Get returns a schema definition by ID.
func (r *Registry) Get(id common.SchemaID) (*SchemaDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	def, ok := r.schemas[id]
	if !ok {
		return nil, fmt.Errorf("%w: schema %s", common.ErrNotFound, id)
	}
	return def, nil
}

// GetLatest returns the latest version of a schema by name and namespace.
func (r *Registry) GetLatest(name, namespace string) (*SchemaDefinition, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	key := nameKey(name, namespace)
	versions, ok := r.byName[key]
	if !ok || len(versions) == 0 {
		return nil, fmt.Errorf("%w: schema %s/%s", common.ErrNotFound, namespace, name)
	}
	return versions[len(versions)-1], nil
}

// Validate validates a JSON payload against a registered schema.
func (r *Registry) Validate(schemaID common.SchemaID, payload []byte) error {
	r.mu.RLock()
	validator, ok := r.validators[schemaID]
	r.mu.RUnlock()

	if !ok {
		return fmt.Errorf("%w: schema %s", common.ErrNotFound, schemaID)
	}

	return validator.Validate(payload)
}

// List returns all registered schemas.
func (r *Registry) List() []*SchemaDefinition {
	r.mu.RLock()
	defer r.mu.RUnlock()

	result := make([]*SchemaDefinition, 0, len(r.schemas))
	for _, def := range r.schemas {
		result = append(result, def)
	}
	return result
}

func nameKey(name, namespace string) string {
	return namespace + "/" + name
}
