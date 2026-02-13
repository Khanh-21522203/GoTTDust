package schema

import (
	"encoding/json"
	"fmt"

	"GoTTDust/internal/common"

	"github.com/xeipuuv/gojsonschema"
)

// Validator validates JSON payloads against a JSON Schema.
type Validator struct {
	schema *gojsonschema.Schema
}

// NewValidator creates a new validator from a raw JSON Schema string.
func NewValidator(rawSchema string) (*Validator, error) {
	loader := gojsonschema.NewStringLoader(rawSchema)
	schema, err := gojsonschema.NewSchema(loader)
	if err != nil {
		return nil, fmt.Errorf("compile JSON schema: %w", err)
	}
	return &Validator{schema: schema}, nil
}

// Validate validates a JSON payload against the schema.
// Returns a ValidationError with field-level details on failure.
func (v *Validator) Validate(payload []byte) error {
	// First check it's valid JSON
	var raw interface{}
	if err := json.Unmarshal(payload, &raw); err != nil {
		return common.NewValidationError(common.FieldError{
			Path:    "$",
			Message: fmt.Sprintf("invalid JSON: %v", err),
		})
	}

	documentLoader := gojsonschema.NewGoLoader(raw)
	result, err := v.schema.Validate(documentLoader)
	if err != nil {
		return fmt.Errorf("schema validation error: %w", err)
	}

	if result.Valid() {
		return nil
	}

	fieldErrors := make([]common.FieldError, 0, len(result.Errors()))
	for _, desc := range result.Errors() {
		fieldErrors = append(fieldErrors, common.FieldError{
			Path:       fmt.Sprintf("$.%s", desc.Field()),
			Message:    desc.Description(),
			SchemaPath: desc.Context().String(),
		})
	}

	return common.NewValidationError(fieldErrors...)
}
