package transforms

import (
	"encoding/json"
	"fmt"
	"strings"
)


// FieldProjection selects a subset of fields from input records.
type FieldProjection struct {
	IncludeFields []string
	ExcludeFields []string
}

// NewFieldProjectionFromConfig creates a FieldProjection from a config map.
func NewFieldProjectionFromConfig(config map[string]interface{}) (*FieldProjection, error) {
	fp := &FieldProjection{}

	if include, ok := config["include"]; ok {
		fields, err := toStringSlice(include)
		if err != nil {
			return nil, fmt.Errorf("invalid include fields: %w", err)
		}
		fp.IncludeFields = fields
	}

	if exclude, ok := config["exclude"]; ok {
		fields, err := toStringSlice(exclude)
		if err != nil {
			return nil, fmt.Errorf("invalid exclude fields: %w", err)
		}
		fp.ExcludeFields = fields
	}

	if len(fp.IncludeFields) == 0 && len(fp.ExcludeFields) == 0 {
		return nil, fmt.Errorf("either include or exclude fields must be specified")
	}

	return fp, nil
}

// Name returns the transform type name.
func (fp *FieldProjection) Name() string {
	return "field_projection"
}

// Process applies field projection to a JSON payload.
func (fp *FieldProjection) Process(payload []byte) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	var result map[string]interface{}

	if len(fp.IncludeFields) > 0 {
		result = make(map[string]interface{})
		for _, field := range fp.IncludeFields {
			if strings.HasSuffix(field, ".*") {
				// Wildcard: include all fields under prefix
				prefix := strings.TrimSuffix(field, ".*")
				val := getNestedValue(data, prefix)
				if val != nil {
					setNestedValue(result, prefix, val)
				}
			} else {
				val := getNestedValue(data, field)
				if val != nil {
					setNestedValue(result, field, val)
				} else {
					// Missing fields result in null values
					setNestedValue(result, field, nil)
				}
			}
		}
	} else {
		// Exclude mode: start with all fields, remove excluded
		result = deepCopyMap(data)
		for _, field := range fp.ExcludeFields {
			removeNestedValue(result, field)
		}
	}

	return json.Marshal(result)
}

// removeNestedValue removes a value from a nested map using dot notation.
func removeNestedValue(data map[string]interface{}, path string) {
	parts := strings.Split(path, ".")

	current := data
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			return
		}
		nextMap, ok := next.(map[string]interface{})
		if !ok {
			return
		}
		current = nextMap
	}

	delete(current, parts[len(parts)-1])
}

// deepCopyMap creates a deep copy of a map.
func deepCopyMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		if m, ok := v.(map[string]interface{}); ok {
			dst[k] = deepCopyMap(m)
		} else {
			dst[k] = v
		}
	}
	return dst
}

