package transforms

import (
	"fmt"
	"strings"
)

// getNestedValue retrieves a value from a nested map using dot notation.
func getNestedValue(data map[string]interface{}, path string) interface{} {
	parts := strings.Split(path, ".")
	current := interface{}(data)

	for _, part := range parts {
		m, ok := current.(map[string]interface{})
		if !ok {
			return nil
		}
		current, ok = m[part]
		if !ok {
			return nil
		}
	}

	return current
}

// setNestedValue sets a value in a nested map using dot notation.
func setNestedValue(data map[string]interface{}, path string, value interface{}) {
	parts := strings.Split(path, ".")

	current := data
	for i := 0; i < len(parts)-1; i++ {
		next, ok := current[parts[i]]
		if !ok {
			next = make(map[string]interface{})
			current[parts[i]] = next
		}
		nextMap, ok := next.(map[string]interface{})
		if !ok {
			nextMap = make(map[string]interface{})
			current[parts[i]] = nextMap
		}
		current = nextMap
	}

	current[parts[len(parts)-1]] = value
}

// toStringSlice converts an interface{} to []string.
func toStringSlice(v interface{}) ([]string, error) {
	switch val := v.(type) {
	case []string:
		return val, nil
	case []interface{}:
		result := make([]string, len(val))
		for i, item := range val {
			s, ok := item.(string)
			if !ok {
				return nil, fmt.Errorf("element %d is not a string", i)
			}
			result[i] = s
		}
		return result, nil
	default:
		return nil, fmt.Errorf("expected string slice, got %T", v)
	}
}
