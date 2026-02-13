package transforms

import (
	"encoding/json"
	"fmt"
	"math"
	"strconv"
	"strings"
)

// CoercionRule defines a single type coercion rule.
type CoercionRule struct {
	Field      string `json:"field"`
	TargetType string `json:"target_type"`
}

// TypeCoercion converts field types according to rules.
type TypeCoercion struct {
	Rules []CoercionRule
}

// NewTypeCoercionFromConfig creates a TypeCoercion from a config map.
func NewTypeCoercionFromConfig(config map[string]interface{}) (*TypeCoercion, error) {
	tc := &TypeCoercion{}

	rulesRaw, ok := config["rules"]
	if !ok {
		return nil, fmt.Errorf("rules is required")
	}

	rulesSlice, ok := rulesRaw.([]interface{})
	if !ok {
		return nil, fmt.Errorf("rules must be an array")
	}

	for _, ruleRaw := range rulesSlice {
		ruleMap, ok := ruleRaw.(map[string]interface{})
		if !ok {
			return nil, fmt.Errorf("each rule must be an object")
		}

		field, ok := ruleMap["field"].(string)
		if !ok {
			return nil, fmt.Errorf("rule field must be a string")
		}

		targetType, ok := ruleMap["target_type"].(string)
		if !ok {
			return nil, fmt.Errorf("rule target_type must be a string")
		}

		tc.Rules = append(tc.Rules, CoercionRule{
			Field:      field,
			TargetType: targetType,
		})
	}

	return tc, nil
}

// Name returns the transform type name.
func (tc *TypeCoercion) Name() string {
	return "type_coercion"
}

// Process applies type coercion rules to the payload.
func (tc *TypeCoercion) Process(payload []byte) ([]byte, error) {
	var data map[string]interface{}
	// Use json.Number to preserve numeric precision
	dec := json.NewDecoder(strings.NewReader(string(payload)))
	dec.UseNumber()
	if err := dec.Decode(&data); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	for _, rule := range tc.Rules {
		val := getNestedValue(data, rule.Field)
		if val == nil {
			continue // Skip null/missing fields
		}

		coerced, err := coerceValue(val, rule.TargetType)
		if err != nil {
			return nil, fmt.Errorf("coerce field %s to %s: %w", rule.Field, rule.TargetType, err)
		}

		setNestedValue(data, rule.Field, coerced)
	}

	return json.Marshal(data)
}

// coerceValue converts a value to the target type.
func coerceValue(value interface{}, targetType string) (interface{}, error) {
	switch targetType {
	case "string":
		return coerceToString(value)
	case "int64":
		return coerceToInt64(value)
	case "float64":
		return coerceToFloat64(value)
	case "bool":
		return coerceToBool(value)
	default:
		return nil, fmt.Errorf("unsupported target type: %s", targetType)
	}
}

func coerceToString(value interface{}) (string, error) {
	switch v := value.(type) {
	case string:
		return v, nil
	case json.Number:
		return v.String(), nil
	case float64:
		// Up to 6 decimal places
		if v == math.Trunc(v) {
			return strconv.FormatInt(int64(v), 10), nil
		}
		return strconv.FormatFloat(v, 'f', 6, 64), nil
	case int64:
		return strconv.FormatInt(v, 10), nil
	case bool:
		return strconv.FormatBool(v), nil
	default:
		return fmt.Sprintf("%v", v), nil
	}
}

func coerceToInt64(value interface{}) (int64, error) {
	switch v := value.(type) {
	case json.Number:
		// Try int first
		if i, err := v.Int64(); err == nil {
			return i, nil
		}
		// Try float and truncate
		if f, err := v.Float64(); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot convert %q to int64", v.String())
	case float64:
		return int64(v), nil // Truncate (floor)
	case int64:
		return v, nil
	case string:
		// Try parsing as integer
		if i, err := strconv.ParseInt(v, 10, 64); err == nil {
			return i, nil
		}
		// Try parsing as float and truncate
		if f, err := strconv.ParseFloat(v, 64); err == nil {
			return int64(f), nil
		}
		return 0, fmt.Errorf("cannot convert %q to int64", v)
	case bool:
		if v {
			return 1, nil
		}
		return 0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to int64", v)
	}
}

func coerceToFloat64(value interface{}) (float64, error) {
	switch v := value.(type) {
	case json.Number:
		return v.Float64()
	case float64:
		return v, nil
	case int64:
		return float64(v), nil
	case string:
		return strconv.ParseFloat(v, 64)
	case bool:
		if v {
			return 1.0, nil
		}
		return 0.0, nil
	default:
		return 0, fmt.Errorf("cannot convert %T to float64", v)
	}
}

func coerceToBool(value interface{}) (bool, error) {
	switch v := value.(type) {
	case bool:
		return v, nil
	case string:
		switch strings.ToLower(v) {
		case "true", "1":
			return true, nil
		case "false", "0":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert %q to bool", v)
		}
	case json.Number:
		s := v.String()
		switch s {
		case "1":
			return true, nil
		case "0":
			return false, nil
		default:
			return false, fmt.Errorf("cannot convert %q to bool", s)
		}
	case float64:
		if v == 1.0 {
			return true, nil
		}
		if v == 0.0 {
			return false, nil
		}
		return false, fmt.Errorf("cannot convert %v to bool", v)
	case int64:
		if v == 1 {
			return true, nil
		}
		if v == 0 {
			return false, nil
		}
		return false, fmt.Errorf("cannot convert %v to bool", v)
	default:
		return false, fmt.Errorf("cannot convert %T to bool", v)
	}
}
