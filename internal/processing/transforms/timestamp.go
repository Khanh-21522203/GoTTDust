package transforms

import (
	"encoding/json"
	"fmt"
	"strconv"
	"time"
)

// TimestampNormalization normalizes timestamp fields to RFC3339 with microsecond precision in UTC.
type TimestampNormalization struct {
	SourceField   string
	SourceFormats []string
	TargetFormat  string // always rfc3339_micros for MVP
}

// NewTimestampNormalizationFromConfig creates a TimestampNormalization from a config map.
func NewTimestampNormalizationFromConfig(config map[string]interface{}) (*TimestampNormalization, error) {
	tn := &TimestampNormalization{
		TargetFormat: "rfc3339_micros",
	}

	if sf, ok := config["source_field"]; ok {
		tn.SourceField, ok = sf.(string)
		if !ok {
			return nil, fmt.Errorf("source_field must be a string")
		}
	} else {
		return nil, fmt.Errorf("source_field is required")
	}

	if formats, ok := config["source_formats"]; ok {
		fmts, err := toStringSlice(formats)
		if err != nil {
			return nil, fmt.Errorf("invalid source_formats: %w", err)
		}
		tn.SourceFormats = fmts
	} else {
		// Default: try all formats
		tn.SourceFormats = []string{"unix_seconds", "unix_millis", "unix_micros", "iso8601", "rfc3339"}
	}

	return tn, nil
}

// Name returns the transform type name.
func (tn *TimestampNormalization) Name() string {
	return "timestamp_normalization"
}

// Process normalizes the timestamp field in the payload.
func (tn *TimestampNormalization) Process(payload []byte) ([]byte, error) {
	var data map[string]interface{}
	if err := json.Unmarshal(payload, &data); err != nil {
		return nil, fmt.Errorf("unmarshal payload: %w", err)
	}

	rawValue := getNestedValue(data, tn.SourceField)

	var t time.Time
	var parsed bool

	if rawValue == nil {
		// Missing timestamp: assign server receive time
		t = time.Now().UTC()
		parsed = true
	} else {
		t, parsed = tn.parseTimestamp(rawValue)
	}

	if !parsed {
		return nil, fmt.Errorf("cannot parse timestamp value %v with formats %v", rawValue, tn.SourceFormats)
	}

	// Format as RFC3339 with microsecond precision, UTC
	formatted := t.UTC().Format("2006-01-02T15:04:05.000000Z")
	setNestedValue(data, tn.SourceField, formatted)

	return json.Marshal(data)
}

// parseTimestamp attempts to parse a timestamp value using configured formats.
func (tn *TimestampNormalization) parseTimestamp(value interface{}) (time.Time, bool) {
	for _, format := range tn.SourceFormats {
		if t, ok := tn.tryParseFormat(value, format); ok {
			return t, true
		}
	}
	return time.Time{}, false
}

func (tn *TimestampNormalization) tryParseFormat(value interface{}, format string) (time.Time, bool) {
	switch format {
	case "unix_seconds":
		if n, ok := toFloat64(value); ok {
			sec := int64(n)
			nsec := int64((n - float64(sec)) * 1e9)
			return time.Unix(sec, nsec).UTC(), true
		}
	case "unix_millis":
		if n, ok := toFloat64(value); ok {
			ms := int64(n)
			return time.UnixMilli(ms).UTC(), true
		}
	case "unix_micros":
		if n, ok := toFloat64(value); ok {
			us := int64(n)
			return time.UnixMicro(us).UTC(), true
		}
	case "iso8601", "rfc3339":
		if s, ok := value.(string); ok {
			// Try multiple ISO8601/RFC3339 layouts
			layouts := []string{
				time.RFC3339Nano,
				time.RFC3339,
				"2006-01-02T15:04:05Z",
				"2006-01-02T15:04:05",
				"2006-01-02T15:04:05.000000Z",
				"2006-01-02 15:04:05",
			}
			for _, layout := range layouts {
				if t, err := time.Parse(layout, s); err == nil {
					return t.UTC(), true
				}
			}
		}
	}
	return time.Time{}, false
}

// toFloat64 converts various numeric types to float64.
func toFloat64(v interface{}) (float64, bool) {
	switch n := v.(type) {
	case float64:
		return n, true
	case float32:
		return float64(n), true
	case int:
		return float64(n), true
	case int64:
		return float64(n), true
	case json.Number:
		f, err := n.Float64()
		return f, err == nil
	case string:
		f, err := strconv.ParseFloat(n, 64)
		return f, err == nil
	default:
		return 0, false
	}
}

