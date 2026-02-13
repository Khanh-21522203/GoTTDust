package fixtures

import (
	"encoding/json"
	"fmt"
	"time"
)

// TestSchema returns a JSON Schema draft-07 definition for testing.
func TestSchema() map[string]interface{} {
	return map[string]interface{}{
		"$schema":  "http://json-schema.org/draft-07/schema#",
		"type":     "object",
		"required": []string{"id"},
		"properties": map[string]interface{}{
			"id": map[string]interface{}{
				"type": "integer",
			},
			"name": map[string]interface{}{
				"type": "string",
			},
			"timestamp": map[string]interface{}{
				"type":   "string",
				"format": "date-time",
			},
		},
	}
}

// TestSchemaJSON returns the test schema as json.RawMessage.
func TestSchemaJSON() json.RawMessage {
	data, _ := json.Marshal(TestSchema())
	return data
}

// TestRecords generates count test records.
func TestRecords(count int) []map[string]interface{} {
	records := make([]map[string]interface{}, count)
	now := time.Now().UTC()

	for i := 0; i < count; i++ {
		records[i] = map[string]interface{}{
			"id":        i,
			"name":      fmt.Sprintf("record-%d", i),
			"timestamp": now.Format(time.RFC3339),
		}
	}

	return records
}

// TestRecordsJSON generates count test records as json.RawMessage slices.
func TestRecordsJSON(count int) []json.RawMessage {
	records := TestRecords(count)
	result := make([]json.RawMessage, len(records))
	for i, r := range records {
		data, _ := json.Marshal(r)
		result[i] = data
	}
	return result
}
