package benchmarks

import (
	"encoding/json"
	"testing"
)

// BenchmarkRecordSerialization benchmarks JSON serialization of records.
func BenchmarkRecordSerialization(b *testing.B) {
	record := map[string]interface{}{
		"id":        1,
		"name":      "test-record",
		"timestamp": "2024-01-01T00:00:00Z",
		"nested": map[string]interface{}{
			"field1": "value1",
			"field2": 42,
		},
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			data, err := json.Marshal(record)
			if err != nil {
				b.Fatal(err)
			}
			_ = data
		}
	})
}

// BenchmarkRecordDeserialization benchmarks JSON deserialization of records.
func BenchmarkRecordDeserialization(b *testing.B) {
	data := []byte(`{"id":1,"name":"test-record","timestamp":"2024-01-01T00:00:00Z","nested":{"field1":"value1","field2":42}}`)

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			var result map[string]interface{}
			if err := json.Unmarshal(data, &result); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// BenchmarkBatchSerialization benchmarks serialization of record batches.
func BenchmarkBatchSerialization(b *testing.B) {
	records := make([]map[string]interface{}, 100)
	for i := range records {
		records[i] = map[string]interface{}{
			"id":        i,
			"name":      "test-record",
			"timestamp": "2024-01-01T00:00:00Z",
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		data, err := json.Marshal(records)
		if err != nil {
			b.Fatal(err)
		}
		_ = data
	}
}
