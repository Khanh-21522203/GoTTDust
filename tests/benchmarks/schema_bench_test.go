package benchmarks

import (
	"encoding/json"
	"fmt"
	"testing"

	"GoTTDust/internal/schema"
)

const testSchema = `{
	"type": "object",
	"required": ["user_id", "event_type", "timestamp"],
	"properties": {
		"user_id": {"type": "string", "minLength": 1, "maxLength": 100},
		"event_type": {"type": "string", "enum": ["click", "view", "purchase", "signup"]},
		"timestamp": {"type": "string", "format": "date-time"},
		"amount": {"type": "number", "minimum": 0},
		"metadata": {
			"type": "object",
			"properties": {
				"source": {"type": "string"},
				"campaign": {"type": "string"},
				"device": {"type": "string", "enum": ["mobile", "desktop", "tablet"]}
			}
		}
	}
}`

func validPayload() []byte {
	p, _ := json.Marshal(map[string]interface{}{
		"user_id":    "usr_12345",
		"event_type": "click",
		"timestamp":  "2025-01-15T10:30:00Z",
		"amount":     29.99,
		"metadata": map[string]interface{}{
			"source":   "google",
			"campaign": "summer_sale",
			"device":   "mobile",
		},
	})
	return p
}

func invalidPayload() []byte {
	p, _ := json.Marshal(map[string]interface{}{
		"user_id":    "", // violates minLength
		"event_type": "invalid_type",
		"amount":     -10,
	})
	return p
}

func largeValidPayload(extraFields int) []byte {
	m := map[string]interface{}{
		"user_id":    "usr_12345",
		"event_type": "click",
		"timestamp":  "2025-01-15T10:30:00Z",
		"metadata":   map[string]interface{}{},
	}
	meta := m["metadata"].(map[string]interface{})
	for i := 0; i < extraFields; i++ {
		meta[fmt.Sprintf("extra_%d", i)] = fmt.Sprintf("value_%d", i)
	}
	p, _ := json.Marshal(m)
	return p
}

// --- Schema Compilation ---

func BenchmarkSchemaCompile(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := schema.NewValidator(testSchema)
		if err != nil {
			b.Fatal(err)
		}
	}
}

// --- Schema Validation ---

func BenchmarkSchemaValidate_ValidPayload(b *testing.B) {
	v, _ := schema.NewValidator(testSchema)
	payload := validPayload()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := v.Validate(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchemaValidate_InvalidPayload(b *testing.B) {
	v, _ := schema.NewValidator(testSchema)
	payload := invalidPayload()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = v.Validate(payload) // expected to fail
	}
}

func BenchmarkSchemaValidate_LargePayload_50Fields(b *testing.B) {
	v, _ := schema.NewValidator(testSchema)
	payload := largeValidPayload(50)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := v.Validate(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchemaValidate_LargePayload_200Fields(b *testing.B) {
	v, _ := schema.NewValidator(testSchema)
	payload := largeValidPayload(200)
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := v.Validate(payload); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSchemaValidateConcurrent_10(b *testing.B) {
	v, _ := schema.NewValidator(testSchema)
	payload := validPayload()
	b.SetBytes(int64(len(payload)))
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			if err := v.Validate(payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

// --- Schema Registry ---

func BenchmarkRegistryRegister(b *testing.B) {
	r := schema.NewRegistry()
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		s := fmt.Sprintf(`{"type":"object","properties":{"f%d":{"type":"string"}}}`, i)
		_, err := r.Register(fmt.Sprintf("schema_%d", i), "bench", s)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryGet(b *testing.B) {
	r := schema.NewRegistry()
	def, _ := r.Register("test_schema", "bench", testSchema)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, err := r.Get(def.ID)
		if err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRegistryGetConcurrent(b *testing.B) {
	r := schema.NewRegistry()
	def, _ := r.Register("test_schema", "bench", testSchema)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, err := r.Get(def.ID)
			if err != nil {
				b.Fatal(err)
			}
		}
	})
}
