package benchmarks

import (
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"GoTTDust/internal/common"
	"GoTTDust/internal/storage"
)

func makeValidatedRecord(size int) *common.ValidatedRecord {
	payload := make([]byte, 0, size)
	m := map[string]interface{}{
		"user_id":    "usr_12345",
		"event_type": "click",
		"timestamp":  "2025-01-15T10:30:00Z",
	}
	// Pad with extra fields
	for i := 0; len(payload) < size; i++ {
		m[fmt.Sprintf("f_%d", i)] = fmt.Sprintf("val_%d", i)
		payload, _ = json.Marshal(m)
	}

	return &common.ValidatedRecord{
		RecordID:       common.NewRecordID(),
		StreamID:       "str_bench",
		SchemaID:       "sch_bench",
		SequenceNumber: 1,
		Payload:        payload,
		IngestedAt:     time.Now(),
	}
}

func BenchmarkParquetBatchAdd_100Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	payload := rec.Payload

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
		for j := 0; j < 100; j++ {
			batch.Add(rec, payload)
		}
	}
}

func BenchmarkParquetBatchAdd_1000Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	payload := rec.Payload

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
		for j := 0; j < 1000; j++ {
			batch.Add(rec, payload)
		}
	}
}

func BenchmarkParquetBatchAdd_10000Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	payload := rec.Payload

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
		for j := 0; j < 10000; j++ {
			batch.Add(rec, payload)
		}
	}
}

func BenchmarkParquetBatchMarshalJSON_100Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
	for j := 0; j < 100; j++ {
		batch.Add(rec, rec.Payload)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := batch.MarshalJSON()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(data)))
	}
}

func BenchmarkParquetBatchMarshalJSON_1000Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
	for j := 0; j < 1000; j++ {
		batch.Add(rec, rec.Payload)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		data, err := batch.MarshalJSON()
		if err != nil {
			b.Fatal(err)
		}
		b.SetBytes(int64(len(data)))
	}
}

func BenchmarkParquetBatchUnmarshalJSON_100Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
	for j := 0; j < 100; j++ {
		batch.Add(rec, rec.Payload)
	}
	data, _ := batch.MarshalJSON()

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result struct {
			Records []storage.ParquetRecord `json:"records"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkParquetBatchUnmarshalJSON_1000Records(b *testing.B) {
	partition := common.PartitionKeyFromTime("str_bench", time.Now())
	rec := makeValidatedRecord(512)
	batch := storage.NewParquetBatch("str_bench", "sch_bench", partition)
	for j := 0; j < 1000; j++ {
		batch.Add(rec, rec.Payload)
	}
	data, _ := batch.MarshalJSON()

	b.SetBytes(int64(len(data)))
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		var result struct {
			Records []storage.ParquetRecord `json:"records"`
		}
		if err := json.Unmarshal(data, &result); err != nil {
			b.Fatal(err)
		}
	}
}
