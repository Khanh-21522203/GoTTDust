package benchmarks

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"GoTTDust/internal/common"
	"GoTTDust/internal/ingestion"
)

// --- RecordBuffer Benchmarks ---

func makeRecord(size int) *common.ValidatedRecord {
	payload := make([]byte, size)
	for i := range payload {
		payload[i] = byte('a' + (i % 26))
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

func BenchmarkBufferAdd_100B(b *testing.B) {
	buf := ingestion.NewRecordBuffer("str_bench", 1_000_000, 1<<30)
	rec := makeRecord(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.Add(rec)
	}
}

func BenchmarkBufferAdd_1KB(b *testing.B) {
	buf := ingestion.NewRecordBuffer("str_bench", 1_000_000, 1<<30)
	rec := makeRecord(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.Add(rec)
	}
}

func BenchmarkBufferAdd_10KB(b *testing.B) {
	buf := ingestion.NewRecordBuffer("str_bench", 1_000_000, 1<<30)
	rec := makeRecord(10240)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.Add(rec)
	}
}

func BenchmarkBufferAddConcurrent_10Writers(b *testing.B) {
	buf := ingestion.NewRecordBuffer("str_bench", 10_000_000, 1<<30)
	rec := makeRecord(1024)
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_ = buf.Add(rec)
		}
	})
}

func BenchmarkBufferFlush_1000Records(b *testing.B) {
	rec := makeRecord(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := ingestion.NewRecordBuffer("str_bench", 100_000, 1<<30)
		for j := 0; j < 1000; j++ {
			_ = buf.Add(rec)
		}
		_ = buf.Flush()
	}
}

func BenchmarkBufferFlush_10000Records(b *testing.B) {
	rec := makeRecord(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		buf := ingestion.NewRecordBuffer("str_bench", 100_000, 1<<30)
		for j := 0; j < 10000; j++ {
			_ = buf.Add(rec)
		}
		_ = buf.Flush()
	}
}

func BenchmarkBufferBackpressureCheck(b *testing.B) {
	buf := ingestion.NewRecordBuffer("str_bench", 10000, 1<<30)
	rec := makeRecord(100)
	for i := 0; i < 5000; i++ {
		_ = buf.Add(rec)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.BackpressureLevel()
	}
}

func BenchmarkBufferShouldFlush(b *testing.B) {
	buf := ingestion.NewRecordBuffer("str_bench", 10000, 1<<30)
	rec := makeRecord(100)
	for i := 0; i < 5000; i++ {
		_ = buf.Add(rec)
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = buf.ShouldFlush(10 * time.Second)
	}
}

// --- BufferManager Benchmarks ---

func BenchmarkBufferManagerGetOrCreate_SingleStream(b *testing.B) {
	bm := ingestion.NewBufferManager(10000, 1<<30)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bm.GetOrCreate("str_bench")
	}
}

func BenchmarkBufferManagerGetOrCreate_100Streams(b *testing.B) {
	bm := ingestion.NewBufferManager(10000, 1<<30)
	streamIDs := make([]common.StreamID, 100)
	for i := range streamIDs {
		streamIDs[i] = common.StreamID(fmt.Sprintf("str_%d", i))
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = bm.GetOrCreate(streamIDs[i%100])
	}
}

func BenchmarkBufferManagerGetOrCreateConcurrent_100Streams(b *testing.B) {
	bm := ingestion.NewBufferManager(10000, 1<<30)
	streamIDs := make([]common.StreamID, 100)
	for i := range streamIDs {
		streamIDs[i] = common.StreamID(fmt.Sprintf("str_%d", i))
	}
	var counter int64
	var mu sync.Mutex
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		mu.Lock()
		id := counter
		counter++
		mu.Unlock()
		for pb.Next() {
			_ = bm.GetOrCreate(streamIDs[id%100])
		}
	})
}

func BenchmarkBufferManagerFlushAll_10Streams_1000Each(b *testing.B) {
	rec := makeRecord(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bm := ingestion.NewBufferManager(100_000, 1<<30)
		for s := 0; s < 10; s++ {
			buf := bm.GetOrCreate(common.StreamID(fmt.Sprintf("str_%d", s)))
			for j := 0; j < 1000; j++ {
				_ = buf.Add(rec)
			}
		}
		_ = bm.FlushAll()
	}
}
