package benchmarks

import (
	"fmt"
	"os"
	"testing"
	"time"

	"GoTTDust/internal/common"
	"GoTTDust/internal/ingestion"
)

func newTestWAL(b *testing.B) (*ingestion.WALManager, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "ttdust-wal-bench-*")
	if err != nil {
		b.Fatal(err)
	}
	cfg := ingestion.WALConfig{
		Dir:               dir,
		SegmentSizeBytes:  64 * 1024 * 1024, // 64MB
		SegmentAge:        10 * time.Minute,
		SyncMode:          "none", // Skip fsync for pure throughput benchmark
		RetentionSegments: 100,
	}
	wm, err := ingestion.NewWALManager(cfg)
	if err != nil {
		b.Fatal(err)
	}
	return wm, dir
}

func newTestWALFsync(b *testing.B) (*ingestion.WALManager, string) {
	b.Helper()
	dir, err := os.MkdirTemp("", "ttdust-wal-bench-fsync-*")
	if err != nil {
		b.Fatal(err)
	}
	cfg := ingestion.WALConfig{
		Dir:               dir,
		SegmentSizeBytes:  64 * 1024 * 1024,
		SegmentAge:        10 * time.Minute,
		SyncMode:          "fsync",
		RetentionSegments: 100,
	}
	wm, err := ingestion.NewWALManager(cfg)
	if err != nil {
		b.Fatal(err)
	}
	return wm, dir
}

// --- WAL Write Benchmarks (no fsync) ---

func BenchmarkWALWrite_100B_NoFsync(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	rec := makeRecord(100)
	b.SetBytes(100)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.Write("str_bench", rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALWrite_1KB_NoFsync(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	rec := makeRecord(1024)
	b.SetBytes(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.Write("str_bench", rec); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALWrite_10KB_NoFsync(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	rec := makeRecord(10240)
	b.SetBytes(10240)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.Write("str_bench", rec); err != nil {
			b.Fatal(err)
		}
	}
}

// --- WAL Write Benchmarks (with fsync) ---

func BenchmarkWALWrite_1KB_Fsync(b *testing.B) {
	wm, dir := newTestWALFsync(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	rec := makeRecord(1024)
	b.SetBytes(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.Write("str_bench", rec); err != nil {
			b.Fatal(err)
		}
	}
}

// --- WAL Batch Write Benchmarks ---

func BenchmarkWALWriteBatch_100x1KB_NoFsync(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	records := make([]*common.ValidatedRecord, 100)
	for i := range records {
		records[i] = makeRecord(1024)
	}
	b.SetBytes(100 * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.WriteBatch("str_bench", records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALWriteBatch_1000x1KB_NoFsync(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	records := make([]*common.ValidatedRecord, 1000)
	for i := range records {
		records[i] = makeRecord(1024)
	}
	b.SetBytes(1000 * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.WriteBatch("str_bench", records); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkWALWriteBatch_100x1KB_Fsync(b *testing.B) {
	wm, dir := newTestWALFsync(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	records := make([]*common.ValidatedRecord, 100)
	for i := range records {
		records[i] = makeRecord(1024)
	}
	b.SetBytes(100 * 1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.WriteBatch("str_bench", records); err != nil {
			b.Fatal(err)
		}
	}
}

// --- WAL Multi-Stream Benchmarks ---

func BenchmarkWALWrite_10Streams_1KB(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	streams := make([]common.StreamID, 10)
	for i := range streams {
		streams[i] = common.StreamID(fmt.Sprintf("str_%d", i))
	}
	rec := makeRecord(1024)
	b.SetBytes(1024)
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		if err := wm.Write(streams[i%10], rec); err != nil {
			b.Fatal(err)
		}
	}
}

// --- WAL Seal + Recovery Benchmarks ---

func BenchmarkWALSealSegment(b *testing.B) {
	wm, dir := newTestWAL(b)
	_ = os.RemoveAll(dir)
	_ = wm.Close()

	rec := makeRecord(1024)
	for i := 0; i < 100; i++ {
		_ = wm.Write("str_bench", rec)
	}

	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = wm.SealSegment("str_bench")
	}
}
