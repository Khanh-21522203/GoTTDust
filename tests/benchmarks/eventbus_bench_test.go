package benchmarks

import (
	"fmt"
	"testing"
	"time"

	"GoTTDust/internal/domain"
)

func BenchmarkEventBusPublish_NoSubscribers(b *testing.B) {
	bus := domain.NewEventBus()
	event := domain.SchemaCreated{
		SchemaID:  "sch_bench",
		Name:      "bench_schema",
		Namespace: "bench",
		Version:   1,
		Actor:     "bench",
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bus.Publish(event)
	}
}

func BenchmarkEventBusPublish_1Subscriber(b *testing.B) {
	bus := domain.NewEventBus()
	bus.Subscribe("SchemaCreated", func(e domain.Event) {
		_ = e
	})
	event := domain.SchemaCreated{
		SchemaID:  "sch_bench",
		Name:      "bench_schema",
		Namespace: "bench",
		Version:   1,
		Actor:     "bench",
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bus.Publish(event)
	}
}

func BenchmarkEventBusPublish_10Subscribers(b *testing.B) {
	bus := domain.NewEventBus()
	for i := 0; i < 10; i++ {
		bus.Subscribe("SchemaCreated", func(e domain.Event) {
			_ = e
		})
	}
	event := domain.SchemaCreated{
		SchemaID:  "sch_bench",
		Name:      "bench_schema",
		Namespace: "bench",
		Version:   1,
		Actor:     "bench",
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bus.Publish(event)
	}
}

func BenchmarkEventBusPublish_SubscribeAll(b *testing.B) {
	bus := domain.NewEventBus()
	bus.SubscribeAll(func(e domain.Event) {
		_ = e
	})
	event := domain.StreamCreated{
		StreamID:  "str_bench",
		Name:      "bench_stream",
		SchemaID:  "sch_bench",
		Actor:     "bench",
		Timestamp: time.Now(),
	}
	b.ResetTimer()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bus.Publish(event)
	}
}

func BenchmarkEventBusPublishConcurrent(b *testing.B) {
	bus := domain.NewEventBus()
	bus.Subscribe("SchemaCreated", func(e domain.Event) {
		_ = e
	})
	b.ResetTimer()
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		event := domain.SchemaCreated{
			SchemaID:  "sch_bench",
			Name:      "bench_schema",
			Namespace: "bench",
			Version:   1,
			Actor:     "bench",
			Timestamp: time.Now(),
		}
		for pb.Next() {
			bus.Publish(event)
		}
	})
}

func BenchmarkEventBusSubscribe(b *testing.B) {
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		bus := domain.NewEventBus()
		for j := 0; j < 100; j++ {
			bus.Subscribe(fmt.Sprintf("Event_%d", j), func(e domain.Event) {
				_ = e
			})
		}
	}
}
