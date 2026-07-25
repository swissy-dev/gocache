package memory

import (
	"context"
	"strconv"
	"sync"
	"testing"
	"time"
)

func benchSaturatedChurn(b *testing.B, maxEntries int) {
	b.Helper()
	d := New(WithMaxEntries(maxEntries))
	ctx := context.Background()
	value := make([]byte, 64)

	for i := range maxEntries {
		if err := d.Set(ctx, "seed:"+strconv.Itoa(i), value, time.Hour); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; b.Loop(); i++ {
		if err := d.Set(ctx, "churn:"+strconv.Itoa(i), value, time.Hour); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkSaturatedChurnAllLive10k(b *testing.B) { benchSaturatedChurn(b, 10_000) }
func BenchmarkSaturatedChurnAllLive50k(b *testing.B) { benchSaturatedChurn(b, 50_000) }

func BenchmarkSaturatedConcurrentGetSet(b *testing.B) {
	const maxEntries = 10_000
	d := New(WithMaxEntries(maxEntries))
	ctx := context.Background()
	value := make([]byte, 64)

	for i := range maxEntries {
		if err := d.Set(ctx, "seed:"+strconv.Itoa(i), value, time.Hour); err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	b.ResetTimer()

	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; ; i++ {
				select {
				case <-stop:
					return
				default:
				}
				_, _, _ = d.Get(ctx, "seed:"+strconv.Itoa(i%maxEntries))
			}
		}()
	}

	for i := 0; b.Loop(); i++ {
		if err := d.Set(ctx, "churn:"+strconv.Itoa(i), value, time.Hour); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	close(stop)
	wg.Wait()
}
