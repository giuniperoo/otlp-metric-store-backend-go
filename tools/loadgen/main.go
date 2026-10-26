// Command loadgen sends synthetic OTLP gauge metrics to the service and reports throughput,
// latency and errors.
//
//	go run ./tools/loadgen -addr localhost:4317 -concurrency 16 -duration 30s
//
// Every request carries resources x metrics x series data points; the same series are sent
// repeatedly with advancing timestamps, as real exporters do.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"dash0.com/otlp-metric-store-backend/internal/otlptest"
)

func main() {
	addr := flag.String("addr", "localhost:4317", "OTLP/gRPC address")
	concurrency := flag.Int("concurrency", 8, "Concurrent exporters")
	duration := flag.Duration("duration", 20*time.Second, "Test duration")
	resources := flag.Int("resources", 5, "Resources per request")
	metrics := flag.Int("metrics", 20, "Metrics per resource")
	seriesPerMetric := flag.Int("series", 10, "Data points (series) per metric")
	flag.Parse()

	conn, err := grpc.NewClient(*addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer conn.Close()
	client := colmetricspb.NewMetricsServiceClient(conn)
	pointsPerRequest := *resources * *metrics * *seriesPerMetric

	var (
		requests, points atomic.Int64
		mu               sync.Mutex
		latencies        []time.Duration
		errorsByCode     = map[string]int{}
	)
	ctx, cancel := context.WithTimeout(context.Background(), *duration)
	defer cancel()
	start := time.Now()
	var wg sync.WaitGroup
	for worker := range *concurrency {
		wg.Go(func() {
			// Each worker owns distinct resources, so the series space is concurrency x request.
			base := otlptest.LoadRequest(*resources, *metrics, *seriesPerMetric, 0)
			for _, rm := range base {
				rm.Resource.Attributes = append(rm.Resource.Attributes, otlptest.Int("loadgen.worker", int64(worker)))
			}
			for ctx.Err() == nil {
				setTime(base, uint64(time.Now().UnixNano()))
				t := time.Now()
				// Requests get their own timeout, so the end of the test does not cancel them.
				reqCtx, cancelReq := context.WithTimeout(context.Background(), 10*time.Second)
				_, err := client.Export(reqCtx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: base})
				cancelReq()
				elapsed := time.Since(t)
				mu.Lock()
				if err != nil {
					errorsByCode[status.Code(err).String()]++
				} else {
					latencies = append(latencies, elapsed)
				}
				mu.Unlock()
				if err == nil {
					requests.Add(1)
					points.Add(int64(pointsPerRequest))
				}
			}
		})
	}
	wg.Wait()
	elapsed := time.Since(start)

	slices.Sort(latencies)
	pct := func(p float64) time.Duration {
		if len(latencies) == 0 {
			return 0
		}
		return latencies[int(float64(len(latencies)-1)*p)]
	}
	fmt.Printf("duration %s, concurrency %d, %d data points per request (%d series per worker)\n",
		elapsed.Round(time.Millisecond), *concurrency, pointsPerRequest, pointsPerRequest)
	fmt.Printf("succeeded: %d requests (%.0f/s), %d data points (%.0f/s)\n",
		requests.Load(), float64(requests.Load())/elapsed.Seconds(), points.Load(), float64(points.Load())/elapsed.Seconds())
	fmt.Printf("latency: p50 %s, p90 %s, p99 %s, max %s\n", pct(0.5), pct(0.9), pct(0.99), pct(1))
	if len(errorsByCode) > 0 {
		fmt.Printf("errors by gRPC code: %v\n", errorsByCode)
	}
}

func setTime(resourceMetrics []*metricspb.ResourceMetrics, timeUnixNano uint64) {
	for _, rm := range resourceMetrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				for _, dp := range m.GetGauge().GetDataPoints() {
					dp.TimeUnixNano = timeUnixNano
				}
			}
		}
	}
}
