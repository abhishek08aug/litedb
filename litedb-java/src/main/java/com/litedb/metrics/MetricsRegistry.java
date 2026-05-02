package com.litedb.metrics;

import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

/**
 * MetricsRegistry — In-process metrics: counters, gauges, histograms, timers.
 *
 * CONCEPT:
 *   Every production database exposes metrics so operators can monitor health.
 *   Prometheus, Grafana, Datadog all consume these metrics.
 *
 *   Metric types:
 *     Counter   — monotonically increasing (queries_total, errors_total)
 *     Gauge     — current value, can go up/down (connections_active, cache_size)
 *     Histogram — distribution of values (query_latency_ms, row_count)
 *     Timer     — measures duration of operations (wraps Histogram)
 *
 *   Key database metrics to track:
 *     - queries_per_second (QPS)
 *     - p50/p95/p99 query latency
 *     - cache hit rate
 *     - WAL write throughput
 *     - compaction frequency
 *     - active connections
 *     - replication lag
 *
 *   This is how Prometheus client_java, Micrometer, and Dropwizard Metrics work.
 */
public class MetricsRegistry {

    // ------------------------------------------------------------------ //
    //  Counter                                                            //
    // ------------------------------------------------------------------ //

    public static class Counter {
        private final String       name;
        private final AtomicLong   value = new AtomicLong(0);

        public Counter(String name) { this.name = name; }

        public void inc()           { value.incrementAndGet(); }
        public void inc(long delta) { value.addAndGet(delta); }
        public long get()           { return value.get(); }

        @Override public String toString() { return name + "=" + value.get(); }
    }

    // ------------------------------------------------------------------ //
    //  Gauge                                                              //
    // ------------------------------------------------------------------ //

    public static class Gauge {
        private final String     name;
        private volatile double  value = 0;

        public Gauge(String name) { this.name = name; }

        public void set(double v)  { this.value = v; }
        public void inc(double d)  { this.value += d; }
        public void dec(double d)  { this.value -= d; }
        public double get()        { return value; }

        @Override public String toString() { return name + "=" + String.format("%.2f", value); }
    }

    // ------------------------------------------------------------------ //
    //  Histogram (HDR-style: fixed buckets)                              //
    // ------------------------------------------------------------------ //

    public static class Histogram {
        private final String      name;
        private final long[]      bucketBounds; // upper bounds in ms
        private final AtomicLong[] bucketCounts;
        private final AtomicLong  totalCount = new AtomicLong(0);
        private final AtomicLong  totalSum   = new AtomicLong(0);
        private volatile long     min = Long.MAX_VALUE;
        private volatile long     max = Long.MIN_VALUE;

        public Histogram(String name, long... bucketBounds) {
            this.name         = name;
            this.bucketBounds = bucketBounds;
            this.bucketCounts = new AtomicLong[bucketBounds.length + 1]; // +1 for overflow
            for (int i = 0; i < bucketCounts.length; i++) bucketCounts[i] = new AtomicLong(0);
        }

        public void record(long value) {
            totalCount.incrementAndGet();
            totalSum.addAndGet(value);
            if (value < min) min = value;
            if (value > max) max = value;
            for (int i = 0; i < bucketBounds.length; i++) {
                if (value <= bucketBounds[i]) { bucketCounts[i].incrementAndGet(); return; }
            }
            bucketCounts[bucketBounds.length].incrementAndGet(); // overflow
        }

        public double mean() {
            long count = totalCount.get();
            return count == 0 ? 0 : (double) totalSum.get() / count;
        }

        /** Approximate percentile from bucket distribution. */
        public double percentile(double p) {
            long count = totalCount.get();
            if (count == 0) return 0;
            long target = (long) Math.ceil(p * count);
            long cumulative = 0;
            for (int i = 0; i < bucketBounds.length; i++) {
                cumulative += bucketCounts[i].get();
                if (cumulative >= target) return bucketBounds[i];
            }
            return max;
        }

        public long count() { return totalCount.get(); }
        public long min()   { return min == Long.MAX_VALUE ? 0 : min; }
        public long max()   { return max == Long.MIN_VALUE ? 0 : max; }

        public String summary() {
            return String.format("%s count=%d mean=%.1f min=%d max=%d p50=%.0f p95=%.0f p99=%.0f",
                    name, count(), mean(), min(), max(),
                    percentile(0.50), percentile(0.95), percentile(0.99));
        }

        @Override public String toString() { return summary(); }
    }

    // ------------------------------------------------------------------ //
    //  Timer (wraps Histogram, measures nanoseconds → ms)                //
    // ------------------------------------------------------------------ //

    public static class Timer {
        private final Histogram histogram;

        public Timer(String name) {
            this.histogram = new Histogram(name,
                    1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000);
        }

        /** Returns a Sample that records elapsed time on close(). */
        public Sample start() { return new Sample(this); }

        public void record(long ms) { histogram.record(ms); }

        public Histogram histogram() { return histogram; }

        public static class Sample implements AutoCloseable {
            private final Timer timer;
            private final long  startNs = System.nanoTime();

            Sample(Timer timer) { this.timer = timer; }

            @Override public void close() {
                long elapsedMs = (System.nanoTime() - startNs) / 1_000_000;
                timer.record(elapsedMs);
            }
        }

        @Override public String toString() { return histogram.summary(); }
    }

    // ------------------------------------------------------------------ //
    //  Registry                                                          //
    // ------------------------------------------------------------------ //

    private final Map<String, Counter>   counters   = new ConcurrentHashMap<>();
    private final Map<String, Gauge>     gauges     = new ConcurrentHashMap<>();
    private final Map<String, Histogram> histograms = new ConcurrentHashMap<>();
    private final Map<String, Timer>     timers     = new ConcurrentHashMap<>();

    public Counter   counter(String name)   { return counters.computeIfAbsent(name, Counter::new); }
    public Gauge     gauge(String name)     { return gauges.computeIfAbsent(name, Gauge::new); }
    public Histogram histogram(String name) {
        return histograms.computeIfAbsent(name, n ->
                new Histogram(n, 1,2,5,10,25,50,100,250,500,1000,5000,10000));
    }
    public Timer     timer(String name)     { return timers.computeIfAbsent(name, Timer::new); }

    /** Print all metrics to stdout (like a /metrics endpoint). */
    public void report() {
        System.out.println("\n========== METRICS REPORT ==========");
        counters.values().forEach(c -> System.out.println("  [counter]   " + c));
        gauges.values().forEach(g   -> System.out.println("  [gauge]     " + g));
        histograms.values().forEach(h -> System.out.println("  [histogram] " + h));
        timers.values().forEach(t   -> System.out.println("  [timer]     " + t));
        System.out.println("=====================================\n");
    }

    // ======================================================================= //
    //  DEMO                                                                    //
    // ======================================================================= //

    public static void main(String[] args) throws InterruptedException {
        System.out.println("============================================================");
        System.out.println("METRICS REGISTRY DEMO");
        System.out.println("============================================================\n");

        MetricsRegistry metrics = new MetricsRegistry();

        // Counters
        Counter queries  = metrics.counter("litedb_queries_total");
        Counter errors   = metrics.counter("litedb_errors_total");
        Counter writes   = metrics.counter("litedb_writes_total");

        // Gauges
        Gauge connections = metrics.gauge("litedb_connections_active");
        Gauge cacheHitPct = metrics.gauge("litedb_cache_hit_pct");

        // Timers
        Timer queryTimer = metrics.timer("litedb_query_duration_ms");
        Timer writeTimer = metrics.timer("litedb_write_duration_ms");

        // Simulate database activity
        System.out.println("[Simulating database activity...]");
        Random rng = new Random(42);

        for (int i = 0; i < 1000; i++) {
            queries.inc();
            // Simulate query latency: mostly fast, occasional slow
            long latency = rng.nextInt(100) < 95
                    ? rng.nextInt(10) + 1      // 95% under 10ms
                    : rng.nextInt(500) + 100;  // 5% slow queries
            queryTimer.record(latency);

            if (rng.nextInt(100) < 40) { // 40% are writes
                writes.inc();
                writeTimer.record(rng.nextInt(5) + 1);
            }
            if (rng.nextInt(1000) < 5) errors.inc(); // 0.5% error rate
        }

        connections.set(42);
        cacheHitPct.set(87.3);

        // Report
        metrics.report();

        // Histogram detail
        System.out.println("[Query latency breakdown]");
        Histogram h = queryTimer.histogram();
        System.out.println("  Total queries : " + h.count());
        System.out.printf("  Mean latency  : %.1f ms%n", h.mean());
        System.out.println("  Min latency   : " + h.min() + " ms");
        System.out.println("  Max latency   : " + h.max() + " ms");
        System.out.printf("  p50           : %.0f ms%n", h.percentile(0.50));
        System.out.printf("  p95           : %.0f ms%n", h.percentile(0.95));
        System.out.printf("  p99           : %.0f ms%n", h.percentile(0.99));

        // Try-with-resources timer
        System.out.println("\n[Timer via try-with-resources]");
        try (Timer.Sample s = queryTimer.start()) {
            Thread.sleep(7); // simulate 7ms query
        }
        System.out.printf("  After timed op: count=%d%n", queryTimer.histogram().count());

        System.out.println("\n[Done] Metrics demo complete.");
        System.out.println("\nKey insights:");
        System.out.println("  1. Counters track totals; gauges track current state");
        System.out.println("  2. Histograms give p50/p95/p99 — critical for SLA monitoring");
        System.out.println("  3. p99 latency matters more than mean for user experience");
        System.out.println("  4. Prometheus scrapes /metrics; Grafana visualizes it");
        System.out.println("  5. Every production DB (Postgres, Cassandra, MongoDB) exposes metrics");
    }
}