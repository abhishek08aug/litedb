# Metrics, Slow Query Log & Distributed Tracing

A database cannot be operated if its behaviour is invisible. Observability answers "is it healthy, and if not, why?" through three complementary pillars, each at a different granularity:

- **Metrics** — aggregate numbers over time (throughput, latency distribution, error rate).
- **Slow query log** — the *outliers*: individual queries that exceeded a latency threshold.
- **Distributed tracing** — a per-request breakdown of *where the time went*.

LiteDB implements all three, mirroring Prometheus, MySQL's slow query log, and tracing systems such as Jaeger.

---

## Metrics

Three metric types cover most operational questions:

| Type | Behaviour | Example |
|------|-----------|---------|
| **Counter** | Monotonically increasing | total queries, total errors |
| **Gauge** | Goes up and down | active connections, MemTable size |
| **Histogram** | Distribution of observed values | query latency |

```python
metrics = MetricsRegistry()
metrics.counter("queries_total").inc()
metrics.gauge("active_connections").set(7)
metrics.histogram("query_latency_ms").observe(12.4)
```

### Why histograms and percentiles, not averages

The mean latency hides the experience of the worst-served requests. If 95 requests take 5 ms and 5 take 500 ms, the mean (~30 ms) describes *no actual request*. Percentiles do:

- **p50 (median)** — the typical request.
- **p95 / p99** — the tail; the experience of the unluckiest 5% / 1%.

The tail is what users notice and what SLOs are written against, so LiteDB's `Histogram` computes exact percentiles from retained samples (and keeps coarse buckets for cheap summaries):

```python
h = metrics.histogram("query_latency_ms")
h.percentile(50)   # typical
h.percentile(99)   # tail
```

---

## Slow Query Log

Metrics tell you *that* latency is high; the slow query log tells you *which* queries are responsible. Any query whose duration exceeds a configured threshold is recorded with its text, duration, and execution shape:

```
record(query, duration_ms, rows_examined, rows_returned)
```

From the captured rows, the log derives analysis that points straight at the problem:

- **Top slow queries** — ranked by duration.
- **Full-table scans** — queries with poor **selectivity** (rows examined ≫ rows returned), the classic "missing index" signature.

```
selectivity = rows_returned / rows_examined      # low ⇒ scanning far more than it keeps
```

A query that examines 1,000,000 rows to return 10 has selectivity 0.00001 — a flashing sign that an index is missing. This mirrors MySQL's `slow_query_log` and PostgreSQL's `log_min_duration_statement`.

---

## Distributed Tracing

A single slow query is itself made of stages — parse, plan, execute, return. Tracing breaks one request into timed **spans** so the expensive stage is obvious. Each request gets a **trace id**; each sub-operation is a span with a start time, duration, and metadata, optionally nested under a parent span.

```
trace 7f3a  (SELECT ... )                         total 142 ms
├─ parse        2 ms   ▏
├─ plan         5 ms   ▎
├─ execute    activity 130 ms   ████████████████   ← the cost is here
│   ├─ index-scan   8 ms
│   └─ table-scan 121 ms        ← drill down: the table scan dominates
└─ return       5 ms   ▎
```

The **waterfall** view makes the critical path visible at a glance — the same model used by Jaeger, Zipkin, CockroachDB, and Spanner. Where metrics aggregate and the slow log isolates, tracing *explains* a single request.

---

## Putting It Together

The three pillars are layered, each narrowing the search:

```
Metrics        → "p99 latency doubled at 14:05"          (what & when, aggregate)
   │
   ▼
Slow query log → "these 5 SELECTs on `orders` are slow,  (which queries)
                  selectivity ~0.0001 → full scans"
   │
   ▼
Tracing        → "for this query, execute→table-scan is  (why, per request)
                  121 of 142 ms"
```

LiteDB ties them together so a single executed query updates the latency histogram, is captured by the slow query log if it crosses the threshold, and produces a trace with per-stage spans.

---

## Real-World Systems

| Mechanism | Where it appears in production |
|-----------|-------------------------------|
| Counters / gauges / histograms | Prometheus, StatsD, OpenTelemetry metrics |
| Percentile latency (p50/p95/p99) | Every latency SLO and Grafana dashboard |
| Slow query log | MySQL `slow_query_log`, PostgreSQL `log_min_duration_statement` |
| Distributed tracing (trace id + spans) | Jaeger, Zipkin, Datadog APM, CockroachDB, Spanner |

---

## Review Questions

1. Why can the *mean* latency be misleading, and which statistic better reflects user experience?
2. Which metric type fits "active connections," and which fits "total queries served"?
3. What does low query **selectivity** indicate, and what fix does it usually point to?
4. Metrics show p99 latency has risen. What is the next observability tool to consult, and then the next?
5. What does a trace add that a slow query log entry does not?

---

**Implemented in:** `litedb-python/metrics.py`  
**Java:** `com.litedb.metrics.MetricsRegistry`

**Next:** return to the [documentation index](../README.md), or explore the [integration that ties every module together](../../litedb-python/run_demo.py).
