"""
metrics.py — Metrics, Slow Query Log & Query Tracing

CONCEPT:
  Production databases expose observability through three pillars:

  1. METRICS — "What is the system doing right now?"
     Counters, gauges, histograms tracked over time.
     Examples:
       - queries_per_second (counter)
       - active_connections (gauge)
       - query_latency_p99 (histogram)
       - cache_hit_ratio (derived gauge)
     Used by: Prometheus + Grafana, DataDog, CloudWatch

  2. SLOW QUERY LOG — "Which queries are hurting performance?"
     Any query exceeding a threshold (e.g., 100ms) is logged with:
       - SQL text
       - Execution time
       - Rows examined vs rows returned
       - Explain plan (index used or full scan)
     MySQL: slow_query_log, PostgreSQL: log_min_duration_statement

  3. QUERY TRACING — "What happened inside a single query?"
     Distributed tracing (OpenTelemetry / Jaeger style):
       - Each query gets a trace_id
       - Sub-operations are spans: parse → plan → execute → return
       - Each span has start_time, duration, metadata
     Used by: CockroachDB, Spanner, Vitess

  4. HISTOGRAMS — "What is the distribution of latencies?"
     Bucket-based: count queries in latency ranges
       [0-1ms]: 1000, [1-10ms]: 500, [10-100ms]: 50, [100ms+]: 2
     Percentiles computed from buckets:
       p50 = median, p95, p99, p999 (the "tail latency")

  Why this matters:
    - p99 latency matters more than average (tail affects 1% of users)
    - Slow queries often cause cascading failures under load
    - Metrics help capacity planning and SLA monitoring
"""

import random
import statistics
import threading
import time
from collections import deque
from contextlib import contextmanager
from dataclasses import dataclass, field
from typing import Optional

# ======================================================================= #
#  Metrics Registry                                                        #
# ======================================================================= #

class Counter:
    """Monotonically increasing counter (e.g., total_queries)."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0.0
        self._lock = threading.Lock()

    def inc(self, amount: float = 1.0):
        with self._lock:
            self._value += amount

    def get(self) -> float:
        with self._lock:
            return self._value

    def reset(self):
        with self._lock:
            self._value = 0


class Gauge:
    """Current value that can go up or down (e.g., active_connections)."""

    def __init__(self, name: str, description: str = ""):
        self.name = name
        self.description = description
        self._value = 0.0
        self._lock = threading.Lock()

    def set(self, value: float):
        with self._lock:
            self._value = value

    def inc(self, amount: float = 1.0):
        with self._lock:
            self._value += amount

    def dec(self, amount: float = 1.0):
        with self._lock:
            self._value -= amount

    def get(self) -> float:
        with self._lock:
            return self._value


class Histogram:
    """
    Latency histogram with configurable buckets.

    Stores counts per bucket and computes percentiles.
    Buckets are upper bounds in milliseconds.
    """

    DEFAULT_BUCKETS = [0.5, 1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, float("inf")]

    def __init__(self, name: str, description: str = "", buckets: Optional[list[float]] = None):
        self.name = name
        self.description = description
        self.buckets = sorted(buckets or self.DEFAULT_BUCKETS)
        self._counts = [0] * len(self.buckets)
        self._sum = 0.0
        self._total = 0
        self._lock = threading.Lock()
        self._samples: deque[float] = deque(maxlen=10_000)  # for exact percentiles

    def observe(self, value_ms: float):
        """Record a latency observation in milliseconds."""
        with self._lock:
            self._sum += value_ms
            self._total += 1
            self._samples.append(value_ms)
            for i, upper in enumerate(self.buckets):
                if value_ms <= upper:
                    self._counts[i] += 1

    def percentile(self, p: float) -> float:
        """Compute exact percentile from samples (0-100)."""
        with self._lock:
            if not self._samples:
                return 0.0
            sorted_samples = sorted(self._samples)
            idx = int(len(sorted_samples) * p / 100)
            idx = min(idx, len(sorted_samples) - 1)
            return sorted_samples[idx]

    def mean(self) -> float:
        with self._lock:
            return self._sum / self._total if self._total > 0 else 0.0

    def count(self) -> int:
        with self._lock:
            return self._total

    def bucket_summary(self) -> list[dict]:
        with self._lock:
            return [
                {"le": b, "count": c}
                for b, c in zip(self.buckets, self._counts)
            ]


class MetricsRegistry:
    """Central registry for all metrics."""

    def __init__(self):
        self._counters: dict[str, Counter] = {}
        self._gauges: dict[str, Gauge] = {}
        self._histograms: dict[str, Histogram] = {}
        self._lock = threading.Lock()

    def counter(self, name: str, description: str = "") -> Counter:
        with self._lock:
            if name not in self._counters:
                self._counters[name] = Counter(name, description)
            return self._counters[name]

    def gauge(self, name: str, description: str = "") -> Gauge:
        with self._lock:
            if name not in self._gauges:
                self._gauges[name] = Gauge(name, description)
            return self._gauges[name]

    def histogram(self, name: str, description: str = "",
                  buckets: Optional[list[float]] = None) -> Histogram:
        with self._lock:
            if name not in self._histograms:
                self._histograms[name] = Histogram(name, description, buckets)
            return self._histograms[name]

    def snapshot(self) -> dict:
        """Return current values of all metrics."""
        with self._lock:
            result = {}
            for name, c in self._counters.items():
                result[name] = {"type": "counter", "value": c.get()}
            for name, g in self._gauges.items():
                result[name] = {"type": "gauge", "value": g.get()}
            for name, h in self._histograms.items():
                result[name] = {
                    "type": "histogram",
                    "count": h.count(),
                    "mean_ms": round(h.mean(), 3),
                    "p50_ms": round(h.percentile(50), 3),
                    "p95_ms": round(h.percentile(95), 3),
                    "p99_ms": round(h.percentile(99), 3),
                    "p999_ms": round(h.percentile(99.9), 3),
                }
            return result


# ======================================================================= #
#  Slow Query Log                                                          #
# ======================================================================= #

@dataclass
class SlowQueryEntry:
    query: str
    duration_ms: float
    rows_examined: int
    rows_returned: int
    timestamp: float = field(default_factory=time.time)
    user: str = "unknown"
    plan: str = ""  # "INDEX_SCAN" or "FULL_SCAN"

    @property
    def selectivity(self) -> float:
        """rows_returned / rows_examined — lower = more selective."""
        if self.rows_examined == 0:
            return 1.0
        return self.rows_returned / self.rows_examined

    def __repr__(self):
        return (f"SlowQuery({self.duration_ms:.1f}ms, "
                f"rows={self.rows_returned}/{self.rows_examined}, "
                f"plan={self.plan!r}, sql={self.query[:50]!r})")


class SlowQueryLog:
    """
    Records queries that exceed a latency threshold.
    Provides analysis: top slow queries, full-scan queries, etc.
    """

    def __init__(self, threshold_ms: float = 100.0, max_entries: int = 1000):
        self.threshold_ms = threshold_ms
        self._entries: deque[SlowQueryEntry] = deque(maxlen=max_entries)
        self._lock = threading.Lock()
        self.total_logged = 0

    def record(self, query: str, duration_ms: float,
               rows_examined: int = 0, rows_returned: int = 0,
               user: str = "unknown", plan: str = "FULL_SCAN"):
        """Log a query if it exceeds the threshold."""
        if duration_ms >= self.threshold_ms:
            entry = SlowQueryEntry(
                query=query,
                duration_ms=duration_ms,
                rows_examined=rows_examined,
                rows_returned=rows_returned,
                user=user,
                plan=plan,
            )
            with self._lock:
                self._entries.append(entry)
                self.total_logged += 1

    def top_slow(self, n: int = 10) -> list[SlowQueryEntry]:
        """Return the N slowest queries."""
        with self._lock:
            return sorted(self._entries, key=lambda e: e.duration_ms, reverse=True)[:n]

    def full_scans(self) -> list[SlowQueryEntry]:
        """Return queries that did a full table scan."""
        with self._lock:
            return [e for e in self._entries if e.plan == "FULL_SCAN"]

    def recent(self, n: int = 20) -> list[SlowQueryEntry]:
        with self._lock:
            return list(self._entries)[-n:]

    def summary(self) -> dict:
        with self._lock:
            if not self._entries:
                return {"total": 0}
            durations = [e.duration_ms for e in self._entries]
            return {
                "total_logged": self.total_logged,
                "threshold_ms": self.threshold_ms,
                "max_ms": max(durations),
                "avg_ms": round(statistics.mean(durations), 2),
                "full_scans": sum(1 for e in self._entries if e.plan == "FULL_SCAN"),
                "index_scans": sum(1 for e in self._entries if e.plan == "INDEX_SCAN"),
            }


# ======================================================================= #
#  Query Tracer (OpenTelemetry-style spans)                               #
# ======================================================================= #

@dataclass
class Span:
    """A single operation within a query trace."""
    name: str
    trace_id: str
    span_id: str
    parent_span_id: Optional[str]
    start_time: float = field(default_factory=time.time)
    end_time: Optional[float] = None
    attributes: dict = field(default_factory=dict)
    status: str = "OK"  # OK, ERROR

    @property
    def duration_ms(self) -> float:
        if self.end_time is None:
            return (time.time() - self.start_time) * 1000
        return (self.end_time - self.start_time) * 1000

    def finish(self, status: str = "OK", **attrs):
        self.end_time = time.time()
        self.status = status
        self.attributes.update(attrs)

    def __repr__(self):
        return (f"Span({self.name!r}, {self.duration_ms:.2f}ms, "
                f"status={self.status})")


class Trace:
    """A complete trace for one query execution."""

    def __init__(self, trace_id: str, query: str):
        self.trace_id = trace_id
        self.query = query
        self.spans: list[Span] = []
        self._span_counter = 0
        self._lock = threading.Lock()

    def start_span(self, name: str, parent: Optional[Span] = None) -> Span:
        with self._lock:
            self._span_counter += 1
            span = Span(
                name=name,
                trace_id=self.trace_id,
                span_id=f"{self.trace_id[:8]}-{self._span_counter:03d}",
                parent_span_id=parent.span_id if parent else None,
            )
            self.spans.append(span)
            return span

    @property
    def total_duration_ms(self) -> float:
        if not self.spans:
            return 0.0
        start = min(s.start_time for s in self.spans)
        end = max(s.end_time or time.time() for s in self.spans)
        return (end - start) * 1000

    def waterfall(self) -> str:
        """ASCII waterfall chart of spans."""
        if not self.spans:
            return "(no spans)"
        t0 = min(s.start_time for s in self.spans)
        total = self.total_duration_ms or 1
        lines = [f"Trace {self.trace_id[:12]}... ({total:.2f}ms total)"]
        lines.append(f"  Query: {self.query[:60]}")
        lines.append("")
        for span in self.spans:
            offset = (span.start_time - t0) * 1000
            dur = span.duration_ms
            bar_start = int(offset / total * 40)
            bar_len = max(1, int(dur / total * 40))
            bar = " " * bar_start + "█" * bar_len
            indent = "  " if span.parent_span_id else ""
            lines.append(f"  {indent}{span.name:20s} {bar}  {dur:.2f}ms")
        return "\n".join(lines)


class QueryTracer:
    """Manages traces for query execution."""

    def __init__(self, max_traces: int = 500):
        self._traces: deque[Trace] = deque(maxlen=max_traces)
        self._lock = threading.Lock()
        self._id_counter = 0

    def new_trace(self, query: str) -> Trace:
        with self._lock:
            self._id_counter += 1
            trace_id = f"trace-{self._id_counter:06d}-{int(time.time()*1000)%100000:05d}"
            trace = Trace(trace_id, query)
            self._traces.append(trace)
            return trace

    @contextmanager
    def span(self, trace: Trace, name: str, parent: Optional[Span] = None, **attrs):
        """Context manager for a span."""
        s = trace.start_span(name, parent)
        s.attributes.update(attrs)
        try:
            yield s
            s.finish("OK")
        except Exception as e:
            s.finish("ERROR", error=str(e))
            raise

    def recent_traces(self, n: int = 10) -> list[Trace]:
        with self._lock:
            return list(self._traces)[-n:]


# ======================================================================= #
#  Instrumented Database (ties everything together)                       #
# ======================================================================= #

class InstrumentedDB:
    """
    A mock database that records metrics, slow queries, and traces
    for every operation.
    """

    def __init__(self, slow_query_threshold_ms: float = 50.0):
        self.metrics = MetricsRegistry()
        self.slow_log = SlowQueryLog(threshold_ms=slow_query_threshold_ms)
        self.tracer = QueryTracer()

        # Register standard metrics
        self.m_queries_total   = self.metrics.counter("queries_total", "Total queries executed")
        self.m_queries_errors  = self.metrics.counter("queries_errors", "Total query errors")
        self.m_active_conns    = self.metrics.gauge("active_connections", "Current active connections")
        self.m_cache_hits      = self.metrics.counter("cache_hits", "Buffer pool cache hits")
        self.m_cache_misses    = self.metrics.counter("cache_misses", "Buffer pool cache misses")
        self.m_rows_read       = self.metrics.counter("rows_read_total", "Total rows read")
        self.m_rows_written    = self.metrics.counter("rows_written_total", "Total rows written")
        self.m_latency         = self.metrics.histogram("query_latency_ms", "Query latency in ms")

        # Simulated data store
        self._data: dict[str, str] = {}
        self._table_size = 10_000  # simulated rows

    def execute(self, sql: str, user: str = "app") -> dict:
        """Execute a query with full instrumentation."""
        trace = self.tracer.new_trace(sql)
        self.m_queries_total.inc()
        self.m_active_conns.inc()

        start = time.time()
        result = {}

        try:
            with self.tracer.span(trace, "parse", sql=sql) as parse_span:
                time.sleep(random.uniform(0.0001, 0.001))  # parse time
                parse_span.attributes["tokens"] = len(sql.split())

            with self.tracer.span(trace, "plan") as plan_span:
                time.sleep(random.uniform(0.0001, 0.002))  # plan time
                # Simulate index vs full scan decision
                has_index = "WHERE id" in sql or "WHERE name" in sql
                plan = "INDEX_SCAN" if has_index else "FULL_SCAN"
                plan_span.attributes["plan"] = plan
                plan_span.attributes["estimated_rows"] = self._table_size

            with self.tracer.span(trace, "execute", plan=plan) as exec_span:
                # Simulate execution time based on plan
                if plan == "INDEX_SCAN":
                    exec_time = random.uniform(0.001, 0.010)
                    rows_examined = random.randint(1, 100)
                else:
                    exec_time = random.uniform(0.010, 0.200)
                    rows_examined = self._table_size
                time.sleep(exec_time)

                rows_returned = random.randint(0, min(rows_examined, 50))
                exec_span.attributes["rows_examined"] = rows_examined
                exec_span.attributes["rows_returned"] = rows_returned

                # Cache simulation
                if random.random() < 0.7:
                    self.m_cache_hits.inc()
                else:
                    self.m_cache_misses.inc()

                self.m_rows_read.inc(rows_examined)

            with self.tracer.span(trace, "serialize") as ser_span:
                time.sleep(random.uniform(0.0001, 0.001))
                ser_span.attributes["rows"] = rows_returned

            duration_ms = (time.time() - start) * 1000
            self.m_latency.observe(duration_ms)

            # Log slow queries
            self.slow_log.record(
                query=sql,
                duration_ms=duration_ms,
                rows_examined=rows_examined,
                rows_returned=rows_returned,
                user=user,
                plan=plan,
            )

            result = {
                "status": "OK",
                "duration_ms": round(duration_ms, 3),
                "rows_returned": rows_returned,
                "plan": plan,
                "trace_id": trace.trace_id,
            }

        except Exception as e:
            self.m_queries_errors.inc()
            result = {"status": "ERROR", "error": str(e)}
        finally:
            self.m_active_conns.dec()

        return result


# ======================================================================= #
#  DEMO                                                                    #
# ======================================================================= #

if __name__ == "__main__":
    print("=" * 60)
    print("METRICS, SLOW QUERY LOG & QUERY TRACING DEMO")
    print("=" * 60)

    db = InstrumentedDB(slow_query_threshold_ms=30.0)

    # ------------------------------------------------------------------ #
    # Part 1: Execute a mix of queries                                    #
    # ------------------------------------------------------------------ #
    print("\n[Part 1] Executing 30 queries (mix of fast/slow, index/full scan)...")
    queries = [
        "SELECT * FROM users WHERE id = 42",
        "SELECT * FROM orders",
        "SELECT name FROM users WHERE name = 'Alice'",
        "SELECT * FROM products",
        "UPDATE users SET status = 'active' WHERE id = 1",
        "SELECT COUNT(*) FROM events",
        "SELECT * FROM logs WHERE timestamp > 1000",
        "DELETE FROM sessions",
        "SELECT * FROM users WHERE city = 'NYC'",
        "INSERT INTO audit (event) VALUES ('login')",
    ]

    results = []
    for i in range(30):
        sql = queries[i % len(queries)]
        r = db.execute(sql, user="app_user")
        results.append(r)

    print(f"  Executed {len(results)} queries")
    ok = sum(1 for r in results if r["status"] == "OK")
    print(f"  OK: {ok}, Errors: {len(results) - ok}")

    # ------------------------------------------------------------------ #
    # Part 2: Metrics snapshot                                            #
    # ------------------------------------------------------------------ #
    print("\n[Part 2] Metrics snapshot")
    snap = db.metrics.snapshot()
    for name, data in sorted(snap.items()):
        if data["type"] == "counter":
            print(f"  {name:30s} = {data['value']:.0f}")
        elif data["type"] == "gauge":
            print(f"  {name:30s} = {data['value']:.0f}")
        elif data["type"] == "histogram":
            print(f"  {name:30s}  count={data['count']}  "
                  f"mean={data['mean_ms']}ms  "
                  f"p50={data['p50_ms']}ms  "
                  f"p95={data['p95_ms']}ms  "
                  f"p99={data['p99_ms']}ms")

    # Cache hit ratio
    hits = db.m_cache_hits.get()
    misses = db.m_cache_misses.get()
    total_io = hits + misses
    ratio = hits / total_io * 100 if total_io > 0 else 0
    print(f"\n  Cache hit ratio: {ratio:.1f}%  ({hits:.0f} hits / {total_io:.0f} total)")

    # ------------------------------------------------------------------ #
    # Part 3: Slow query log                                              #
    # ------------------------------------------------------------------ #
    print("\n[Part 3] Slow query log")
    summary = db.slow_log.summary()
    print(f"  Summary: {summary}")

    print("\n  Top 5 slowest queries:")
    for entry in db.slow_log.top_slow(5):
        print(f"    {entry.duration_ms:7.1f}ms  {entry.plan:12s}  "
              f"rows={entry.rows_returned}/{entry.rows_examined}  "
              f"sql={entry.query[:40]!r}")

    full_scans = db.slow_log.full_scans()
    print(f"\n  Full table scans logged: {len(full_scans)}")
    if full_scans:
        worst = max(full_scans, key=lambda e: e.rows_examined)
        print(f"  Worst full scan: {worst.rows_examined} rows examined, "
              f"{worst.duration_ms:.1f}ms")

    # ------------------------------------------------------------------ #
    # Part 4: Query trace waterfall                                       #
    # ------------------------------------------------------------------ #
    print("\n[Part 4] Query trace waterfall (last 3 traces)")
    for trace in db.tracer.recent_traces(3):
        print()
        print(trace.waterfall())

    # ------------------------------------------------------------------ #
    # Part 5: Histogram buckets                                           #
    # ------------------------------------------------------------------ #
    print("\n[Part 5] Latency histogram buckets")
    hist = db.m_latency
    print(f"  Total observations: {hist.count()}")
    print("  Bucket distribution:")
    prev = 0
    for bucket in hist.bucket_summary():
        le = bucket["le"]
        count = bucket["count"]
        in_bucket = count - prev
        if in_bucket > 0:
            bar = "█" * in_bucket
            label = f"{le}ms" if le != float("inf") else "∞"
            print(f"    ≤{label:8s}: {in_bucket:3d}  {bar}")
        prev = count

    print("\n  Percentiles:")
    for p in [50, 75, 90, 95, 99, 99.9]:
        print(f"    p{p:5.1f}: {hist.percentile(p):.2f}ms")

    print("\n[Done] Metrics & observability demo complete.")
    print("\nKey insights:")
    print("  1. Counters track totals; gauges track current state; histograms track distributions")
    print("  2. p99 latency matters more than average — tail latency affects real users")
    print("  3. Slow query log identifies queries needing indexes or optimization")
    print("  4. Full table scans on large tables are the #1 performance killer")
    print("  5. Distributed traces show exactly where time is spent inside a query")
    print("  6. Cache hit ratio: aim for >95% to avoid disk I/O bottlenecks")
    print("  7. These are the exact tools used in PostgreSQL, MySQL, MongoDB, CockroachDB")
