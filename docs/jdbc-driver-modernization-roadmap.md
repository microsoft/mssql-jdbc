# JDBC Driver Modernization Roadmap — Off-Heap, Zero-Copy, and AI-Workload Readiness

> Follow-up to [off-heap-memory-strategy.md](./off-heap-memory-strategy.md). That doc covers
> what was built and benchmarked so far (Phases 1-2: `MemoryUtil` centralization, `HEAP`/
> `DIRECT`/`MEMORY_SEGMENT` strategy selection for TDS writer and reader buffers). This doc is
> the strategic roadmap for what comes next, and why - specifically targeting the customer
> outcomes that matter: higher sustained throughput, lower tail latency via reduced GC pressure,
> and readiness for AI/vector workloads, delivered through genuine JDK modernization rather than
> incremental buffer-swapping.

## 0. Phase A results (executed) - and a critical methodology lesson

Phase A (§4 below, as originally planned) was executed: a large sustained fetch
(1,500,000 rows x 512-byte `VARBINARY`, 732 MiB) under a constrained heap (`-Xmx384m`) with GC
logging, comparing `HEAP`/`DIRECT`/`MEMORY_SEGMENT`. The first attempt - three full sequential
runs, one strategy at a time (`HEAP` then `DIRECT` then `MEMORY_SEGMENT`) - appeared to show a
dramatic result: `DIRECT` ran in 8.78s vs `HEAP`'s 17.29s, nearly **2x faster**. That looked like
exactly the signal this whole effort was hoping to find.

**It wasn't real.** Re-running the identical three configurations in *reverse order* inverted
the result (now `HEAP`, run last, was fastest) even after clearing SQL Server's buffer pool
(`DBCC DROPCLEANBUFFERS`) before each run. Whatever was driving the "2x" difference was
monotonic warm-up across the sequential run order (most likely OS/container-level disk or
network cache state in the Docker Desktop/WSL2 stack, since `DBCC DROPCLEANBUFFERS` only clears
SQL Server's own buffer pool, not the underlying filesystem cache) - **not** a genuine
client-side buffer-strategy effect. A naive "run A, then B, then C, compare" benchmark would
have shipped a false, confounded conclusion here.

**Corrected experiment:** round-robin interleaving (`HEAP`, `DIRECT`, `MEMORY_SEGMENT`, repeat)
across 4 rounds, clearing the SQL Server buffer pool before every single run, so each strategy
is equally likely to benefit or suffer from any drift over the session:

| Strategy | Mean elapsed | Rounds won (of 4) |
|---|---|---|
| HEAP | 8462.5 ms | **4 / 4** |
| DIRECT | 8538.6 ms (+0.9%) | 0 / 4 |
| MEMORY_SEGMENT | 8612.3 ms (+1.8%) | 0 / 4 |

**`HEAP` won every single round.** GC pause time itself was a non-factor at this scale: parsed
GC logs showed ~72-78 pauses totaling only ~100-135ms across an 8-17 second run (~1% of total
time) for all three strategies - nowhere near enough to explain any meaningful difference either
way. The small, consistent `DIRECT`/`MEMORY_SEGMENT` slowdown matches the architectural
prediction from the earlier doc exactly: `TDSReader.readIntoPayload`'s non-`hasArray()` branch
adds one real extra heap-scratch-array copy per packet for off-heap strategies, and at 732 MiB
over a few seconds there's no compensating GC-pressure benefit large enough to offset it.

**What this actually tells us:** even deliberately engineering GC pressure (constrained heap,
732 MiB sustained fetch) wasn't enough to produce a measurable off-heap benefit on the read path
in this environment. This strengthens, not weakens, the case made in §2 below: the ceiling isn't
really about buffer backing at this scale - it's the forced copy at the `InputStream`/
`OutputStream` boundary, and no amount of further buffer-strategy tuning will get past that. The
`SocketChannel`+`SSLEngine` rewrite (§2, Phase B) is looking like the real next step, not an
optional one - buffer-strategy selection alone, however it's tuned, is unlikely to ever show a
reliable win while that copy remains mandatory. Whether GC-pressure benefits would emerge at
multi-GB/sustained-hours production scale remains untested and is flagged in §4 as unfinished
work, but should be treated as a hypothesis still needing evidence, not an assumption to design
around.

### 0.1 Concurrent-load follow-up (executed) - same conclusion, from a different angle

The one dimension every benchmark so far had missed: a real connection-pool shape, many
connections active at once, not one connection running one query at a time. Tested directly:
64 threads, each with its own connection, each hammering an indexed range query
(`SELECT TOP 300 ... WHERE id >= ? ORDER BY id`) for 15 seconds, `-Xmx512m`, comparing
`HEAP`/`DIRECT`/`MEMORY_SEGMENT`.

First attempt hit a real benchmark bug worth recording: the target table had no index, so 64
concurrent full-table-scans of a 732 MiB heap table saturated the server (p50 latency in the
*seconds*, not milliseconds). Fixed by adding a clustered index - a reminder that "realistic
concurrent load" numbers are easy to accidentally measure as "server overload" instead.

With that fixed, 5 rounds (3 same-order, 2 with rotated start order to re-check for the same
positional confound that produced a false "2x" result in §0):

| Strategy | Mean QPS | StDev | vs HEAP |
|---|---|---|---|
| HEAP | 577.8 | 51.8 | - |
| DIRECT | 581.5 | 32.8 | +0.6% |
| MEMORY_SEGMENT | 605.5 | 26.9 | +4.8% |

Same lesson as §0: the rotated-order rounds again showed "whichever strategy runs last tends to
win" - the +4.8% is smaller than HEAP's own 51.8 QPS (~9%) run-to-run noise, so this is **not** a
confident result either way.

**The GC pause logs are more interesting, and point the opposite direction from the off-heap
hypothesis:**

| Strategy | Total GC pause (ms), 3 rounds |
|---|---|
| HEAP | 64.8 / 59.3 / 76.8 |
| DIRECT | 89.3 / 88.7 / 94.3 |
| MEMORY_SEGMENT | 79.1 / 81.1 / 79.4 |

`HEAP` consistently showed the **lowest** total GC pause time of the three, not the highest.
This is consistent with, and further confirms, the architectural explanation in §2: on the read
path, `DIRECT`/`MEMORY_SEGMENT` pay one extra heap-scratch-array copy per packet
(`TDSReader.readIntoPayload`'s non-`hasArray()` branch) that `HEAP` doesn't - so under the
*current* architecture, enabling an off-heap strategy doesn't just fail to reduce GC pressure,
it can make it slightly *worse*, since that scratch copy is itself heap garbage on top of
whatever `HEAP` already allocates. (All three are small enough in absolute terms - under 100ms
of pause across a 15-second run - that this isn't dominating latency either way at this scale.)

**Combined with §0, this closes the loop on "does buffer-strategy tuning alone help, at any
scale we've tested": no, consistently, across single-connection throughput, single-connection
GC-pressure-under-constrained-heap, and now multi-connection concurrent load.** The copy at the
socket boundary is the right place to focus next, not further buffer-strategy experimentation.

### 0.2 Isolated copy-removal test (executed) - the decisive result

§0/§0.1 tested buffer-strategy selection *within the current architecture* (which always pays
the socket-boundary copy for `DIRECT`/`MEMORY_SEGMENT`). The one thing still untested: does
*removing* that copy actually matter? This is exactly what
[socketchannel-sslengine-rewrite-scope.md](./socketchannel-sslengine-rewrite-scope.md) §7's POC
was extended to answer directly, rather than guessing from §0/§0.1's indirect evidence.

Built `SocketChannelCopyComparisonPoc`: the same hand-rolled `SocketChannel`-based TDS client
from the POC, run under the same 64-thread/15-second concurrent load as §0.1, in two modes that
are **identical in every other respect**:

- `zerocopy`: every packet's payload is read/processed directly from the off-heap receive
  buffer via absolute `get()` calls.
- `simulated`: identical, except each packet's payload is additionally copied into a `byte[]`
  before processing - precisely reproducing `TDSReader.readIntoPayload`'s non-`hasArray()` copy.

This isolates the *one* variable the entire effort hinges on, with everything else (transport,
connection count, query shape, buffer backing) held constant - something no earlier benchmark
in this doc could do, since they all compared whole strategies, not just the copy in isolation.

6 rounds, alternating start order (ruling out the positional confound from §0):

| Mode | Mean QPS | StDev |
|---|---|---|
| `zerocopy` | 694.8 | 11.8 |
| `simulated` (extra copy) | 701.9 | 17.5 |

**No measurable difference (-1.0%, within noise).** More tellingly: **GC logs showed zero GC
pauses in all 12 runs, for both modes.** At `-Xmx512m` over a sustained 15-second run with real
network I/O and real data processing, heap usage at exit was ~42 MiB out of 512 MiB committed -
the extra per-packet `byte[]` copy that `simulated` mode adds simply never generated enough
garbage to trigger even one young-gen collection, let alone enough to move a tail-latency
number.

**This is the most direct evidence yet, and it's conclusive for the workload shapes tested so
far:** for this driver's typical query shape (rows of a few hundred bytes, TDS packets of a few
KB), the socket-boundary copy this whole effort set out to eliminate is too small, in absolute
allocation volume, to produce measurable GC pressure - independent of buffer strategy,
independent of connection count. Combined with §0's single-connection result and §0.1's
concurrent-load result, three independent angles now agree: **buffer backing, and now even the
copy itself, don't measurably matter at any scale tested.**

**What would change this conclusion:** a workload with dramatically higher sustained allocation
*rate* than anything tested here - e.g. sustained multi-GB/minute throughput for minutes-to-hours
(not 15-second bursts), many more concurrent connections (hundreds, not 64), or much smaller
production heap sizes than `-Xmx512m`. None of that has been tested. Until it is, **building the
full `SocketChannel`/`SSLEngine` `TDSChannel` port cannot be justified by a GC-pressure/tail-
latency argument** - the evidence gathered across every test in this document doesn't support
it. If this modernization is still pursued, it should be justified on other grounds (API
modernization, enabling virtual threads, removing `SSLSocket`/`ProxySocket` legacy complexity,
positioning for future JDK features) rather than the performance hypothesis this effort started
with.

## 1. Why the last round of benchmarks weren't the end of the story

Round 1 (see off-heap-memory-strategy.md §10) measured **mean wall-clock throughput** over 5
short iterations. That's the wrong instrument for the goal stated here: "better throughput and
minimize tail latency via lesser GC pressure." Two things were never measured:

- **GC pause time/frequency and allocation rate.** The entire premise of off-heap buffering is
  reducing what the garbage collector has to scan/move/pause for - which shows up in GC logs and
  JFR allocation profiles, not in a 5-sample throughput average. We didn't look at either.
- **Tail latency distribution (p99/p99.9/p99.99).** Mean throughput can be identical while p99.9
  improves dramatically if off-heap buffering removes a class of GC-pause-induced latency
  spikes. Enterprise customers at Walmart/BlackRock scale care about the tail, not the mean -
  a trading risk calculation or an inventory-sync batch that's "usually fast" but occasionally
  stalls for a stop-the-world pause is the actual production pain point off-heap strategies are
  meant to solve.

**Conclusion: "no clear win" from Round 1 is not yet a real verdict.** It's a verdict on mean
throughput at ~50 MiB payload over a noisy Docker-Desktop loopback link. It says nothing about
GC pause reduction under sustained, GB-scale, memory-pressured load - which is the actual
customer scenario.

## 2. The real architectural ceiling: `OutputStream`/`InputStream`-based I/O

Confirmed empirically on both the write side (§10.1) and read side (§10.2) of the earlier doc:
`TDSChannel` wraps `SSLSocket.getOutputStream()`/`getInputStream()`. Those APIs only accept
`byte[]`. So regardless of whether `TDSWriter`'s/`TDSPacket`'s buffers are heap, direct, or
MemorySegment-backed, there is **always** one heap-array copy at the socket boundary
(`TDSWriter.writeChannelBuffer` / `TDSReader.readIntoPayload`, both added in this effort).

This means today's off-heap strategies can only ever deliver a **partial** win: less heap
*retention* while packets are in flight/chained (good for GC scan time and footprint), but not
a *zero-copy* wire path. To remove the ceiling entirely requires moving `TDSChannel` from
`SSLSocket` (blocking stream I/O) to `SocketChannel` + `SSLEngine` (the standard modern-Java TLS
pattern used by Netty, Jetty, Tomcat NIO, etc.), which allows reading/writing directly
into/out of direct or MemorySegment `ByteBuffer`s with **no** intermediate heap array, ever.

This is a foundational, high-risk, high-reward rewrite - not something to start without first
confirming (via Phase A below) that there's a real customer-relevant signal to chase.

## 3. Why this matters for Walmart/BlackRock-style customers specifically

| Customer profile | Representative workload | What this roadmap targets |
|---|---|---|
| High-throughput retail/OLTP (Walmart-style) | Sustained high-QPS batch writes: inventory, pricing, order updates via `SQLServerBulkCopy`/batched `PreparedStatement` | Write-path zero-copy + fewer/shorter GC pauses under sustained load → more predictable p99 write latency at peak traffic (e.g. Black Friday-scale bursts) |
| Latency-sensitive analytics/trading (BlackRock-style) | Large analytical/reporting fetches: risk calculations, pricing/time-series pulls, potentially GB-scale result sets | Read-path zero-copy + bounded heap footprint while streaming large results → lower p99.9 fetch latency, fewer GC-induced stalls mid-stream |
| AI/RAG and vector search workloads | Bulk embedding ingestion (millions of vectors into a vector index table) and large similarity-search result fetches, using the driver's `VECTOR` type/`SQLServerBulkCopy` | Same zero-copy + GC-pressure benefits, applied specifically to the large, uniform, high-volume payload shape vector workloads produce - arguably the clearest, most clean-cut case for this investment |

The AI/vector angle deserves its own callout: a prior exploration branch already added primitive
`float[]` storage to `microsoft.sql.Vector` and bulk float-buffer get/put in `VectorUtils`
(avoiding autoboxing for vector data specifically - see `docs/memory-segment-vector-case-study.md`
reference in that branch). That work and this effort should converge: vector embeddings are
fixed-width, large, and bulk-transferred - exactly the shape where `MemoryUtil.Strategy` +
eventual zero-copy I/O has the least noise and the clearest signal.

## 4. Fixing the methodology first (cheap, do before any further rewrite)

Before investing in the NIO rewrite (§2), re-run the *existing* `HEAP`/`DIRECT`/`MEMORY_SEGMENT`
infrastructure with instruments that actually measure what matters:

1. **JFR-based allocation profiling** (`jdk.ObjectAllocationSample`,
   `jdk.ObjectAllocationOutsideTLAB`) during both benchmarks, to empirically confirm/refute where
   allocation hotspots really are, rather than relying on code-reading inference.
2. **GC log analysis** (`-Xlog:gc*:file=gc.log`) with a **constrained heap** (`-Xmx` set low
   enough to force real GC activity) - the current benchmarks ran with default/ample heap, so
   GC pressure differences may simply never have been triggered.
3. **Tail latency histograms**, not just mean/median: capture p50/p95/p99/p99.9 per-query
   latency over a sustained run (thousands of iterations, not 5), using something like HdrHistogram.
4. **Realistic scale**: GB-range result sets/bulk loads (not ~50 MiB), sustained over minutes,
   matching the order of magnitude of real enterprise batch/analytics jobs.
5. **Remove the Docker-Desktop-on-Windows noise floor**: run against a bare-metal/cloud Linux VM
   SQL Server instance (or at least native Linux Docker, not Hyper-V/WSL2-virtualized loopback).
6. **JMH** for any pure in-process micro-benchmarking (DDC/dtv conversion hot loops) instead of
   hand-rolled timing loops, for statistically defensible numbers if this work is ever presented
   externally or used to justify engineering investment internally.

This phase is cheap (reuses everything already built), fast to execute, and tells us definitively
whether the existing `MemoryUtil.Strategy` infrastructure already moves the needle on tail
latency/GC pressure *before* committing to the much larger NIO rewrite.

## 5. Modernization opportunities beyond buffer strategy

| Opportunity | JDK version | Why it matters here |
|---|---|---|
| `SocketChannel` + `SSLEngine` I/O | 8+ (always available; just never adopted) | The actual unlock for zero-copy read/write - see §2 |
| `java.lang.foreign` (MemorySegment/Arena) | 22+ stable | Already adopted (this effort); pairs with the above once zero-copy I/O exists |
| Real Multi-Release JAR packaging | 9+ | Today: separate `jre8`/`jre11`/.../`jre26` artifact jars. A true MR-JAR (`META-INF/versions/22/...`) ships **one** artifact that auto-selects the fastest implementation per customer JVM - no manual jar-picking required for customers to benefit from modernized paths |
| `jdk.incubator.vector` (Vector API/SIMD) | 16+ incubator | Candidate for accelerating bulk encode/decode in the `VECTOR` type path and other fixed-width bulk conversions (checksums, AES) - directly relevant to the AI/embeddings workload in §3 |
| Virtual threads | 21+ | Speculative/longer-term: relevant if customer telemetry shows connection-pool/thread-per-request scaling pain at extreme concurrency (Walmart-scale). Not yet justified by evidence - flagged for awareness, not immediate action |

## 6. Proposed phased roadmap

**Phase A - Fix the benchmark methodology (§4).** ✅ Done - see §0. Result: round-robin,
cache-controlled testing at 732 MiB under constrained heap found **no measurable benefit** from
`DIRECT`/`MEMORY_SEGMENT` over `HEAP` on the read path (`HEAP` won 4/4 rounds); GC pause time was
~1% of total runtime regardless of strategy. The sequential, same-order benchmarking done in the
earlier doc (and the first, uncontrolled attempt at this one) would have reported a false "2x
win" - a reminder that this entire line of investigation needs the same experimental rigor
going forward, not just for this one result.

**Phase A.1 - Concurrent-load test (§0.1) and isolated copy-removal test (§0.2).** ✅ Done.
Neither showed a measurable benefit either - the concurrent-load test again found the
positional confound rather than a real effect, and the isolated copy-removal POC (controlling
for everything except the one copy this whole effort targets) found literally zero GC pauses
triggered in 12 runs, for both the zero-copy and simulated-copy-overhead modes alike.

**Phase B - Re-scoped given §0.2's result.** The original plan was to treat the
`SocketChannel`+`SSLEngine` rewrite as the automatic next step once buffer-strategy tuning
alone proved insufficient. §0.2 changes that: it directly tested whether removing the copy
matters, independent of buffer strategy, and found no measurable effect at any scale tested
(single connection, concurrent connections, 732 MiB sustained, isolated-copy-only). **The
GC-pressure/tail-latency argument for this rewrite is not currently supported by evidence.**
Before committing multi-week effort to it:
- Either find a workload shape that *does* show a difference (§0.2's "what would change this
  conclusion" - sustained multi-GB/minute rates for minutes-to-hours, hundreds of concurrent
  connections, smaller production heaps than tested) and validate against it first, or
- Proceed only if justified on other grounds (API modernization, removing legacy
  `SSLSocket`/`ProxySocket` complexity, positioning for virtual threads/future JDK features) -
  explicitly *not* as a performance fix, since the evidence doesn't currently support that framing.

**Phase C - Converge with the Vector/AI workload path (§3).** ✅ Partially tested, and this is
the first genuinely strong, reproducible result in this entire investigation - see §0.3. Extend
`MemoryUtil.Strategy` coverage and (once available) zero-copy I/O specifically to bulk vector
ingestion (`SQLServerBulkCopy` + `VECTOR`) and large similarity-search result fetches is likely
still a dead end for the same reasons as §0.2 (per-vector scratch buffer, same socket-boundary
copy ceiling) - but the SIMD/boxing-removal half of this phase is not, and should be prioritized.

### 0.3 Vector/AI workload: SIMD encode benchmark (executed) - the first clear win in this investigation

Split the combined "off-heap + SIMD" hypothesis for vector bulk transfer (1536-dim float32
embeddings, the shape of a typical OpenAI-style embedding) into what it actually is: a CPU-bound,
uniform, branch-free numeric transform (`float[]` -> little-endian bytes) - architecturally
nothing like the protocol-parsing/socket-copy work that §0/§0.1/§0.2 showed no benefit from.
Also found, while checking: `VectorUtils.toBytes()` iterates `Vector.getData()`, which is
`Object[]` of boxed `Float` - a real, driver-owned (not JDBC-spec-mandated) boxing cost on every
dimension of every vector, independent of SIMD.

Pure in-process microbenchmark (no DB/network involved - this is a different kind of claim than
anything else in this doc, so it needed a different kind of test), encoding 2000 vectors x 1536
dimensions per iteration, 3 runs:

| Encoding | Mean throughput | vs baseline |
|---|---|---|
| `scalar + Object[]` (today's real `VectorUtils.toBytes()` code path) | 542.6M floats/sec | - |
| `scalar + float[]` (boxing removed only) | 2048.0M floats/sec | **3.8x** |
| `SIMD + float[]` (`jdk.incubator.vector`, boxing removed) | 4162.4M floats/sec | **7.7x** (2.0x over boxing-removal alone) |

All three produce byte-identical output (verified before timing). Consistent across 3 runs with
low run-to-run variance (unlike every DB-involving benchmark in this doc) - this is exactly what
you'd expect for a pure-CPU, no-I/O microbenchmark, and is itself a useful contrast with how
noisy §0/§0.1's results were.

**This is a real, substantial, reproducible win** - unlike every off-heap-buffer/zero-copy result
in this document. It confirms the zero-copy-theory distinction directly: §0/§0.1/§0.2 were
testing a protocol-parsing workload (where copy-elimination is a small fraction of total CPU
work, dominated by token parsing), while this is a pure-transform workload (where the "copy"
*is* essentially the entire workload) - precisely the regime where this class of technique
pays off, matching the referenced zero-copy article's own finding of 81-91% speedups for
pure-pass-through file copies.

**Recommendation:** pursue this specifically - switch `Vector`'s internal storage to primitive
`float[]` (a prior exploration branch already prototyped this) and use `jdk.incubator.vector`
for `VectorUtils`' encode/decode hot loop - as its own, narrowly-scoped, evidence-backed change,
separate from (and not blocked by) the `SocketChannel`/`SSLEngine` question in Phase B. Do *not*
bundle in an off-heap buffer-strategy change for the per-vector scratch buffer - nothing found so
far suggests that part would help, and conflating it with the SIMD win (which is real) would
risk misattributing credit if a combined benchmark is run without separating the two again.

**Phase D - Packaging modernization.** Move to a genuine Multi-Release JAR so every customer
gets the best available implementation automatically, without needing to know which of six
per-JDK jars to depend on. Document clear JVM-version/GC-collector guidance (G1 vs ZGC vs
Generational Shenandoah) alongside the driver's buffer-mode setting for enterprise customers.
This phase's value doesn't depend on §0.2's result - it's a packaging/adoption improvement
regardless.

**Phase E - Longer-term, evidence-driven.** Virtual threads/structured concurrency, further
SIMD adoption, or other modernization - only pursued if customer telemetry/feedback identifies a
concrete pain point, not speculatively.

## 7. What "done" looks like for this problem

Not "MEMORY_SEGMENT is faster than HEAP in a microbenchmark." Done looks like:
- A documented, reproducible methodology (JFR + GC logs + tail-latency histograms) that can be
  re-run whenever the driver or JDK changes, so this doesn't become a one-time experiment that
  bit-rots.
- A clear, evidence-backed statement of *which* workload shapes benefit from *which* strategy -
  as of §0.2, the honest answer for every shape tested so far is **"none measurably do"**, which
  is itself a valid, useful, and now well-evidenced conclusion, not a gap to paper over.
- A migration path customers can actually adopt: a single modern JAR (Phase D), sensible
  defaults, and clear guidance for when to opt into `DIRECT`/`MEMORY_SEGMENT` explicitly - which,
  per current evidence, should be "generally don't, `HEAP` is at least as good."
- If AI/vector workloads are pursued (Phase C), apply the same rigor before claiming a benefit -
  don't assume vector payload size alone is sufficient to change the §0.2 conclusion.

