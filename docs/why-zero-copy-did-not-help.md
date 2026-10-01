# Why Zero-Copy / Off-Heap (`MemorySegment`, `DirectByteBuffer`) Didn't Help This JDBC Driver

> Capstone summary of a multi-stage investigation. Companion docs with full detail:
> [off-heap-memory-strategy.md](./off-heap-memory-strategy.md) (Phases 1-2: building `MemoryUtil`
> and strategy selection), [jdbc-driver-modernization-roadmap.md](./jdbc-driver-modernization-roadmap.md)
> (all benchmark rounds, §0-§0.3), [socketchannel-sslengine-rewrite-scope.md](./socketchannel-sslengine-rewrite-scope.md)
> (the `SocketChannel` zero-copy POCs). This doc exists to answer one question on its own,
> without needing to read the others: **why didn't it work, in plain terms?**

## TL;DR

Across every test we ran - single-connection throughput, GC-pressure under a constrained heap,
64-connection concurrent load, and an isolated test that removed *only* the copy and nothing
else - `HEAP` buffers performed the same as or better than `DIRECT`/`MemorySegment` buffers,
every time. The reason isn't a bug or a tuning miss. It's architectural: **this driver's I/O
layer is built on `java.io.InputStream`/`OutputStream` (`Socket`/`SSLSocket`), which only accept
`byte[]`.** That forces one heap-array copy at the socket boundary no matter how the TDS packet
buffers upstream of it are backed - so making those buffers off-heap doesn't remove a copy, it
just moves where one heap array gets allocated. Separately, **every column value and bound
parameter is boxed into `java.lang.Object`** to satisfy the JDBC API contract - a cost that has
nothing to do with buffers at all and that no buffer strategy can touch.

The one place this investigation *did* find a large, reproducible win (7.7x) was a CPU-bound
numeric encode loop for vector/embedding data - a fundamentally different kind of workload from
everything else tested, explained in §5 below.

## 1. What we set out to test

The hypothesis: replacing the driver's heap `byte[]`/`ByteBuffer` allocations for TDS wire
buffers with direct or `java.lang.foreign.MemorySegment`-backed buffers would reduce GC pressure
and improve throughput/tail latency - particularly valuable for high-throughput, latency-
sensitive enterprise workloads.

To test it fairly, we first built `MemoryUtil`, a central seam that lets any TDS buffer be
allocated as `HEAP` (today's behavior, unchanged by default), `DIRECT` (`ByteBuffer.allocateDirect`),
or `MEMORY_SEGMENT` (real `java.lang.foreign.Arena`-backed native memory, JDK 22+), selected via
one system property - applied to both the write path (`TDSWriter`'s socket/staging buffers) and
the read path (`TDSPacket.payload`, after a full rewrite from `byte[]` to `ByteBuffer`).

## 2. Every test, and what it found

| # | Test | Shape | Result |
|---|---|---|---|
| 1 | Write-path throughput | Single connection, `SQLServerBulkCopy`, 100K rows x 256B | `DIRECT` +2.0%, `MEMORY_SEGMENT` +5.2% vs `HEAP` - both within `HEAP`'s own noise (StDev 3.77) |
| 2 | Read-path throughput | Single connection, full-table `SELECT`, 200K rows x 256B | `DIRECT` +2.6%, `MEMORY_SEGMENT` -2.4% vs `HEAP` - again within noise |
| 3 | GC-pressure, large stream | Single connection, 1.5M rows x 512B (732 MiB), `-Xmx384m`, round-robin + cache-cleared | **`HEAP` won 4/4 rounds.** GC pause time ~1% of runtime for all three strategies |
| 4 | Concurrent load | 64 connections, 15s sustained, indexed range queries, `-Xmx512m` | No confident difference (effect smaller than positional/run noise); `HEAP` showed the *lowest* total GC pause time of the three |
| 5 | Isolated copy-removal | Same 64-connection load, hand-rolled `SocketChannel` client, `zerocopy` vs `simulated`-copy-added, everything else identical | **No measurable difference (-1.0%). Zero GC pauses triggered in 12 runs, either mode.** |

Test 5 is the decisive one: it isolated the *one* variable this entire effort targeted - the
copy itself - from buffer strategy, connection count, and everything else, and still found
nothing. Full methodology, raw numbers, and the positional-confound traps we had to catch along
the way are in the roadmap doc §0-§0.2.

## 3. The architectural reason: there's always a copy, somewhere

`TDSChannel` (`IOBuffer.java`) is built on `Socket`/`SSLSocket`, whose `getInputStream()`/
`getOutputStream()` only accept `byte[]`. So:

- **Write path:** handing a direct/`MemorySegment` buffer to `OutputStream.write()` requires
  copying it into a `byte[]` first (`TDSWriter.writeChannelBuffer`'s non-`hasArray()` branch).
- **Read path:** filling a direct/`MemorySegment` buffer from `InputStream.read()` requires
  reading into a `byte[]` first, then copying that into the buffer
  (`TDSReader.readIntoPayload`'s non-`hasArray()` branch).

Making the TDS buffers off-heap doesn't eliminate this copy - it *adds* one, since `HEAP`
buffers can hand their backing array straight to the stream with zero extra copies, while
`DIRECT`/`MEMORY_SEGMENT` buffers now need an extra heap-array round-trip they didn't need
before. This is exactly why test 4 found `HEAP` has the *lowest* GC pause time: the "off-heap"
strategies are, perversely, allocating *more* heap garbage per packet under the current
architecture, not less.

A true zero-copy path would require replacing `Socket`/`SSLSocket` with `SocketChannel` +
`SSLEngine` (the pattern Netty/Jetty/Tomcat NIO use), which accept `ByteBuffer`s directly with no
forced array. We proved this is *mechanically possible* - a standalone POC
(`SocketChannelZeroCopyPoc`) hand-built the TDS PRELOGIN/LOGIN7/query handshake over a raw
`SocketChannel` and successfully round-tripped real queries against a live SQL Server with zero
`byte[]` copies, for both `DirectByteBuffer` and real `MemorySegment` buffers. But building that
mechanic doesn't mean it pays off - see test 5, which specifically isolated and measured the
value of removing this exact copy, and found none.

## 4. The separate reason: boxing

Independent of buffers entirely: every column value and bound parameter is stored as
`java.lang.Object` (`DTVImpl.value`), because `ResultSet.getObject()`/`PreparedStatement.setObject()`
are part of the JDBC contract. A boxed `Integer`/`Long`/`Double` is always an ordinary Java heap
object - there's no off-heap representation for it in the current JDK (Project Valhalla's value
types may eventually change this, but that's not available yet). `ResultSet.getInt()`/`getLong()`/
etc. box internally even though their public signature returns a primitive, because they share
the same `Object`-returning conversion pipeline as `getObject()`.

This cost is real but is capped in practice by JLS 5.1.7's mandatory autobox caching (small
`Integer`/`Long`/`Short`/`Boolean` values are often non-allocating); `double`/`float` always
allocate. It's unrelated to buffer strategy and would persist even with a perfect zero-copy I/O
layer - it's a ceiling on *this* kind of GC-pressure reduction, not something buffers (or
`SocketChannel`) can ever fix.

## 5. The one place it *did* work, and why that's not a contradiction

A separate test - encoding 1536-dimension float32 vectors (the shape of a typical embedding) to
TDS wire bytes - found a **real, reproducible 7.7x speedup**: removing `Vector`'s current
boxed-`Float[]` storage gave 3.8x, and adding `jdk.incubator.vector` (SIMD) on top gave another
2.0x. Full numbers in the roadmap doc §0.3.

This doesn't contradict anything above - it's a different *kind* of workload. Tests 1-5 were all
protocol-parsing work: TDS token framing, type dispatch, value conversion - CPU work that has to
happen regardless of where bytes live, and where the "copy" is a small fraction of total cost.
The vector encode loop is a uniform, branch-free numeric transform where the copy/transform *is*
essentially the entire workload - precisely the regime classic zero-copy techniques (`mmap`,
`sendfile`, and by extension SIMD-accelerated bulk transforms) are proven to help, as described
in the industry literature on OS-level zero-copy (disk-to-socket file transfer via `sendfile()`
measures 81-91% speedups for exactly this reason - the transfer *is* the work, so eliminating
copies eliminates most of the work). Protocol parsing never fit that shape; numeric vector
encoding does.

## 6. Bottom line

| Question | Answer |
|---|---|
| Does `DIRECT`/`MEMORY_SEGMENT` help TDS buffer allocation under the current architecture? | No, measurably, across 5 independent tests |
| Would removing the socket-boundary copy entirely (`SocketChannel`/`SSLEngine`) help? | Tested in isolation - also no, at every scale tried so far |
| Is there a cheaper lever left to pull for GC pressure on the protocol-parsing path? | Not identified yet - the boxing ceiling (§4) is the next-largest cost, but is JDBC-spec-constrained and high-risk to fix |
| Is off-heap/SIMD worth anything in this driver at all? | Yes - for CPU-bound numeric transforms (vector encoding), not for protocol I/O |

**Recommendation:** don't pursue the `SocketChannel`/`SSLEngine` `TDSChannel` rewrite as a
performance fix - the evidence doesn't support it. If pursued at all, justify it on other grounds
(API modernization, removing `SSLSocket`/`ProxySocket` legacy complexity). Do pursue the
vector/SIMD encoding improvement (§5) - it's a real, isolated, low-risk win, unrelated to the
buffer-strategy question.
