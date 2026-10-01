# Off-Heap Memory Strategy — Design Notes

> Personal/working design doc for the Heap vs. Direct vs. MemorySegment buffer-strategy
> experiment in the JDBC driver's TDS I/O layer. Captures what was built, why it's scoped the
> way it is, and what's intentionally deferred.

## 1. Motivation

The driver's TDS read/write path has always allocated its buffers on the Java heap
(`ByteBuffer.allocate(...)`, `new byte[...]`), scattered across `IOBuffer.java` and several
supporting classes. Before trying alternative allocation strategies (direct `ByteBuffer`s,
JDK 22+ `java.lang.foreign.MemorySegment`), every allocation call site needed to be centralized
behind one seam, so a strategy can be swapped without re-touching dozens of files each time.

## 2. `MemoryUtil` — the central seam

[`MemoryUtil`](/c:/Users/divangsharma/workspace-memory-segment/mssql-jdbc-utils/src/main/java/com/microsoft/sqlserver/jdbc/MemoryUtil.java)
is a package-private, static-only utility with four allocation APIs:

| Method | Backing | Used for |
|---|---|---|
| `newArray(int)` | always heap `byte[]` | every `byte[]` scratch/payload allocation |
| `newCharArray(int)` | always heap `char[]` | char-array scratch buffers |
| `newByteBuffer(int, ByteOrder)` | always heap `ByteBuffer` | small, short-lived encode/decode scratch buffers |
| `newChannelBuffer(int, ByteOrder)` | **strategy-aware** | the few large, per-connection TDS channel buffers |

### Why only `newChannelBuffer` is strategy-aware

Direct/MemorySegment allocation has real per-call overhead (a syscall-adjacent native
allocation, plus — for `MemorySegment` — an `Arena` and bounds-check machinery) that heap
allocation + JIT escape analysis does not. Applying it to every tiny scratch array (GUID bytes,
decimal encoding, date/time encode buffers — allocated millions of times per large result set)
would very likely be a net loss. Earlier prototyping of this idea on a sibling branch reached
the same conclusion. So the strategy switch is deliberately scoped to just the handful of
buffers that are large (packet-sized, e.g. 4KB-32KB) and live for the lifetime of a connection
or TDS message:

- `TDSWriter.socketBuffer`
- `TDSWriter.stagingBuffer`

`TDSWriter.logBuffer` and `cachedTVPHeaders` stay heap-only on purpose — see §5.

## 3. Buffer strategy selection

```java
enum MemoryUtil.Strategy { HEAP, DIRECT, MEMORY_SEGMENT }
```

Selected once per process from a system property, mirroring the convention used in earlier
prototyping of this idea:

```
-Dmssql.jdbc.bufferMode=heap            (default)
-Dmssql.jdbc.bufferMode=direct
-Dmssql.jdbc.bufferMode=memory_segment
```

The legacy boolean spelling `-Dmssql.jdbc.useMemorySegment=true|false` is also accepted.
**Default is `HEAP` — zero behavior change for anyone who doesn't opt in.**

## 4. JDK-version gating for `MemorySegment`

This repository's `pom.xml` compiles the driver across `jre8` → `jre26` profiles. The
`java.lang.foreign` API only exists from JDK 22 onward, so it cannot be referenced from the
main `src/main/java` source set without breaking the `jre8`/`jre11`/`jre17`/`jre21` builds.

The fix: a second source root, compiled only when the build profile's JDK can support it.

```
src/main/java22/com/microsoft/sqlserver/jdbc/MemorySegmentBufferAllocator.java
```

- Compiled with a **second** `maven-compiler-plugin` execution (`--release 22`), added only to
  the `jre25` and `jre26` profiles in `pom.xml`.
- `MemoryUtil` never imports this class. It loads it **reflectively by name**
  (`Class.forName("...MemorySegmentBufferAllocator")`) the first time `MEMORY_SEGMENT` is
  requested, caches the resolved `Method`, and falls back to `DIRECT` if the class can't be
  loaded (`ReflectiveOperationException` / `LinkageError` — i.e. running on JDK < 22, or a
  build profile that never compiled it).
- Each allocation uses `Arena.ofAuto()` rather than `Arena.ofShared()`/`ofConfined()`. An
  automatic arena ties native memory lifetime to GC reachability of the segment, exactly like a
  JDK direct buffer already behaves — this avoids needing a new explicit
  `close()`/cleanup lifecycle API on `MemoryUtil` for what are, today, only a handful of
  long-lived buffers. Trade-off: reclamation is GC-driven, not deterministic.

Validated end-to-end on this machine (JDK 17 and JDK 25 installed):

| Scenario | JDK | Result |
|---|---|---|
| `bufferMode=heap` | 17 or 25 | `HeapByteBuffer`, `hasArray=true` |
| `bufferMode=direct` | 17 or 25 | `DirectByteBuffer`, `hasArray=false` |
| `bufferMode=memory_segment` | 25 (jre25 build, allocator class present) | `DirectByteBuffer` (MemorySegment-backed), `hasArray=false` |
| `bufferMode=memory_segment` | 17 (jre17 build, allocator class **absent**) | gracefully falls back to `DirectByteBuffer` - no crash |
| *(no property set)* | any | `HeapByteBuffer` - confirms default behavior is unchanged |

## 5. A real correctness trap that had to be fixed: `.array()`

Before wiring `socketBuffer`/`stagingBuffer` to `newChannelBuffer`, the write path called
`.array()` directly on them (`tdsChannel.write(socketBuffer.array(), ...)`,
`cachedTVPHeaders.put(stagingBuffer.array(), ...)`). `ByteBuffer.array()` throws
`UnsupportedOperationException` for any direct or MemorySegment-backed buffer. Selecting
`DIRECT`/`MEMORY_SEGMENT` would have crashed the connection on the very first packet flush.

Fixes applied:
- Added `TDSWriter.writeChannelBuffer(...)`: uses `.array()` directly when the buffer
  `hasArray()` (heap — no copy, same as before), otherwise copies into a short-lived heap
  array first (unavoidable - see §6) before handing it to `TDSChannel.write(byte[], int, int)`.
- Replaced the `stagingBuffer.array()` read in the TVP-header-caching path with a
  buffer-agnostic relative bulk `put(ByteBuffer)`.
- Left `logBuffer` (diagnostic packet-tracing dump, only active when packet logging is
  enabled) and `cachedTVPHeaders` (rare TVP + server-cursor combination) on
  `MemoryUtil.newByteBuffer` - always heap - so their existing `.array()` use stays valid and
  low-risk, since off-heap wouldn't meaningfully help either of these rare/non-hot paths anyway.

## 6. Known ceiling: `OutputStream`/`InputStream`-based channel I/O

`TDSChannel` is built on `java.io.OutputStream`/`InputStream` (required for layering
`SSLSocket`), not `java.nio.channels.SocketChannel`. `OutputStream.write()` has no `ByteBuffer`
overload, so handing a direct/MemorySegment buffer to the socket still requires **one** copy
into a heap `byte[]` at the flush boundary (see `writeChannelBuffer` in §5). This is why earlier
prototyping of this idea found little-to-no write-side throughput benefit from off-heap
buffers — the final hop to the socket is still heap-copy-based either way. The expected payoff
of this effort is primarily in *reduced long-lived heap residency / GC pressure* from the
per-connection staging buffers, not in eliminating every copy. A true zero-copy write path
would require moving `TDSChannel` to `SocketChannel` + `SSLEngine`-based I/O, which is a much
larger, separate effort.

## 7. The other ceiling: autoboxing (not a buffer problem at all)

Separately documented in `MemoryUtil`'s class Javadoc: every column/parameter value is stored
as `java.lang.Object` in `DTVImpl.value`, to satisfy the JDBC `getObject()`/`setObject()`
contract. Boxed `Integer`/`Long`/`Double`/etc. are always ordinary Java heap objects — no buffer
strategy can change that. This caps how much *total* GC-pressure reduction is achievable for
narrow, column-at-a-time `ResultSet`/`PreparedStatement` access patterns, independent of which
`MemoryUtil.Strategy` is active. (JLS 5.1.7 autobox caching already makes most small
int/long/short/boolean values non-allocating; `double`/`float` always allocate.) A real fix
would require duplicating `ServerDTVImpl.getValue()`'s Always-Encrypted/PLP-streaming
conversion pipeline into primitive-returning variants — judged too high-risk to do as a
"quick" optimization; out of scope here.

## 8. What's covered vs. deferred

### Done (behavior-preserving, `HEAP` still the default everywhere)

| Area | File(s) | Strategy-aware? |
|---|---|---|
| TDS packet framing (writer) | `IOBuffer.java` (`TDSWriter`) | `socketBuffer`/`stagingBuffer` - yes; `logBuffer` - no (by design, §5) |
| TDS packet framing (reader) | `IOBuffer.java` (`TDSPacket`, `TDSReader`) | Yes - `payload` (see §8.1); `header` stays `byte[]` (tiny, no benefit) |
| Value (de)serialization | `DDC.java`, `dtv.java`, `Util.java` | No - small scratch allocations only |
| Bulk copy row encoding | `SQLServerBulkCopy.java` | No - small per-row scratch allocations only |
| MAX-type streaming | `PLPInputStream.java` | No - chunk accumulation arrays |

### 8.1 Read path: `TDSPacket.payload` is now strategy-aware

Unlike the earlier prototype's ~955-line diff, this rewrite turned out to be narrow: every
`payload[i]`-style index access in `TDSReader` was already funneled through a small number of
`Util.read*(byte[], offset)` helper calls, so only **~13 access points** needed changing (plus
the `TDSPacket` constructor and the socket-fill loop):

- `TDSPacket.payload`: `byte[]` → `ByteBuffer`, allocated via `MemoryUtil.newChannelBuffer`
  (one buffer *per packet*, not reused/shared - packets form a linked chain via `next` that may
  all be referenced at once through marks/streaming, so each needs to own its memory for as
  long as something references it; mirrors the earlier prototype's same per-packet-arena choice).
- All fixed-width reads (`readShort`/`readUnsignedShort`/`readInt`/`readLong`/single-byte peeks)
  now call `ByteBuffer`'s absolute `getShort(int)`/`getInt(int)`/`getLong(int)`/`get(int)`
  directly, since `payload` is always allocated `LITTLE_ENDIAN` (matching the TDS wire format
  for every field except one - see below). These are all **absolute** accessors, so `payloadOffset`
  (already independently tracked) remains the single source of truth; the buffer's own
  `position()` is never relied upon for these.
- The one big-endian payload field (`readIntBigEndian()`, used once per connection to read the
  TDS version during login) can't use `getInt()` directly (the buffer is LE), so it's assembled
  manually from four order-independent single-byte `.get(offset+i)` reads instead - exactly
  mirroring `Util.readIntBigEndian`'s own byte-order arithmetic.
- The one **bulk** accessor (`readBytes(byte[], offset, length)` - the hot path for strings,
  binary, decimal values) and the socket-fill loop both need **relative** bulk
  `get(byte[],off,len)`/`put(byte[],off,len)`, since there's no Java-8-compatible absolute bulk
  overload (those were added in Java 13). Both explicitly set `((Buffer) payload).position(...)`
  immediately before the call, so nothing depends on position being preserved across calls -
  safe in this single-threaded reader.
- New `TDSReader.readIntoPayload(...)` helper mirrors `TDSWriter.writeChannelBuffer(...)`: zero
  extra copy when `payload.hasArray()` (the default `HEAP` strategy - reads straight from the
  socket into the array, exactly as before), otherwise reads into a short-lived heap scratch
  array first and bulk-puts it into the off-heap buffer (same unavoidable-copy-at-the-socket-
  boundary reality as the write side, see §6 - confirmed to apply symmetrically on read, §10.1).

**Correctness validation** (this is the core read path for every query result, so this mattered
more than the benchmark): a standalone test harness exercised every touched accessor - all
scalar types, `NULL`/NBC-ROW handling, `UNIQUEIDENTIFIER`, a 600,000-character `NVARCHAR(MAX)` +
600,000-byte `VARBINARY(MAX)` (CRC32-verified, hundreds of packets via `PLPInputStream`), and a
20,000-row batch with a per-row checksum - all run with `packetSize=512` (deliberately tiny, to
force nearly every value across a packet boundary and exercise the `readWrappedBytes` fallback
path, not just the single-packet fast path). **All checks passed identically under `HEAP`,
`DIRECT`, and `MEMORY_SEGMENT`** (JDK 25, real `java.lang.foreign` allocator, not just fallback).

## 9. How to try it

```bash
# Default - identical to pre-existing behavior
mvn test

# Opt into direct buffers for both the writer's socket/staging buffers and reader packets
mvn test -Dmssql.jdbc.bufferMode=direct

# Opt into MemorySegment (falls back to direct automatically on JDK < 22
# or on build profiles that didn't compile src/main/java22)
mvn test -Dmssql.jdbc.bufferMode=memory_segment -Pjre25
```

## 10. Benchmark results (measured in this environment)

**Setup (both benchmarks):** SQL Server 2022 (`mcr.microsoft.com/mssql/server:2022-latest`) in a
local Docker container, `encrypt=true;trustServerCertificate=true`, `localhost:1433`, JDK 25 /
`jre25` build (real `java.lang.foreign` allocator for `MEMORY_SEGMENT`, not the fallback). 2
rounds x 5 iterations per strategy; first iteration of each round discarded (connection setup +
JIT warmup).

### 10.1 Write path - `SQLServerBulkCopy.writeToServer(...)`, 100,000 rows x 256-byte `VARBINARY` (24.41 MiB/run)

| Strategy | Mean MiB/s | Median | StDev | vs HEAP |
|---|---|---|---|---|
| HEAP (baseline) | 48.39 | 48.10 | 3.77 | - |
| DIRECT | 49.38 | 49.60 | 2.21 | +2.0% |
| MEMORY_SEGMENT | 50.91 | 50.41 | 3.25 | +5.2% |

### 10.2 Read path - full-table `SELECT`, 200,000 rows x 256-byte `VARBINARY` (48.83 MiB/run)

| Strategy | Mean MiB/s | Median | StDev | vs HEAP |
|---|---|---|---|---|
| HEAP (baseline) | 51.20 | 51.11 | 0.92 | - |
| DIRECT | 52.55 | 51.96 | 4.73 | +2.6% |
| MEMORY_SEGMENT | 49.96 | 50.43 | 2.28 | -2.4% |

**Interpretation: no clear win on either side, in this environment, at this payload size.** All
three strategies land within a few percent of each other, and the gaps are smaller than (write)
or comparable to (read) each strategy's own run-to-run noise. `MEMORY_SEGMENT` trending
*slower* than `HEAP` on the read benchmark is itself informative, not just noise: it confirms
the architectural ceiling from §6 applies **symmetrically on the read side too** - just like
`TDSChannel.write()`, `TDSChannel.read()` is `InputStream`-based with no `ByteBuffer` overload,
so filling an off-heap `payload` costs one extra heap-scratch-array copy per packet
(`readIntoPayload`'s non-`hasArray()` branch) that the `HEAP` strategy simply doesn't pay. Unlike
the write side, this isn't just "no win" - it's a real extra cost that has to be earned back by
some other benefit (e.g. reduced GC pressure from not retaining many heap-resident packet
buffers) before it's worth it.

This result does **not** reproduce the earlier prototype's own claimed "large improvement" for
the read side. Plausible reasons, most likely in combination: (a) the payload here (~49 MiB) may
be too small for GC-pressure reduction to show up as wall-clock time - a win from *avoiding heap
growth/collection pauses* would need a much larger result set (hundreds of MB to GB) and/or
constrained heap size to manifest; (b) Docker-Desktop-on-Windows loopback networking
(WSL2/Hyper-V virtualization) adds its own jitter that likely dominates at this scale - note
`DIRECT`'s read-side StDev (4.73) is over 5x `HEAP`'s (0.92); (c) the earlier prototype's own
numbers were never independently re-validated against this codebase before now. Caveats: only 2
rounds, one payload shape, one (containerized) SQL Server.

## 11. Suggested next steps

1. ~~Benchmark `HEAP` vs `DIRECT` vs `MEMORY_SEGMENT` for the writer buffers~~ - done, §10.1. No
   clear write-side win in this environment.
2. ~~Scope out and implement the `TDSPacket.payload`/`TDSReader` read-path rewrite~~ - done, §8.1,
   correctness-validated. Benchmarked - §10.2. No clear read-side win in this environment either,
   contrary to the earlier prototype's claim.
3. If this is still worth pursuing, the next experiment should specifically target what §10.2's
   interpretation flags: a **much larger** result set (hundreds of MB+, enough rows/packets that
   GC pause time becomes a measurable fraction of total time), ideally with `-Xmx` constrained
   enough to force collections during the fetch, and ideally on a non-containerized SQL Server
   to remove the Docker-Desktop network jitter from the measurement. Without that signal, the
   `MemoryUtil.Strategy` infrastructure is correctness-tested and ready, but `HEAP` (the
   existing default) looks like the right choice for this driver's typical workloads.
4. Consider whether an SSL-aware automatic downgrade (e.g. `MEMORY_SEGMENT` → `DIRECT` when a
   TLS session is active) is still warranted - given neither off-heap strategy showed a
   consistent edge over the other here, this is likely moot unless (3) changes the picture.
   This is a larger, higher-risk change (duplicating `.array()`/index-based accessors across
   `TDSReader`) and should be its own separately-reviewed effort.
3. If pursuing (2), benchmark large `ResultSet` fetches (wide rows, MAX-length columns, and/or
   `encrypt=true`) the same way this section did, before and after, to get a real before/after
   comparison rather than relying on the earlier prototype's numbers.
4. Consider whether an SSL-aware automatic downgrade (e.g. `MEMORY_SEGMENT` → `DIRECT` when a
   TLS session is active) is still warranted - the earlier prototype added this based on its
   own benchmarking; given §10 found no write-side benefit from `MEMORY_SEGMENT` over `DIRECT`
   either, it may be moot for the write path, but could still matter once the read path is tackled.

