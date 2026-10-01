# Scoping: `SocketChannel` + `SSLEngine` Rewrite of `TDSChannel`

> Scoping doc for Phase B of [jdbc-driver-modernization-roadmap.md](./jdbc-driver-modernization-roadmap.md).
> Goal: eliminate the mandatory heap-array copy at the socket boundary (`TDSWriter.writeChannelBuffer`
> / `TDSReader.readIntoPayload`'s non-`hasArray()` branches) that Phase A (§0 of that doc) showed
> is the real reason buffer-strategy selection alone hasn't produced a reliable win.

## 1. Current architecture

`TDSChannel` ([IOBuffer.java](/c:/Users/divangsharma/workspace-memory-segment/mssql-jdbc-utils/src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java))
is built on `java.net.Socket`/`SSLSocket` and their `InputStream`/`OutputStream`, which only
accept `byte[]`. Three things make this more involved than "just swap `Socket` for
`SocketChannel`":

### 1.1 `SocketChannel` is already used - but only for connection racing

`connectHelper()` already opens one `SocketChannel` per candidate IP (multi-subnet failover /
parallel-connection-attempt support) and uses a `Selector` to pick whichever connects first.
But the moment a winner is chosen:

```java
selectedChannel.configureBlocking(true);
selectedSocket = selectedChannel.socket();   // <-- converted back to a plain Socket here
```

...it's immediately converted back to the `Socket` adapter, and every byte of actual TDS traffic
for the rest of the connection's life goes through `Socket`/`SSLSocket` streams. **This is good
news for Phase B**: `SocketChannel` doesn't need to be introduced as a new concept - it's already
chosen at connect time and just needs to *not be discarded*.

### 1.2 Two distinct SSL/TLS negotiation modes

- **TDS 8.0 / "strict" encryption** (SQL Server 2022+, `isTDS8 == true`): the `SSLSocket` is
  layered directly over the real socket (`createSocket(channelSocket, host, port, true)`) with
  ALPN negotiating the `tds/8.0` protocol - this is ordinary, standard TLS, exactly like HTTPS.
  **This is the easy case** - a completely standard `SSLEngine`-over-`SocketChannel` pattern,
  identical to how Netty/Jetty/Tomcat NIO already do TLS.
- **Legacy TDS 7.x** (`isTDS8 == false`): the `SSLSocket` is layered over a custom `ProxySocket`
  ([IOBuffer.java:1427](/c:/Users/divangsharma/workspace-memory-segment/mssql-jdbc-utils/src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java)),
  whose `ProxyInputStream`/`ProxyOutputStream` wrap the SSL handshake bytes **inside TDS packet
  framing** (TDS's own historical quirk - the handshake rides inside `PKT_PRELOGIN`-style TDS
  messages, not as a raw TLS stream). Only *after* the handshake completes does
  `proxySocket.setStreams(...)` rewire things to plain pass-through, and `TDSChannel` switches to
  the `SSLSocket`'s own streams directly. **This is the hard case** - it needs a custom adapter
  that feeds `SSLEngine.wrap()`/`unwrap()`'s network-side bytes through TDS packet framing during
  the handshake only, then stops doing so once the handshake completes.
- **Unencrypted** (`encrypt=false`): no SSL layer at all - just the raw socket. **Easiest case,
  useful as the first proof of concept.**

### 1.3 Supporting behaviors that must be preserved

- **Liveness polling** (`networkSocketStillConnected()`): briefly sets `SO_TIMEOUT=1` and
  polls - `SocketChannel.socket().setSoTimeout(...)` still affects blocking reads done via the
  channel directly, so this should port with minimal change.
- **Idle Connection Resiliency** reconnect flow, and the `close(Selector)`/`close(SocketChannel)`
  cleanup helpers already present for the connection-racing code - these patterns already exist
  in this file and can largely be reused/extended rather than invented from scratch.
- **Attention/cancel signaling**, `socketTimeout` semantics, MARS packet-header-based
  multiplexing (not socket-level - no change needed there, it rides in the TDS packet header
  regardless of transport).

## 2. Target architecture - and the key risk-reducing decision

**Stay in blocking mode.** `SocketChannel.read(ByteBuffer)`/`write(ByteBuffer)` work as ordinary
blocking calls when `configureBlocking(true)` (already the case today, see §1.1) - they are not
inherently tied to non-blocking/async/event-loop usage. This means Phase B is **not** "rewrite
the driver to be async" - it's "keep using the `SocketChannel` that's already chosen today, and
give it `ByteBuffer`s directly instead of converting to `Socket`/streams." The driver's existing
synchronous, one-thread-per-connection execution model is unaffected. This substantially reduces
the risk/size of this effort relative to a full non-blocking NIO rewrite, and is the
recommended approach.

With that decided:

- `TDSChannel.read(byte[], ...)`/`write(byte[], ...)` gain `ByteBuffer`-native siblings that call
  `socketChannel.read(buffer)`/`write(buffer)` directly - for the unencrypted case, zero
  intermediate copy, full stop.
- For TLS, `SSLEngine` replaces `SSLSocket`, driven by a manual wrap/unwrap loop (the standard,
  well-documented `SSLEngine` usage pattern - see `SSLEngine`'s own Javadoc, or reference
  implementations in Tomcat's NIO connector / Netty's `SslHandler`). `SSLEngine` naturally
  operates on `ByteBuffer`s on both the application and network sides, so once routed through it,
  `MemoryUtil`'s `newChannelBuffer`-backed buffers flow straight through with no forced heap copy
  either.
- `TDSWriter.writeChannelBuffer`/`TDSReader.readIntoPayload` (added in Phase 1/§8.1 of the
  off-heap doc) - the exact methods whose non-`hasArray()` fallback copy Phase A's benchmark
  blamed - become unnecessary for the ported path(s); the `hasArray()` branch stops being "the
  fast path" and becomes simply "the only path," for both heap and off-heap buffers alike.

## 3. Recommended staged migration (three tracks, in this order)

1. **Unencrypted (`encrypt=false`)** - simplest possible case, no `SSLEngine` at all. Good first
   deliverable: proves the `SocketChannel`-direct-read/write plumbing and measures whether
   removing the copy actually produces the throughput/GC-pressure improvement Phase A couldn't
   find with buffer-strategy tuning alone. If this doesn't show a real improvement either, that's
   important, cheap-to-get evidence before investing in the harder tracks.
2. **TDS 8.0 strict encryption** - standard `SSLEngine`-over-`SocketChannel`, well-trodden pattern.
3. **Legacy TDS 7.x SSL-in-TDS-packets** - the genuinely novel part: an adapter that feeds
   `SSLEngine`'s handshake-phase network bytes through TDS packet framing, switching to direct
   pass-through once the handshake completes - mirroring what `ProxySocket`/`setStreams()` already
   do today, just adapted to `SSLEngine`'s wrap/unwrap buffers instead of `SSLSocket`'s streams.

All three should land behind a feature flag (e.g. extending the existing
`mssql.jdbc.bufferMode`-style system property convention) with the current `Socket`/`SSLSocket`
path kept fully intact as the default and fallback - this is far too central to the driver to
flip by default without extensive soak testing.

## 4. Risk register

| Risk | Mitigation |
|---|---|
| `SSLEngine` handshake loop bugs (wrap/unwrap state machine is fiddly to get exactly right) | Start with track 1 (no `SSLEngine` at all) to validate the `SocketChannel` plumbing in isolation first; borrow/adapt a reference `SSLEngine` loop implementation rather than writing one from scratch |
| Legacy TDS 7.x SSL-in-packets adapter (§1.2, track 3) is genuinely novel - no off-the-shelf reference exists | Land tracks 1-2 first and get real production soak time before attempting track 3; keep `ProxySocket`'s existing logic as the design reference since the framing rules are already correctly implemented there |
| Regressions in idle-connection-resiliency, cancellation, or MARS behavior | These are header/protocol-level behaviors, not socket-API-level - should be largely unaffected, but need explicit regression tests per §5 given how central this code path is |
| Feature-flag complexity (two parallel I/O implementations to maintain) | Time-box: once a track is validated and defaults are flipped, remove the old path for that track rather than maintaining both indefinitely |
| "We do all this work and it still doesn't show a win" | Track 1 (unencrypted, simplest) is specifically sequenced first as a cheap way to find this out before investing in tracks 2-3 |

## 5. Validation plan (mirrors what worked in Phase A)

- Reuse/extend the `CorrectnessTest` harness from the off-heap effort (scalar types, NULL
  handling, GUID, `MAX`-type PLP streaming, multi-packet boundaries via small `packetSize`) -
  must pass identically to today's `Socket`-based path before any default-behavior change.
- Reuse the round-robin, cache-cleared benchmark methodology from Phase A (§0 of the roadmap
  doc) - **not** a naive sequential comparison, given how badly that misled us the first time.
- Add the GC-log/JFR instrumentation from Phase A to specifically check whether removing the
  copy (not just changing buffer backing) finally produces a measurable GC-pressure/tail-latency
  improvement.

## 6. Suggested first concrete deliverable

A small, isolated proof-of-concept - **not** a change to `TDSChannel` itself yet - that opens a
raw `SocketChannel` to the live SQL Server container, manually performs the TDS pre-login
handshake plus an unencrypted login, and reads/writes a few TDS packets directly via
`MemoryUtil`-backed `ByteBuffer`s with no `byte[]` copy anywhere, to prove the zero-copy
mechanics end-to-end before touching the real, security-and-correctness-critical `TDSChannel`
code. This is cheap, low-risk, and directly tests track 1 from §3.

## 7. POC results (executed)

Built and run against the live SQL Server container: a standalone program
(`SocketChannelZeroCopyPoc.java`, outside the repo - does not touch `TDSChannel` or any
production code) that opens a raw blocking `SocketChannel`, hand-constructs the TDS PRELOGIN,
LOGIN7, and SQL_BATCH packets per MS-TDS, and performs every read/write directly against
off-heap buffers (`ByteBuffer.allocateDirect` and, separately, real
`java.lang.foreign.MemorySegment`-backed buffers via `Arena.ofAuto()`) via
`SocketChannel.read(ByteBuffer)`/`write(ByteBuffer)` - no `byte[]` anywhere in the I/O path.

**Approach note**: rather than hand-rolling TLS, the POC first asked the server whether a fully
unencrypted session (`ENCRYPT_NOT_SUP`) was acceptable - this SQL Server 2022 container allowed
it, which made Track 1 (unencrypted) fully testable without needing `SSLEngine` at all yet. A
`TdsCaptureProxy` was used first to observe a real driver connection's byte-level conversation,
which is what revealed (a) this server actually negotiates login-only TLS by default for
`encrypt=false` connections (confirming the Track 2/3 distinction in §1.2 is real, not
theoretical) and (b) the exact PRELOGIN/LOGIN7/SQL_BATCH structure to replicate by hand.

**Result: full success**, both as `DIRECT` and `MEMORY_SEGMENT`-backed buffers, reliably
repeatable (3/3 runs):

```
poc.mode=direct|memory_segment
Writing PRELOGIN request (47 bytes), direct=true hasArray=false
Read PRELOGIN response (43 bytes), direct=true hasArray=false
Server granted encryption mode: NOT_SUP (fully unencrypted)
Writing LOGIN7 request (254 bytes)
Read LOGIN7 response (401 bytes)
LOGINACK token present in response: true
Writing SQL_BATCH request (56 bytes)
Read SQL_BATCH response (39 bytes)
ROW token present: true, DONE token present: true
RESULT: FULL zero-copy round trip (PRELOGIN -> LOGIN7 -> SQL_BATCH -> result) succeeded
end-to-end via SocketChannel + off-heap ByteBuffers, with zero byte[] copies.
```

Two real bugs were hit and fixed while building this, both worth recording since they'll recur
in the real `TDSChannel` port:

1. **Byte order is not uniform across TDS.** The 8-byte packet header's length/SPID fields and
   PRELOGIN's option table (token/offset/length triplets) are big-endian (network byte order);
   essentially everything else (LOGIN7's fields, SQL_BATCH's ALL_HEADERS, string data) is
   little-endian. Getting this wrong produces a connection the server silently closes, not a
   clear error - worth a deliberate, well-commented helper rather than relying on a single
   buffer-wide `ByteOrder` setting, exactly the trap hit here.
2. **`SQL_BATCH` requires an `ALL_HEADERS` block** (transaction descriptor + outstanding request
   count, TDS 7.2+) before the SQL text - omitting it (as an initial attempt did) is also a
   silent-failure mode, not an explicit protocol error.

**What this validates:** the core mechanic proposed in §2 - keeping the already-chosen
`SocketChannel` in blocking mode and reading/writing `ByteBuffer`s (including genuine
`MemorySegment`-backed ones) directly, with zero `byte[]` copy - works correctly end-to-end
against a real SQL Server instance, for both off-heap backings `MemoryUtil.Strategy` supports.
This substantially de-risks §6's recommended next step: a real `TDSChannel` port (still a larger,
separately-scoped effort - this POC deliberately hand-rolled a minimal protocol subset rather
than reusing the driver's existing, far more complete and correct LOGIN7/PRELOGIN construction
logic, which the real port absolutely should reuse rather than reimplement).

**What this does *not* yet validate:** whether removing this copy actually produces a measurable
throughput/GC-pressure improvement, and nothing about the `SSLEngine` tracks (2-3) - this was
Track 1 only, by design.

**Update - this was tested next, isolated from buffer strategy entirely:** see
[jdbc-driver-modernization-roadmap.md](./jdbc-driver-modernization-roadmap.md) §0.2.
`SocketChannelCopyComparisonPoc` extended this POC to run under 64-thread concurrent load,
comparing a true zero-copy mode against an identical mode with the copy artificially
reintroduced. Result: no measurable throughput difference, and literally zero GC pauses
triggered in either mode across 12 rounds. This is a materially important result for §6's
recommendation - see the roadmap doc's revised Phase B framing.

