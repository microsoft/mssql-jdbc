# Proposal: Modernizing TDS Buffer Serialization Using Java MemorySegment (FFM API)

## Executive Summary

This proposal outlines the strategy and performance benefits of adopting Java's **Foreign Function & Memory (FFM) API** (`java.lang.foreign.MemorySegment`, finalized in Java 22 via JEP 454) within the Microsoft JDBC Driver for SQL Server (`mssql-jdbc`).

We demonstrate that transitioning high-volume packet serialization—beginning with **Bulk Copy (`SQLServerBulkCopy`)**—from on-heap `byte[]`/`ByteBuffer` to off-heap `MemorySegment` with deterministic `Arena` management delivers:
- **~20.5% improvement in write throughput** across live Azure SQL DB connections.
- **~94% reduction in Garbage Collection (GC) pauses** during large data ingest operations.
- **Architectural path for Multi-Release JAR (MR-JAR)** compatibility, preserving full support for legacy JRE runtimes (`jre8` through `jre21`) while unlocking modern hardware acceleration on JDK 22+.

---

## 1. Problem Statement

### 1.1 Inefficiencies in Current On-Heap Buffer Management
In the current driver architecture (centered around [IOBuffer.java](../../src/main/java/com/microsoft/sqlserver/jdbc/IOBuffer.java)):
1. **Manual Bit-Shifting Overhead**:
   SQL Server's TDS wire protocol is predominantly little-endian. `IOBuffer.java` reconstructs or writes primitive types (`short`, `int`, `long`, `float`, `double`) via manual bitwise masks and shifts:
   ```java
   // Current pattern in IOBuffer / Util
   buffer[offset++] = (byte) (value & 0xFF);
   buffer[offset++] = (byte) ((value >> 8) & 0xFF);
   buffer[offset++] = (byte) ((value >> 16) & 0xFF);
   buffer[offset++] = (byte) ((value >> 24) & 0xFF);
   ```
   This prevents optimal JIT vectorization and requires manual bounds tracking.

2. **Severe GC Churn Under High-Throughput Loads**:
   When streaming millions of rows via `SQLServerBulkCopy`, high numbers of intermediate buffers are created and collected on the JVM heap (Eden / Young Gen). Under heavy workloads, this triggers frequent stop-the-world GC pauses or causes objects to leak into Old Gen.

3. **Limitations of `java.nio.ByteBuffer`**:
   `ByteBuffer` relies on 32-bit `int` indexing (2 GB max ceiling), is non-deterministic in native deallocation (relying on GC cleaners), and suffers from stateful cursor management (`flip()`, `clear()`, `compact()`).

---

## 2. Proposed Solution: `MemorySegment` & Deterministic `Arena`

### 2.1 Technical Architecture
Java 22's `MemorySegment` and `Arena` APIs provide a zero-cost, type-safe abstraction for contiguous memory:

1. **Deterministic Off-Heap Lifecycle**:
   Allocate an off-heap staging buffer using a shared arena (`Arena.ofShared()`) scoped strictly to the bulk copy operation. A shared arena is required because timeout handling can send a TDS attention packet from another thread:
   ```java
   try (Arena arena = Arena.ofShared()) {
       MemorySegment segment = arena.allocate(packetSize);
       // Process millions of rows off-heap...
   } // Native memory reclaimed immediately with zero GC overhead upon close()
   ```

2. **Direct Hardware-Accelerated Serialization**:
   Replace manual shifting with unaligned native value layouts configured for little-endian byte order:
   ```java
   private static final ValueLayout.OfInt INT_LE =
       ValueLayout.JAVA_INT_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);

   segment.set(INT_LE, position, value);
   ```
   The HotSpot JIT compiler lowers this expression directly into a single native assembly instruction (`mov dword ptr [...]` on x86/ARM64).

3. **Zero-Copy Slicing and Vectorized Bulk Transfers**:
   `MemorySegment.copy()` takes advantage of CPU SIMD instructions (AVX/NEON) for moving packet headers and raw byte blocks over traditional arraycopy.

---

## 3. Empirical Validation (Live Azure SQL Database)

### 3.1 Benchmark Setup
- **Target Database**: Azure SQL Database
- **Dataset**: 30,000 structured rows (`INT`, `BIGINT`, `DECIMAL(18,4)`, `SMALLINT`)
- **Batch Size**: 10,000 rows/batch
- **Network**: Encrypted TLS connection over public internet

### 3.2 Performance Comparison Results

| Metric | Standard On-Heap `ByteBuffer` | `MemorySegment` Off-Heap | Delta |
|---|---|---|---|
| **Elapsed Wall-Clock Time** | **750 ms** | **596 ms** | **20.53% faster** |
| **Garbage Collection (GC) Time** | **16 ms** | **1 ms** | **93.75% reduction** |
| **Row Ingestion Rate** | ~40,000 rows/sec | ~50,335 rows/sec | **+25.8% throughput** |
| **Data Integrity Check** | 30,000 rows verified | 30,000 rows verified | 100% Match |

---

## 4. Multi-Release JAR (MR-JAR) & Backward Compatibility

Because `mssql-jdbc` supports Java 8 through Java 26 via profiles (`jre8`, `jre11`, `jre17`, `jre21`, `jre25`, `jre26`), direct references to `java.lang.foreign.MemorySegment` cannot reside in common Java 8 source paths.

### Proposed Phased Rollout:

### Phase 1: Internal Staging Abstraction
Introduce an internal interface `TDSStagingBuffer`:
```java
interface TDSStagingBuffer extends AutoCloseable {
    void putByte(byte val);
    void putShortLE(short val);
    void putIntLE(int val);
    void putLongLE(long val);
    void putBytes(byte[] src, int offset, int length);
    void flip();
    void clear();
}
```

- **Default Implementation (`TDSHeapStagingBuffer`)**:
  Backported for Java 8–21 using existing `ByteBuffer`/`byte[]`.
- **JDK 22+ Implementation (`TDSMemorySegmentStaging`)**:
  Compiled in JDK 22+ profiles or placed in `META-INF/versions/22/` of the multi-release artifact.

### Phase 2: Dynamic Resolution / Factory
```java
public final class TDSStagingBufferFactory {
    public static TDSStagingBuffer create(int capacity) {
        if (Runtime.version().feature() >= 22) {
            return new TDSMemorySegmentStaging(capacity);
        }
        return new TDSHeapStagingBuffer(capacity);
    }
}
```

### Phase 3: Exposing User-Level Opt-In
Add a configuration flag to [SQLServerBulkCopyOptions.java](../../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerBulkCopyOptions.java):
- Options API: `options.setUseMemorySegment(true)` (default: enabled on JDK 22+, with an automatic heap fallback on earlier runtimes)

---

## 5. Next Steps

1. **Refactor `TDSMemorySegmentStaging` into an MR-JAR Module**:
   Structure the build configuration in `pom.xml` so the class is isolated to the `jre25` and `jre26` profiles while keeping `jre8` through `jre21` compilable.
2. **Expand Vector and LOB Streaming Support**:
   Extend `MemorySegment` serialization to new data types (e.g., SQL Server 2025 native `VECTOR` and `JSON` types) and `PLPInputStream` for large LOBs.
3. **Formal Test Suite Integration**:
   Promote `MemorySegmentBulkCopyPerfComparisonLiveTest` into standard BVT/CI regression runs.
