
# Proposal: Server-Side Statistics Acquisition & OpenTelemetry Export

---

## Core User-Facing Features (Summary)

This feature introduces **zero-friction, standards-based observability** for SQL Server workloads via the JDBC driver. All user-visible changes are summarized here:

### 1. TDS Feature Extension (Server-Side Statistics)
- **New TDS feature extension**: The driver negotiates a new TDS feature (`TDS_FEATURE_EXT_EXECSTATS`) with SQL Server to receive structured, binary server-side execution statistics (IO, timing, compilation) as a dedicated token (`TDS_EXECSTATS`).
- **No manual `SET STATISTICS` required**: Statistics are delivered in a programmatic, locale-independent format.

### 2. PerformanceLogCallback (Single, Backward-Compatible API)
- **Single callback interface**: All client and server metrics are surfaced via the existing `PerformanceLogCallback` interface, with a new default overload for server stats.
- **No application code changes required**: If you already use the callback, you get server stats automatically when enabled.

### 3. Connection String Options (Zero-Friction Activation)
- **All features are enabled/controlled via connection string properties only.** No JVM args, no config files, no code changes.

| Property | Purpose |
|----------|---------|
| `serverExecutionStatistics` | Request server-side stats: `false`, `io`, `time`, `all` |
| `otelEndpoint` | Enable OTLP/HTTP export (URL to collector/backend) |
| `otelExportInterval` | Export interval in seconds (default: 60) |
| `otelBatchSize` | Export when buffer reaches N data points (default: 1000) |
| `otelSamplingRate` | Sample metrics (e.g., 0.1 = 10%) |
| `otelServiceName` | OTLP `service.name` resource attribute |
| `otelHeaders` | Extra HTTP headers for OTLP endpoint (e.g., `Authorization=Bearer ...`) |
| `otelResourceAttributes` | Extra OTLP resource attributes |


### 4. Four Alternative Export Solutions (Select One)

#### Solution 1: Optional OpenTelemetry SDK Bridge
- **Preferred architectural boundary:** The core driver keeps emitting performance events through `PerformanceLogCallback`; an optional companion module converts those events into OpenTelemetry instruments and lets the OpenTelemetry SDK/exporters handle OTLP transport.
- **What the core driver does:** Registers an internal callback discovered through `ServiceLoader` or similar SPI when `otelEndpoint` is set and the optional module is present.
- **What the optional module does:** Implements `PerformanceLogCallback`, maps events to OTel histograms/counters/gauges, and relies on OTel SDK exporters for OTLP/HTTP or OTLP/gRPC.
- **Why this is different:** No OTLP JSON, Protobuf, gRPC, or HTTP payload construction lives in the core driver.

#### Solution 2: Built-In OTLP/HTTP JSON Exporter
- **Single-jar deployment:** The driver directly maps callback events to an internal metric record, batches them, formats OTLP/HTTP JSON, and sends them with `HttpURLConnection`.
- **What the core driver does:** Owns metric naming, attribute mapping, batching, JSON formatting, gzip, and HTTP retry/error behavior.
- **Why this exists:** It preserves the zero-extra-dependency deployment model.

#### Solution 3: JUL File -> Collector `filelog`
- **Ops-centric path:** The driver writes structured performance events to JUL, and the OpenTelemetry Collector parses the file and exports metrics downstream.
- **What the core driver does:** Emits structured log lines and optionally enables JUL performance logging automatically.
- **Why this exists:** It avoids OTLP encoding in the driver entirely and fits environments that already standardize on collector-based log ingestion.

#### Solution 4: Direct OpenTelemetry Dependency in Core Driver
- **Single-artifact OTel:** Same architecture as Solution 1, but `opentelemetry-api`/`-sdk`/`-exporter-otlp` are direct (non-optional) dependencies of `mssql-jdbc.jar`; no companion artifact or SPI.
- **What the core driver does:** Owns the `OpenTelemetryPerformanceCallback`, instrument catalog, and SDK bootstrap (reuses `GlobalOpenTelemetry` if the host application already configured one).
- **Why this exists:** Simplest user experience - one Maven coordinate and one connection string property - at the cost of a permanent dependency on OpenTelemetry libraries inside the driver.

**For this proposal decision, these are mutually exclusive implementation options. One solution is selected for initial delivery.**

---

**Authors:** JDBC Driver Team  
**Date:** May 2026  
**Status:** Draft  
**Scope:** Microsoft JDBC Driver for SQL Server  

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Goals & Non-Goals](#2-goals--non-goals)
3. [Executive Summary](#executive-summary)
4. [Part I: Server-Side Execution Statistics](#3-part-i-server-side-execution-statistics)
   - [Current State Analysis](#31-current-state-analysis)
   - [Server-Side Dependencies](#32-server-side-dependencies-what-sql-server-must-provide)
   - [Proposed TDS Feature Extension](#33-proposed-tds-feature-extension)
   - [New Performance Activities](#34-new-performance-activities)
   - [Driver Implementation](#35-driver-implementation)
5. [Part II: OpenTelemetry Export](#4-part-ii-opentelemetry-export)
  - [Solution 1: Optional OpenTelemetry SDK Bridge](#41-solution-1-optional-opentelemetry-sdk-bridge)
  - [Solution 2: Built-In OTLPHTTP JSON Exporter](#42-solution-2-built-in-otlphttp-json-exporter)
  - [Solution 3: JUL File to Collector filelog](#43-solution-3-jul-file-to-collector-filelog)
  - [Solution 4: Direct OpenTelemetry Dependency in Core Driver](#44-solution-4-direct-opentelemetry-dependency-in-core-driver)
  - [Cross-Solution Comparison](#45-cross-solution-comparison)
6. [Connection String Properties Summary](#5-connection-string-properties-summary)
7. [Security Considerations](#6-security-considerations)
8. [Testing Strategy](#7-testing-strategy)
9. [Overhead & Optimization Strategies](#8-overhead--optimization-strategies)
10. [Future Work](#9-future-work)
11. [Appendix A: End-to-End Usage Example](#appendix-a-end-to-end-usage-example)
12. [Appendix B: Programmatic Callback Usage](#appendix-b-programmatic-callback-usage-without-otlp)
13. [Appendix C: Architecture Diagrams](#appendix-c-architecture-diagrams)
14. [Appendix D: Implementation Details](#appendix-d-implementation-details)
    - [D.1 Implementation Plan](#d1-implementation-plan)
    - [D.2 File Change Inventory](#d2-file-change-inventory)
    - [D.3 Solution 1 Implementation Details](#d3-solution-1-implementation-details)
    - [D.4 Solution 2 Implementation Details](#d4-solution-2-implementation-details)
    - [D.5 Solution 3 Implementation Details](#d5-solution-3-implementation-details)
    - [D.6 Solution 4 Implementation Details](#d6-solution-4-implementation-details)

---

<a id="1-motivation"></a>
## 1. Motivation

The JDBC driver already provides client-side performance metrics via the `PerformanceLogCallback` infrastructure. These metrics cover connection lifecycle (PRELOGIN, LOGIN, TOKEN_ACQUISITION) and statement execution phases (REQUEST_BUILD, FIRST_SERVER_RESPONSE, PREPARE, PREPEXEC, EXECUTE). However, **the driver has no visibility into what SQL Server did internally** during query execution:

- How much CPU time did the query consume on the server?
- Were there physical reads (cold cache) vs. logical reads (buffer pool)?
- How many LOB pages were accessed?
- What was the server-side parse and compile time?

Today, the only way to obtain this information is via `SET STATISTICS TIME ON` / `SET STATISTICS IO ON`, which returns data as **unstructured text in TDS INFO messages** (`TDS_MSG 0xAB`). These arrive as `SQLWarning` objects that applications must manually parse — dealing with locale-specific formatting, collation differences, and fragile string parsing.

Additionally, there is no built-in mechanism to **export** any performance metrics (client or server-side) to modern observability platforms. Teams must build custom plumbing to bridge `PerformanceLogCallback` to their monitoring systems.

This proposal addresses both gaps.

---

<a id="2-goals--non-goals"></a>
## 2. Goals & Non-Goals

### Goals

| # | Goal |
|---|------|
| G1 | Acquire server-side execution statistics (IO, timing, compilation) in a structured, binary format via TDS |
| G2 | Surface server-side stats through the existing `PerformanceLogCallback` as new `PerformanceActivity` types |
| G3 | Provide an opt-in connection string property to enable server-side stats collection |
| G4 | Export combined client + server metrics to an OpenTelemetry endpoint in OTLP format |
| G5 | Entire feature activated via connection string properties only — no programmatic setup required |

### Non-Goals

| # | Non-Goal |
|---|----------|
| N1 | Implementing W3C Distributed Tracing / TraceContext propagation (orthogonal feature) |
| N2 | Replacing the existing `ActivityCorrelator` mechanism |
| N3 | Exporting query text or parameter values (privacy/security concern) |
| N4 | Providing a pull-based metrics endpoint (Prometheus-style scraping) |
| N5 | Supporting OTLP/gRPC transport (HTTP/protobuf only, to avoid gRPC dependency) |

---

<a id="executive-summary"></a>
## Executive Summary

This proposal has two independent parts that can ship together or separately.

**Part I — Server-side execution statistics over TDS.** A new TDS feature extension (`TDS_FEATURE_EXT_EXECSTATS = 0x11`) negotiates a per-batch binary token (`TDS_EXECSTATS = 0xE5`) that carries structured IO, timing, and compilation counters. These flow through the existing `PerformanceLogCallback` SPI as new `PerformanceActivity` types, so any consumer of the driver's perf callback automatically sees server-side numbers alongside client-side timings — no text parsing of `SET STATISTICS` output, no locale dependence, no session-level side effects. Activated by a connection string property; off by default.

**Part II — Exporting those metrics as OpenTelemetry.** Four mutually exclusive export options are presented, each with different trade-offs around dependency footprint, operational model, and integration depth. The driver remains agnostic about which one is chosen at build time of the feature, but exactly one is selected per deployment:

| # | Option | How metrics leave the JVM | Driver dependency footprint |
|---|--------|---------------------------|-----------------------------|
| 1 | Optional bridge JAR (`ServiceLoader` SPI) | OTel SDK in user's app, loaded if companion JAR is on the classpath | None added to core driver |
| 2 | Built-in OTLP/HTTP JSON exporter | Driver POSTs OTLP/HTTP to a Collector/backend endpoint | Only JDK `HttpURLConnection` |
| 3 | JUL `FileHandler` writing OTLP-shaped JSON | OTel Collector `filelog` receiver tails the file | None added (uses `java.util.logging`) |
| 4 | Direct OTel API/SDK dependency in the driver | Driver uses OTel SDK directly (reuses `GlobalOpenTelemetry` if set) | OTel API + SDK + OTLP exporter pulled into `mssql-jdbc.jar` |

**Common properties across all four options:**

- **Opt-in.** Disabled by default; activated via connection string properties only — no programmatic setup required.
- **No breaking changes.** Existing applications observe no behavioral or API change when the feature is unused.
- **Zero overhead when not configured.** No buffers allocated, no threads spawned, no classes loaded beyond what existing driver code already references.
- **Standard OTLP semantics.** Whichever option a user picks, the data eventually lands in a backend as OTLP-compatible metrics — Solutions 1, 2, and 4 emit OTLP directly; Solution 3 produces OTLP-shaped JSON that the Collector converts to OTLP on egress.

Section 4 ("Part II: OpenTelemetry Export") describes each solution in detail along with cross-solution comparison and selection guidance. Implementation details, code shapes, and deployment examples for each are in [Appendix D](#appendix-d-implementation-details).

---

<a id="3-part-i-server-side-execution-statistics"></a>
## 3. Part I: Server-Side Execution Statistics

<a id="31-current-state-analysis"></a>
### 3.1 Current State Analysis

Today, SET STATISTICS output flows through the TDS protocol as follows:

```
SQL Server                          Driver                         Application
    │                                  │                               │
    │  TDS_MSG (0xAB) tokens           │                               │
    │  containing text like:           │                               │
    │  "Table 't1'. Scan count 1,      │  SQLServerStatement           │
    │   logical reads 1, ..."          │  .NextResult.onInfo()         │
    │ ────────────────────────────────>│  → SQLServerInfoMessage       │
    │                                  │  → SQLWarning                 │
    │                                  │ ────────────────────────────>│
    │                                  │                               │  stmt.getWarnings()
    │                                  │                               │  → parse text manually
```

**Problems with this approach:**

| Problem | Impact |
|---------|--------|
| Unstructured text | Applications must regex-parse locale-dependent strings |
| Collation-dependent | Message text varies by server collation/language |
| No binary encoding | Cannot be consumed programmatically without parsing |
| Mixed with real warnings | Statistics text is interleaved with actual SQL warnings |
| Session-level side effect | `SET STATISTICS ON` changes session state, affects all subsequent queries |
| Performance overhead | Text formatting on the server + parsing on the client |

<a id="32-server-side-dependencies-what-sql-server-must-provide"></a>
### 3.2 Server-Side Dependencies: What SQL Server Must Provide

This section explicitly documents **what this feature requires from SQL Server**. The JDBC driver cannot implement Part I without these server-side capabilities.

#### 3.2.1 SQL Server Engine Requirements

| Requirement | Details | Rationale |
|---|---|---|
| **TDS Protocol Support** | SQL Server must support TDS Feature Extensions negotiation (already supported in TDS 7.4+) | Feature extension IDs (0x01–0x10) are already defined; we need to register 0x11 |
| **New TDS Token Type** | SQL Server must emit a new `TDS_EXECSTATS` token (proposed ID: 0xE5) after statement execution | The current `TDS_MSG` text format is unstructured and locale-dependent; we need a dedicated token with binary JSON payload |
| **Binary JSON Encoding** | SQL Server must serialize execution statistics as UTF-8 JSON and wrap it in the new token | Ensures deterministic, schema-versioned output independent of collation or language settings |
| **Feature Negotiation** | Server must respond to `FEATUREEXT (0x11)` request with `FEATUREEXTACK (0x11)` indicating supported version and stats mask | Allows graceful fallback if server doesn't support this version |
| **Runtime Statistics Capture** | Server must gather IO and timing statistics during query execution and make them available for export | Requires access to existing `sys.dm_exec_session_stats` or internal query engine telemetry |

#### 3.2.2 SQL Server Version Prerequisites

| Component | Minimum Version | Notes |
|---|---|---|
| **TDS Protocol** | 7.4+ (SQL Server 2016+) | Feature extensions already supported; 0x11 is a new registration |
| **Query Engine** | SQL Server 2016+  | Statistics capture for `SET STATISTICS IO` and `TIME` already exists; we need structured export |
| **JSON Support** | SQL Server 2016+ (native JSON) | Not required on server-side, but mentioned for context |

> **Key Point:** This requires a **SQL Server engine change**. The driver alone cannot request this feature; the server must implement the `TDS_EXECSTATS` token emission and feature extension support.

#### 3.2.3 What Happens Without Server Support

If the SQL Server instance does not support `TDS_FEATURE_EXT_EXECSTATS` (0x11):

1. Driver sends feature request during LOGIN7 handshake
2. Server responds with `FEATUREEXTACK (0x11) = NOT_SUPPORTED` (or omits it entirely)
3. Driver detects lack of support and **gracefully degrades**:
   - Sets `serverExecutionStatistics = "false"` internally
   - No server-side stats are captured or exported
   - Driver continues normally (client-side metrics still work)
   - **No error is raised to the application**

**Current workaround (until server support available):**

Applications can enable `SET STATISTICS IO ON` and `SET STATISTICS TIME ON` manually, then parse the `SQLWarning` messages using regex (existing behavior unchanged). This remains an option for servers without native support.

#### 3.2.4 Coordination with SQL Server Team

This feature requires **coordination with the SQL Server product team**:

1. **TDS Feature ID registration** — Request allocation of 0x11 in the TDS specification
2. **Token ID allocation** — Request a new token type ID (e.g., 0xE5) for `TDS_EXECSTATS`
3. **Feature extension support** — Implement feature extension negotiation for `TDS_FEATURE_EXT_EXECSTATS`
4. **Runtime statistics export** — Implement `TDS_EXECSTATS` token emission with binary JSON payload
5. **Version handling** — Support graceful fallback for older servers

**Prerequisite for Part I Implementation:** SQL Server team must commit to implementing this feature in a future release (e.g., SQL Server 2027, Azure SQL Managed Instance 2024-10 or later).

#### 3.2.5 Binary JSON Payload Format (Server → Client)

This section defines what the server **must send** in the `TDS_EXECSTATS` token:

```
┌─────────────────────────────────────────────┐
│ TDS_EXECSTATS Token Stream                  │
├─────────────┬──────────────────────────────┤
│ Token Type  │ 0xE5                         │
│ Total Length│ Length in bytes (4-byte LE)  │
│ Format      │ 0x01 = UTF-8 JSON            │
│ Payload     │ Raw UTF-8 bytes (no BOM)     │
└─────────────┴──────────────────────────────┘
```

**Example server-side execution:**

```sql
-- Client request:
EXEC sp_executesql N'SELECT COUNT(*) FROM orders WHERE status=@status',
                    N'@status NVARCHAR(20)', 'Active'

-- Server response sequence:
1. COLMETADATA token  (column schema for SELECT result)
2. ROW token          (COUNT(*) = 42)
3. DONE token         (statement complete)
4. TDS_EXECSTATS token (binary JSON with IO/timing stats) ◄── NEW
5. (more statements if batched)
```

**Binary JSON content (UTF-8 encoded):**

```json
{
  "version": 1,
  "statementIndex": 0,
  "executionStats": {
    "io": {
      "tables": [
        {
          "databaseId": 5,
          "objectId": 245575913,
          "name": "dbo.orders",
          "schema": "dbo",
          "scanCount": 1,
          "logicalReads": 8,
          "physicalReads": 0,
          "pageServerReads": 0,
          "readAheadReads": 0,
          "pageServerReadAheadReads": 0,
          "lobLogicalReads": 0,
          "lobPhysicalReads": 0,
          "lobPageServerReads": 0,
          "lobReadAheadReads": 0,
          "lobPageServerReadAheadReads": 0
        }
      ]
    },
    "timing": {
      "parseAndCompileCpuTimeMs": 1,
      "parseAndCompileElapsedTimeMs": 2,
      "executionCpuTimeMs": 5,
      "executionElapsedTimeMs": 15
    }
  }
}
```

**Constraints for the server implementation:**

- UTF-8 encoding only (no collation dependency)
- JSON keys must be stable (no random ordering)
- Version field is mandatory (for future schema evolution)
- All numeric values are integers (milliseconds, page counts)
- No floating-point numbers (avoid precision issues)

---

<a id="33-proposed-tds-feature-extension"></a>
### 3.3 Proposed TDS Feature Extension

We propose a new TDS Feature Extension that allows the driver to **request structured execution statistics** from the server in a binary JSON format.

#### 3.2.1 Feature Extension Constants

```java
// IOBuffer.java — new constants
static final byte TDS_FEATURE_EXT_EXECSTATS = 0x11;  // Next available ID after USERAGENT (0x10)
static final byte EXECSTATS_NOT_SUPPORTED = 0x00;
static final byte EXECSTATS_VERSION_1 = 0x01;
static final byte MAX_EXECSTATS_VERSION = 0x01;
```

> **Note:** The actual feature ID (`0x11`) must be coordinated with the SQL Server team and registered in the TDS specification. The value shown is illustrative.

#### 3.2.2 Feature Extension Negotiation

**Login (Client → Server):**

```
┌──────────────────────────────────────────────────────┐
│ FEATUREEXT payload for EXECSTATS                      │
├──────────┬────────┬──────────────────────────────────┤
│ FeatureId│ Length │ Data                              │
│  (1 byte)│(4 byte)│                                  │
│   0x11   │  0x02  │ Version(1) + StatsMask(1)        │
└──────────┴────────┴──────────────────────────────────┘
```

The `StatsMask` byte is a bitmask indicating which categories the driver wants:

| Bit | Category | Equivalent to |
|-----|----------|---------------|
| 0 | IO Statistics | `SET STATISTICS IO ON` |
| 1 | Time Statistics | `SET STATISTICS TIME ON` |
| 2-7 | Reserved | Future: profile, query plan stats, wait stats |

**Login ACK (Server → Client):**

```
┌──────────────────────────────────────────────────────┐
│ FEATUREEXTACK payload for EXECSTATS                   │
├──────────┬────────┬──────────────────────────────────┤
│ FeatureId│ Length │ Data                              │
│   0x11   │  0x02  │ Version(1) + AckedMask(1)        │
└──────────┴────────┴──────────────────────────────────┘
```

The server responds with the version it supports and the subset of stats it can provide.

#### 3.2.3 Server-Side Statistics Token

When the feature is negotiated, instead of (or in addition to) sending `TDS_MSG` info messages for statistics, the server sends a **new TDS token** or repurposes the `FEATUREEXTACK` token stream to include a structured binary payload **after each statement execution**.

**Proposed approach — new token type:**

```
┌──────────────────────────────────────────────────────────────┐
│ TDS_EXECSTATS Token (0xE5 — illustrative, must be reserved) │
├──────────┬───────────────────────────────────────────────────┤
│ TokenType│ 0xE5                                   (1 byte)   │
│ Length   │ Total payload length                    (4 bytes)  │
│ Format   │ 0x01 = Binary JSON (UTF-8)             (1 byte)   │
│ Data     │ Binary-encoded JSON document            (N bytes)  │
└──────────┴───────────────────────────────────────────────────┘
```

**Why binary JSON over TDS_MSG text?**

| Concern | TDS_MSG (text) | TDS_EXECSTATS (binary JSON) |
|---------|---------------|---------------------------|
| Parsing | Regex over locale-dependent text | Deterministic binary/JSON decode |
| Collation | Varies by server language | UTF-8 always |
| Schema | None — format can change | Versioned JSON schema |
| Overhead | Text formatting + client parsing | Direct binary write + read |
| Separation | Mixed with real warnings | Dedicated token, clean separation |

#### 3.2.4 JSON Statistics Schema (v1)

The binary payload is a UTF-8 encoded JSON document (sent as raw bytes, no BOM, no collation header). The driver deserializes it using a lightweight internal JSON reader (no external dependency).

```json
{
  "version": 1,
  "statements": [
    {
      "statementId": 0,
      "io": {
        "tables": [
          {
            "name": "t1",
            "schema": "dbo",
            "scanCount": 1,
            "logicalReads": 1,
            "physicalReads": 0,
            "pageServerReads": 0,
            "readAheadReads": 0,
            "pageServerReadAheadReads": 0,
            "lobLogicalReads": 0,
            "lobPhysicalReads": 0,
            "lobPageServerReads": 0,
            "lobReadAheadReads": 0,
            "lobPageServerReadAheadReads": 0
          }
        ]
      },
      "time": {
        "parseAndCompile": {
          "cpuTimeMs": 0,
          "elapsedTimeMs": 1
        },
        "execution": {
          "cpuTimeMs": 0,
          "elapsedTimeMs": 0
        }
      }
    }
  ]
}
```

<a id="34-new-performance-activities"></a>
### 3.4 New Performance Activities

The following new `PerformanceActivity` enum values surface server-side stats through the existing callback:

```java
// PerformanceActivity.java — new entries

// Server-side timing
SERVER_PARSE_AND_COMPILE("Server parse and compile time"),
SERVER_EXECUTION_TIME("Server execution time"),

// Server-side I/O (aggregate across all tables in the statement)
SERVER_IO_STATS("Server IO statistics"),
```

<a id="35-driver-implementation"></a>
### 3.5 Driver Implementation

#### 3.5.1 New Classes

| Class | Purpose |
|-------|---------|
| `ServerExecutionStats.java` | Immutable data class holding parsed server statistics |
| `ServerIOStats.java` | Per-table I/O statistics record |
| `ServerTimeStats.java` | Parse/compile and execution timing record |
| `StreamExecStats.java` | TDS token handler for the new `TDS_EXECSTATS` token |

#### 3.5.2 ServerExecutionStats

```java
package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.List;

/**
 * Immutable container for server-side execution statistics received via TDS.
 */
public final class ServerExecutionStats {
    private final List<ServerIOStats> ioStats;
    private final ServerTimeStats parseAndCompileTime;
    private final ServerTimeStats executionTime;

    ServerExecutionStats(List<ServerIOStats> ioStats,
                         ServerTimeStats parseAndCompileTime,
                         ServerTimeStats executionTime) {
        this.ioStats = Collections.unmodifiableList(ioStats);
        this.parseAndCompileTime = parseAndCompileTime;
        this.executionTime = executionTime;
    }

    public List<ServerIOStats> getIOStats() { return ioStats; }
    public ServerTimeStats getParseAndCompileTime() { return parseAndCompileTime; }
    public ServerTimeStats getExecutionTime() { return executionTime; }
}
```

#### 3.5.3 ServerIOStats

```java
public final class ServerIOStats {
    private final String tableName;
    private final String schemaName;
    private final long scanCount;
    private final long logicalReads;
    private final long physicalReads;
    private final long pageServerReads;
    private final long readAheadReads;
    private final long pageServerReadAheadReads;
    private final long lobLogicalReads;
    private final long lobPhysicalReads;
    private final long lobPageServerReads;
    private final long lobReadAheadReads;
    private final long lobPageServerReadAheadReads;

    // Constructor, getters omitted for brevity
}
```

#### 3.5.4 ServerTimeStats

```java
public final class ServerTimeStats {
    private final long cpuTimeMs;
    private final long elapsedTimeMs;

    // Constructor, getters
}
```

#### 3.5.5 Feature Request Method

```java
// SQLServerConnection.java

int writeExecStatsFeatureRequest(boolean write, TDSWriter tdsWriter) throws SQLServerException {
    if (!requestServerExecStats) {
        return 0;  // Feature not requested, skip
    }
    int len = 7;  // 1 (featureId) + 4 (length) + 1 (version) + 1 (statsMask)
    if (write) {
        tdsWriter.writeByte(TDS.TDS_FEATURE_EXT_EXECSTATS);
        tdsWriter.writeInt(2);  // data length: version + mask
        tdsWriter.writeByte(TDS.EXECSTATS_VERSION_1);
        tdsWriter.writeByte(execStatsMask);  // e.g. 0x03 = IO + Time
    }
    return len;
}
```

#### 3.5.6 Feature ACK Processing

```java
// SQLServerConnection.java — in onFeatureExtAck()

case TDS.TDS_FEATURE_EXT_EXECSTATS: {
    if (data.length < 2) {
        throw new SQLServerException(
            SQLServerException.getErrString("R_invalidExecStatsAck"), null);
    }
    serverExecStatsVersion = data[0];
    serverExecStatsMask = data[1];
    if (serverExecStatsVersion == 0 || serverExecStatsVersion > TDS.MAX_EXECSTATS_VERSION) {
        serverExecStatsVersion = TDS.EXECSTATS_NOT_SUPPORTED;
        serverExecStatsMask = 0;
    }
    serverSupportsExecStats = (serverExecStatsVersion > 0);
    break;
}
```

#### 3.5.7 Token Handling

```java
// tdsparser.java — in parse() switch

case TDS.TDS_EXECSTATS:
    parsing = tdsTokenHandler.onExecStats(tdsReader);
    break;
```

```java
// TDSTokenHandler — default implementation
boolean onExecStats(TDSReader tdsReader) throws SQLServerException {
    TDSParser.ignoreLengthPrefixedToken(tdsReader);  // Ignore if handler doesn't care
    return true;
}
```

```java
// SQLServerStatement.NextResult — override
boolean onExecStats(TDSReader tdsReader) throws SQLServerException {
    ServerExecutionStats stats = StreamExecStats.parse(tdsReader);
    lastServerExecStats = stats;
    publishServerStats(stats);
    return true;
}
```

#### 3.5.8 Publishing to PerformanceLogCallback

```java
// SQLServerStatement.java

private void publishServerStats(ServerExecutionStats stats) {
    // Server parse/compile time
    if (stats.getParseAndCompileTime() != null) {
        try (PerformanceLog.Scope scope = PerformanceLog.createScope(
                PerformanceLog.perfLoggerStatement,
                connectionId, statementId,
                PerformanceActivity.SERVER_PARSE_AND_COMPILE)) {
            scope.overrideDuration(stats.getParseAndCompileTime().getElapsedTimeMs());
        }
    }
    // Server execution time
    if (stats.getExecutionTime() != null) {
        try (PerformanceLog.Scope scope = PerformanceLog.createScope(
                PerformanceLog.perfLoggerStatement,
                connectionId, statementId,
                PerformanceActivity.SERVER_EXECUTION_TIME)) {
            scope.overrideDuration(stats.getExecutionTime().getElapsedTimeMs());
        }
    }
    // Server I/O stats — published via existing callback overload
    if (!stats.getIOStats().isEmpty()) {
        try (PerformanceLog.Scope scope = PerformanceLog.createScope(
                PerformanceLog.perfLoggerStatement,
                connectionId, statementId,
                PerformanceActivity.SERVER_IO_STATS)) {
            scope.overrideDuration(0);  // I/O stats are counters, not durations
            scope.setServerIOStats(stats.getIOStats());
        }
    }
}
```

### 3.5 Public API Surface

#### 3.5.1 Existing Callback: New Server-Stats Publish Overload

To pass rich server-side statistics (which are not just durations), we add a **new default publish overload** to the existing `PerformanceLogCallback` interface. This keeps a single callback registration model while preserving backward compatibility.

```java
/**
 * Existing performance callback with an additional default overload
 * for structured server-side execution statistics.
 */
public interface PerformanceLogCallback {

  void publish(PerformanceActivity activity, int connectionId,
         long durationMs, Exception exception);

  void publish(PerformanceActivity activity, int connectionId,
         int statementId, long durationMs, Exception exception);

    /**
     * Called when server-side execution statistics are available.
     *
     * @param activity     SERVER_IO_STATS, SERVER_PARSE_AND_COMPILE, or SERVER_EXECUTION_TIME
     * @param connectionId Connection identifier
     * @param statementId  Statement identifier
     * @param stats        The server-side execution statistics
     */
    default void publish(PerformanceActivity activity, int connectionId,
               int statementId, ServerExecutionStats stats) {
        // Default: no-op. Existing implementations don't need to change.
    }
}
```

#### 3.5.2 Statement Accessor

```java
// ISQLServerStatement.java — new method

/**
 * Returns the server-side execution statistics from the last executed statement,
 * or null if server stats collection is not enabled or the server did not return stats.
 */
ServerExecutionStats getServerExecutionStats();
```

#### 3.5.3 Connection String Property

```java
// SQLServerDriverStringProperty — new enum value
SERVER_EXEC_STATS("serverExecutionStatistics", "false"),
```

Valid values: `"false"` (default), `"io"`, `"time"`, `"all"` (equivalent to `"io,time"`).

```
jdbc:sqlserver://host;serverExecutionStatistics=all
```

---


<a id="4-part-ii-opentelemetry-export"></a>
## 4. Part II: OpenTelemetry Export

This part presents three complete, self-contained solution options. They are **alternatives**, not simultaneously active paths, for the initial implementation.

<a id="41-solution-1-optional-opentelemetry-sdk-bridge"></a>
### 4.1 Solution 1: Optional OpenTelemetry SDK Bridge

Solution 1 is the cleanest architectural boundary. The core JDBC driver keeps doing only what it already knows how to do well: emit performance events through `PerformanceLogCallback`. An optional companion module consumes those events and uses the OpenTelemetry API/SDK to create instruments and export them.

#### 4.1.1 User Experience

- User-visible connection string properties remain unchanged: `otelEndpoint`, `otelExportInterval`, `otelBatchSize`, `otelSamplingRate`, `otelServiceName`, `otelHeaders`, and `otelResourceAttributes`.
- The application adds an optional companion artifact, for example `mssql-jdbc-opentelemetry`, to the classpath.
- If the artifact is present and `otelEndpoint` is configured, the bridge activates automatically.
- If the artifact is absent, the driver logs one clear warning and continues normally without export.

##### Application Dependency Setup

What the application team must add depends on which deployment path they take. The core driver coordinate never changes.

**Path A — Already running the OpenTelemetry Java agent (most common):**

```xml
<dependencies>
  <!-- existing -->
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc</artifactId>
    <version>${mssql-jdbc.version}</version>
  </dependency>

  <!-- NEW: optional bridge. Pulls in OTel API only; SDK/exporter come from the javaagent. -->
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-opentelemetry</artifactId>
    <version>${mssql-jdbc.version}</version>
  </dependency>
</dependencies>
```

Plus the existing `-javaagent:opentelemetry-javaagent.jar` JVM flag the team already uses. No extra OTel SDK or exporter coordinates needed — the agent provides them at runtime.

**Path B — Already running the Azure Monitor Application Insights Java agent:**

```xml
<dependencies>
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc</artifactId>
    <version>${mssql-jdbc.version}</version>
  </dependency>
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-opentelemetry</artifactId>
    <version>${mssql-jdbc.version}</version>
  </dependency>
</dependencies>
```

Same two coordinates. The Azure agent (added via `-javaagent:applicationinsights-agent.jar`) registers a `GlobalOpenTelemetry` that the bridge picks up automatically.

**Path C — Pure programmatic, no javaagent on the JVM:**

```xml
<dependencies>
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc</artifactId>
    <version>${mssql-jdbc.version}</version>
  </dependency>

  <!-- Single fat coordinate: bundles bridge + OTel SDK + OTLP/HTTP exporter -->
  <dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-opentelemetry-all</artifactId>
    <version>${mssql-jdbc.version}</version>
  </dependency>
</dependencies>
```

Single new coordinate. Application teams who do not run any javaagent get the same one-line UX as Paths A/B.

##### Why Two Bridge Coordinates Instead of One

The proposal ships **two** bridge artifacts:

| Coordinate | Contents | Intended for |
|------------|----------|--------------|
| `mssql-jdbc-opentelemetry` | Bridge classes + `opentelemetry-api` only | Paths A and B (a javaagent already provides the SDK + exporter on the classpath) |
| `mssql-jdbc-opentelemetry-all` | Bridge classes + `opentelemetry-api` + `opentelemetry-sdk` + `opentelemetry-exporter-otlp` (declared as regular `runtime` dependencies in the POM) | Path C (no javaagent; the bridge is the only thing wiring the SDK) |

A single coordinate that bundles the SDK is **not** safe for all paths because:

1. The OpenTelemetry Java agent and the Azure Monitor agent each ship and load their own copy of the OTel SDK from an isolated classloader. If the application also has `opentelemetry-sdk` on its main classloader, two SDKs are present in the same JVM. Depending on classloader visibility this leads to:
   - duplicate `GlobalOpenTelemetry` registrations (one wins, the other's metrics vanish silently),
   - `LinkageError` / `NoSuchMethodError` if the agent and app pin different SDK minor versions,
   - double-counting if both SDKs end up with active exporters.
2. Shading the SDK into the bridge jar (rather than depending on it) avoids the version conflict but makes OTel SDK upgrades a driver release event, which is the wrong release cadence.

Shipping two coordinates keeps each path's classpath minimal and version-honest: agent-based deployments get only the API artifact (no SDK overlap), and standalone deployments get one fat coordinate (no manual SDK wiring).

##### Resolved Classpath, by Path

| Path | `opentelemetry-api` on classpath | `opentelemetry-sdk` on classpath | OTLP exporter on classpath | Source |
|------|----------------------------------|----------------------------------|----------------------------|--------|
| A (otel-javaagent) | yes (via bridge) | yes (via agent's isolated loader) | yes (via agent's isolated loader) | bridge + agent |
| B (Azure agent) | yes (via bridge) | yes (via Azure agent's loader) | yes (via Azure agent's loader) | bridge + agent |
| C (standalone) | yes (via `-all` bridge) | yes (via `-all` bridge) | yes (via `-all` bridge) | `-all` bridge only |

In Paths A/B the bridge's `opentelemetry-api` is a thin facade; the agent's SDK provides the actual implementation. The bridge never instantiates an SDK in those paths — `SdkBuilder.buildOrReuseGlobal(...)` returns the already-registered `GlobalOpenTelemetry` and skips the programmatic build.

**Gradle equivalent (Path A):**

```groovy
dependencies {
    implementation "com.microsoft.sqlserver:mssql-jdbc:${mssqlJdbcVersion}"
    runtimeOnly    "com.microsoft.sqlserver:mssql-jdbc-opentelemetry:${mssqlJdbcVersion}"
}
```

`runtimeOnly` is sufficient because nothing in application source code references the bridge — it is discovered via `ServiceLoader` at driver initialization. For Path C, swap the second line for `runtimeOnly "com.microsoft.sqlserver:mssql-jdbc-opentelemetry-all:${mssqlJdbcVersion}"`.

##### Version Alignment Rules

| Pair | Rule |
|------|------|
| `mssql-jdbc` ↔ `mssql-jdbc-opentelemetry[-all]` | Must match major.minor. Patch versions are independent. Mismatched majors produce a clear startup warning and the bridge stays inert. |
| Bridge ↔ OpenTelemetry API | Bridge is built against a pinned OTel API minor; OTel API is binary-compatible across minors, so runtime upgrades to a newer compatible OTel API are safe. |
| `-all` bridge ↔ bundled OTel SDK / exporter | Pinned together at build time of the `-all` jar; the application does not pick the SDK version on Path C. To upgrade, bump the `-all` coordinate. |

##### What the Application Code Changes

Nothing. There are no new imports, no new initializer code, no new bean definitions. The only changes are:

1. One coordinate added to `pom.xml` / `build.gradle` (`-opentelemetry` for Paths A/B, `-opentelemetry-all` for Path C).
2. `otelEndpoint=...` (and any other `otel*` properties) added to the JDBC connection string.

#### 4.1.2 Runtime Flow

```
JDBC work completes
  -> PerformanceLog.Scope closes
  -> PerformanceLog invokes callbacks
  -> optional OTel bridge callback receives event
  -> bridge maps event to OTel instrument call
  -> OTel SDK batches and exports
  -> OTLP backend / collector receives data
```

#### 4.1.3 Callback Fit

The callback is the handoff point.

- Existing client-side events already arrive through `publish(PerformanceActivity, ...)`.
- New server-side stats arrive through the new default overload `publish(PerformanceActivity, int, int, ServerExecutionStats)`.
- In Solution 1, the core driver does **not** convert these events into OTLP records, JSON payloads, or Protobuf messages.
- Instead, the optional bridge module provides a `PerformanceLogCallback` implementation that directly records OTel measurements.

That means the core driver owns event production; the optional bridge owns telemetry semantics and export.

#### 4.1.4 Code Responsibilities

Core driver responsibilities:

- Emit performance events exactly once through `PerformanceLog`.
- Discover an optional bridge implementation through `ServiceLoader` or equivalent SPI.
- Register the discovered callback alongside any user callback.
- Never import OpenTelemetry SDK classes directly.

Optional bridge module responsibilities:

- Implement `PerformanceLogCallback`.
- Create OTel instruments using `Meter`.
- Map JDBC events to instrument updates.
- Configure the OTel SDK exporter stack from the same connection string properties.

#### 4.1.5 Optional Dependency Model

This solution is possible because the dependency boundary sits between two artifacts, not inside one compile unit.

- `mssql-jdbc` compiles without any OTel dependency because it references only its own SPI and callback interfaces.
- `mssql-jdbc-opentelemetry` compiles separately and depends on both `mssql-jdbc` and the OpenTelemetry API/SDK/exporters.
- At runtime, the core driver discovers the bridge only if that optional artifact is present.

#### 4.1.6 What the Driver Does Not Do

In Solution 1, the core driver does **not** do any of the following:

- build OTLP JSON
- build OTLP Protobuf messages
- open gRPC channels
- open HTTP exporter connections
- define an internal `MetricDataPoint` wire model

This is the main reason to prefer Solution 1 as the architectural lead.

#### 4.1.7 Pros and Cons

Pros:

- Cleanest separation of concerns
- No OTLP wire-format code in core driver
- No OpenTelemetry dependency in core driver jar
- Lets the OpenTelemetry SDK own exporter behavior, retries, and protocol evolution

Cons:

- Requires an extra optional artifact on the classpath
- Still needs event-to-metric mapping logic, just not in the core driver
- Requires version compatibility between driver and bridge module


> **Implementation details.** For example code, deep walkthroughs, deployment examples, and debugging guidance for Solution 1, see [Appendix D.3: Solution 1 Implementation Details](#d3-solution-1-implementation-details).

<a id="42-solution-2-built-in-otlphttp-json-exporter"></a>
### 4.2 Solution 2: Built-In OTLP/HTTP JSON Exporter

Solution 2 is the fully self-contained option. The core driver owns both the event-to-metric mapping and the OTLP/HTTP JSON export path.

#### 4.2.1 User Experience

- User-visible properties remain the same.
- No extra dependency is needed.
- In this solution option, `otelEndpoint` is the direct OTLP/HTTP destination and the driver exports directly.

#### 4.2.2 Runtime Flow

```
JDBC work completes
  -> PerformanceLog.Scope closes
  -> InternalPerformanceCollector callback receives event
  -> collector maps event to internal MetricDataPoint
  -> MetricsBuffer stores the data point
  -> ExportScheduler drains the buffer
  -> OtlpJsonFormatter builds OTLP JSON
  -> OtlpHttpExporter POSTs gzipped payload
```

#### 4.2.3 Callback Fit

This is the solution where the callback-to-metric conversion happens inside the driver.

- `InternalPerformanceCollector` implements `PerformanceLogCallback`.
- On each callback, it creates one or more internal metric records.
- Those records carry metric name, value, timestamp, and attributes.
- The exporter later serializes those internal records into OTLP JSON.

This is the solution the user was concerned about earlier: the driver must understand metric naming and OTLP-oriented structure here.

#### 4.2.4 Internal Metric Mapping

Example mappings:

| Driver event | Internal metric name | Unit |
|---|---|---|
| `STATEMENT_EXECUTE` | `db.client.statement.execute.duration` | `ms` |
| `LOGIN` | `db.client.connection.login.duration` | `ms` |
| `SERVER_EXECUTION_TIME` | `db.server.execution.elapsed_time` | `ms` |
| `SERVER_IO_STATS.logicalReads` | `db.server.io.logical_reads` | `{pages}` |

Common attributes on each internal metric record:

- `db.connection.id`
- `db.statement.id`
- `server.address`
- `server.port`
- `db.namespace`
- `db.operation.name`
- `error.type` when applicable

#### 4.2.5 Wire Format Responsibility

Because there is no OTel dependency here, the driver must explicitly own:

- metric name mapping
- attribute layout
- OTLP JSON schema formatting
- gzip compression
- HTTP retries and error handling

That is the tradeoff for a single-jar solution.

#### 4.2.6 Transport Behavior

- `HttpURLConnection` is sufficient for POSTing OTLP JSON
- batches are time-triggered and size-triggered
- failures are logged and dropped without surfacing to the application
- `otelHeaders` provides custom headers such as authorization

#### 4.2.7 Pros and Cons

Pros:

- No extra dependencies
- No extra artifact to ship
- Fully connection-string driven

Cons:

- Highest telemetry-specific code ownership in the core driver
- Driver must keep up with OTLP schema expectations
- Driver owns retry, batching, and serialization details itself


> **Implementation details.** For example code, deep walkthroughs, deployment examples, and debugging guidance for Solution 2, see [Appendix D.4: Solution 2 Implementation Details](#d4-solution-2-implementation-details).

<a id="43-solution-3-jul-file-to-collector-filelog"></a>
### 4.3 Solution 3: JUL File to Collector `filelog`

Solution 3 removes OTLP export from the driver entirely. The driver emits structured JUL log records; the collector turns those records into metrics.

#### 4.3.1 User Experience

- User-visible properties remain the same in the proposal, but the implementation uses them only to enable the feature and tag the emitted logs.
- No OpenTelemetry dependency is needed in the driver.
- A collector deployment is required.

#### 4.3.2 Runtime Flow

```
JDBC work completes
  -> PerformanceLog.Scope closes
  -> existing PerformanceLog JUL emission path writes record
  -> collector filelog receiver tails the file
  -> collector parser extracts metric fields
  -> collector exporter sends OTLP onward
```

#### 4.3.3 Callback Fit

The callback still remains the central hook.

- The driver already has integrated JUL performance logging through `PerformanceLog`; Solution 3 reuses that existing path.
- No new JUL-specific callback implementation is required for this solution.
- On each event, the existing performance logger emits JUL records that the collector tails and parses.
- The driver does not create OTLP records and does not know about OpenTelemetry instrument types.
- The collector configuration becomes the place where semantic mapping lives.

#### 4.3.4 Driver Responsibilities

- Ensure performance logging is enabled when this solution is active
- Emit a deterministic structured format suitable for parsing
- Include enough fields to reconstruct metrics later: timestamp, activity, connection ID, statement ID, duration, server stats fields, and error type

#### 4.3.5 Collector Responsibilities

- parse the log line
- map parsed fields to metric names and types
- attach resource attributes
- export to any backend

Example collector shape:

```yaml
receivers:
  filelog:
    include: [/var/log/jdbc/performance.log]
    operators:
      - type: regex_parser
        regex: '^(?<ts>[^ ]+) (?<activity>[^ ]+) conn=(?<conn>\d+) stmt=(?<stmt>\d+) value=(?<value>\d+)$'

exporters:
  otlp:
    endpoint: otel-collector:4317
```

#### 4.3.6 Pros and Cons

Pros:

- No OTLP serialization in the driver
- No OTel SDK dependency in the driver
- Very flexible for ops-managed environments
- Easy to inspect raw records during troubleshooting

Cons:

- Requires a collector and parsing configuration
- Data quality depends on parser correctness
- Metrics hit disk before export


> **Implementation details.** For example code, deep walkthroughs, deployment examples, and debugging guidance for Solution 3, see [Appendix D.5: Solution 3 Implementation Details](#d5-solution-3-implementation-details).

<a id="44-solution-4-direct-opentelemetry-dependency-in-core-driver"></a>
### 4.4 Solution 4: Direct OpenTelemetry Dependency in Core Driver

Solution 4 is a variant of Solution 1 that eliminates the companion JAR and the
`ServiceLoader<TelemetryBridgeFactory>` SPI. Instead, `opentelemetry-api` (and
optionally `opentelemetry-sdk` plus `opentelemetry-exporter-otlp`) become
**direct, non-optional compile dependencies of `mssql-jdbc.jar`**. The OTel
bridge code lives inside the driver and runs unconditionally.

This is the simplest design from the application developer's point of view -
the driver "just works" with `otelEndpoint=...` and no companion artifact is
required - at the cost of permanently coupling the driver's classpath to the
OpenTelemetry Java libraries.

#### 4.4.1 User Experience

Application Maven dependency:

```xml
<dependency>
  <groupId>com.microsoft.sqlserver</groupId>
  <artifactId>mssql-jdbc</artifactId>
  <version>13.x.x</version>
</dependency>
```

That is the entire setup. No companion artifact, no javaagent, no SPI JAR.
OpenTelemetry API/SDK/exporter are pulled in transitively by `mssql-jdbc`.

Connection string:

```text
jdbc:sqlserver://host;serverExecutionStatistics=all;otelEndpoint=http://collector:4318/v1/metrics
```

Application code is unchanged from any of the other solutions - the driver
publishes metrics to the configured endpoint as soon as it sees `otelEndpoint`.

##### Coexistence With an Application-Owned OTel SDK

If the application already configures `GlobalOpenTelemetry` (for example via
`opentelemetry-javaagent` or `applicationinsights-agent`), the driver detects
the existing `OpenTelemetry` instance and reuses it (same `Meter`,
`MeterProvider`, resource attributes, export pipeline). If no global instance
is present, the driver auto-configures a minimal `SdkMeterProvider` with an
`OtlpHttpMetricExporter` pointed at `otelEndpoint`.

This dual mode is what makes Solution 4 viable: the driver does not fight the
host application's telemetry stack.

#### 4.4.2 Architecture & Components

```
mssql-jdbc.jar  (single artifact, OTel baked in)
  +-- com.microsoft.sqlserver.jdbc.PerformanceLog
  +-- com.microsoft.sqlserver.jdbc.telemetry.OpenTelemetryPerformanceCallback
  |     - imports io.opentelemetry.api.metrics.*
  |     - imports io.opentelemetry.sdk.* (only on auto-config path)
  +-- com.microsoft.sqlserver.jdbc.telemetry.OtelBootstrap
        - resolves GlobalOpenTelemetry vs. auto-configured SDK
        - registers the callback on PerformanceLog
```

Compared to Solution 1:

- No `TelemetryBridgeFactory` SPI, no `ServiceLoader` lookup.
- No `mssql-jdbc-opentelemetry` / `mssql-jdbc-opentelemetry-all` artifacts.
- `OpenTelemetryPerformanceCallback` is part of the driver's own package.

#### 4.4.3 Class Inventory (New)

| Class | Purpose |
|---|---|
| `OtelBootstrap` | One-time init triggered on first `Connection` with `otelEndpoint` set. Picks `GlobalOpenTelemetry.get()` or builds an autoconfigured SDK. |
| `OpenTelemetryPerformanceCallback` | Implements `PerformanceLogCallback`; maps events to `LongHistogram` / `LongCounter` / attributes. |
| `OtelMetricCatalog` | Holds the singleton `Meter` and pre-built instrument handles (`db.client.execute.duration`, `db.server.cpu_time`, `db.server.io.logical_reads`, etc.). |
| `OtelAttributeBuilder` | Centralizes attribute keys (`db.system`, `db.connection_id`, `db.statement.kind`, `server.address`, ...). |

#### 4.4.4 Initialization & Lifecycle

1. First `SQLServerConnection` whose URL contains `otelEndpoint` calls
   `OtelBootstrap.ensureInitialized(props)`.
2. `OtelBootstrap`:
   - If `GlobalOpenTelemetry.get()` is non-noop, use it.
   - Else build `SdkMeterProvider` with `PeriodicMetricReader` +
     `OtlpHttpMetricExporter(otelEndpoint, otelHeaders, gzip)` and set as
     global via `OpenTelemetrySdk.builder().setMeterProvider(...).buildAndRegisterGlobal()`.
3. Instruments are created lazily on the shared `Meter` and cached in
   `OtelMetricCatalog`.
4. `OpenTelemetryPerformanceCallback` is added to `PerformanceLog` exactly
   once (idempotent).
5. On driver shutdown, the auto-configured SDK is flushed and closed via a
   JVM shutdown hook; an app-provided SDK is left alone.

#### 4.4.5 Metric Mapping

Identical to Solution 1:

| OTel Instrument | Source Event | Unit | Attributes |
|---|---|---|---|
| `db.client.execute.duration` (LongHistogram) | EXECUTE | ms | `db.system`, `db.connection_id`, `db.statement.kind` |
| `db.client.prepare.duration` (LongHistogram) | PREPARE | ms | same |
| `db.server.elapsed_time` (LongHistogram) | TDS_EXECSTATS | ms | same + `server.address` |
| `db.server.cpu_time` (LongHistogram) | TDS_EXECSTATS | ms | same |
| `db.server.io.logical_reads` (LongCounter) | TDS_EXECSTATS | reads | same |
| `db.server.io.physical_reads` (LongCounter) | TDS_EXECSTATS | reads | same |
| `db.server.io.writes` (LongCounter) | TDS_EXECSTATS | writes | same |
| `db.client.errors` (LongCounter) | EXECUTE (failed) | errors | same + `error.type` |

#### 4.4.6 Configuration

All `otel*` connection properties from the shared property table apply:

- `otelEndpoint` - required, switches the bootstrap on.
- `otelHeaders` - merged into the exporter when the driver auto-configures.
- `otelServiceName`, `otelResourceAttributes` - merged into the SDK resource on
  the auto-config path; ignored when the host application owns the SDK.
- `otelExportInterval`, `otelBatchSize` - drive the auto-configured
  `PeriodicMetricReader`.
- `otelSamplingRate` - applied as a `View` on the histograms.

#### 4.4.7 Error Handling & Backpressure

- Exporter failures are logged via JUL at `WARNING` and never propagated to
  the JDBC call path. The OTel SDK handles its own bounded queue and drop
  policy.
- If `OtelBootstrap` fails (for example, classpath missing an OTel class
  because the user excluded it), the driver logs `SEVERE` once and disables
  the callback - JDBC calls continue to work.

#### 4.4.8 Threading Model

- The OTel SDK owns the export thread (single daemon thread from
  `PeriodicMetricReader`).
- Instrument record calls (`histogram.record(...)`, `counter.add(...)`) are
  lock-free on the hot path. No additional driver-owned threads are
  introduced.

#### 4.4.9 Security Considerations

- Adding OTel as a direct dependency expands the driver's transitive surface.
  Every CVE in `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp`,
  or their transitive deps (e.g. `okhttp`, `protobuf-java`) becomes a CVE in
  `mssql-jdbc`.
- `otelHeaders` may contain bearer tokens; same redaction rules as the other
  solutions apply (never echoed in logs).
- TLS for the OTLP endpoint is delegated to the OTel SDK's `OkHttpSender`.

#### 4.4.10 Testing

- Unit tests stub `OpenTelemetry` via `OpenTelemetrySdk.builder().build()` +
  `InMemoryMetricReader` from `opentelemetry-sdk-testing`.
- Integration test uses a local OTel Collector container (same fixture as
  Solution 1).
- Compatibility test asserts that when the app sets a custom
  `GlobalOpenTelemetry`, the driver reuses it and does **not** register a
  second SDK.

#### 4.4.11 Pros / Cons / Trade-offs

Pros:

- Simplest user experience - one artifact, one connection string property.
- No SPI plumbing or `ServiceLoader` mechanics in the core driver.
- Uses the OTel SDK's well-tested export pipeline (batching, retry, gzip,
  TLS, mTLS, queue backpressure).
- Works out of the box with `opentelemetry-javaagent` and
  `applicationinsights-agent` because both publish a `GlobalOpenTelemetry`
  that the driver will detect and reuse.

Cons:

- **Permanently changes `mssql-jdbc`'s dependency contract.** Every consumer
  of the driver now transitively pulls OpenTelemetry (currently ~3 MB across
  `api` + `sdk` + `exporter-otlp` + `okhttp`/`protobuf`).
- Risk of version conflicts when the host application also depends on a
  different OTel version (the driver would need to track OTel API
  compatibility carefully, similar to how Hibernate handles JPA versions).
- CVE blast-radius grows: every OTel/transport vulnerability becomes an
  `mssql-jdbc` advisory.
- Cannot be opted out of - even applications that never set `otelEndpoint`
  carry the OTel classes on their classpath.
- Diverges from the historical "single jar, zero runtime deps" posture of
  the Microsoft JDBC driver.

#### 4.4.12 Relationship to Solution 1

Solution 4 is structurally Solution 1 with the SPI removed and the bridge
classes promoted into the core driver:

| Aspect | Solution 1 | Solution 4 |
|---|---|---|
| OTel API on driver classpath | Optional (companion JAR) | Always |
| OTel SDK on driver classpath | Path C only | Always (or reuses global) |
| Discovery mechanism | `ServiceLoader<TelemetryBridgeFactory>` | Direct instantiation |
| Number of published artifacts | 3 (`mssql-jdbc`, `-opentelemetry`, `-opentelemetry-all`) | 1 |
| Driver works without OTel on classpath | Yes (degrades) | No (compile-time required) |
| Easiest path for app developers | No - requires picking a path | Yes |
| Cleanest dependency posture for driver | Yes | No |

Selection between Solution 1 and Solution 4 is essentially a policy decision
about whether `mssql-jdbc` is willing to take a permanent dependency on
OpenTelemetry.


> **Implementation details.** For example code, deep walkthroughs, deployment examples, and debugging guidance for Solution 4, see [Appendix D.6: Solution 4 Implementation Details](#d6-solution-4-implementation-details).

<a id="45-cross-solution-comparison"></a>
### 4.5 Cross-Solution Comparison

| Criterion | Solution 1: Optional OTel Bridge | Solution 2: Built-In OTLP/HTTP JSON | Solution 3: JUL Filelog | Solution 4: Direct OTel Dependency |
|---|---|---|---|---|
| Core driver owns OTel mapping | No | Yes (custom) | No | Yes (via OTel API) |
| Core driver owns OTLP wire format | No | Yes | No | No (OTel SDK does it) |
| Extra dependency required by app | Yes, optional artifact | No | No | No (transitive) |
| OTel libs on driver classpath | Optional | Never | Never | Always |
| Extra runtime component required | No | No | Yes, collector | No |
| Single Maven coordinate for app | No | Yes | Yes | Yes |
| Cleanest architecture | Best | Moderate | Good | Moderate |
| Smallest driver jar / dependency surface | Best | Best | Best | Worst |
| Easiest single-jar deployment | No | Best | No | Best (but heavy) |
| Best for ops-managed pipelines | Moderate | Moderate | Best | Moderate |
| Reuses app's existing OTel SDK | Yes (Paths A/B) | No | No | Yes (auto-detect) |

Selection guidance:

- Choose Solution 1 if the priority is a clean boundary and avoiding telemetry protocol code in the core driver, while still letting users opt in.
- Choose Solution 2 if the priority is zero extra dependencies in a single jar.
- Choose Solution 3 if the priority is collector-centric operational ownership.
- Choose Solution 4 if the priority is the simplest possible developer experience (one coordinate, one property) and the team is willing to take a permanent OpenTelemetry dependency in `mssql-jdbc`.

### 4.6 Optional Future Coexistence Model (Post-Selection)

The initial implementation should select **one** solution to minimize complexity and reduce rollout risk. After that baseline is stable, coexistence can be considered as a follow-on enhancement.

If coexistence is introduced later, it should be explicit and deterministic:

- Add a dedicated selector property (for example, `otelSolution=bridge|http|jul`) instead of inferring behavior from endpoint shape.
- Keep the existing connection string properties unchanged for per-solution configuration.
- Enforce exactly one active export path per connection.
- Log the selected solution at connection initialization for diagnosability.

This avoids ambiguity while preserving a migration path to support multiple solution modes over time.

### 4.7 Azure Monitor Application Insights Integration (Applies to All Solutions)

> **Positioning:** Azure Monitor Application Insights is a deployment target, not a solution. The Azure Monitor Java agent exposes a local OTLP endpoint (`http://localhost:8888/v1/metrics`) that ingests OpenTelemetry data from any source — including the JDBC driver via **Solution 1** (OTel SDK exporter targets that endpoint), **Solution 2** (`otelEndpoint` points to that endpoint), or **Solution 3** (collector pipeline forwards there). This section describes the deployment pattern once; it applies regardless of which solution is selected.

#### 4.7.1 What is Azure Monitor Application Insights?

**Azure Monitor Application Insights** is Microsoft's Application Performance Management (APM) service in Azure. It ingests OpenTelemetry metrics, traces, logs, and exceptions from applications, providing:

- Real-time performance monitoring
- Distributed trace correlation across services
- Custom metrics and dimensions
- Live metrics stream (low-latency view)
- Automatic dependency tracking (SQL, HTTP, etc.)
- Failure analysis and alerting

#### 4.7.2 Azure Monitor Java Agent (Local OTEL Agent)

The **Azure Monitor Java Agent** is a Java-specific OpenTelemetry implementation (via Java bytecode instrumentation). It runs locally in your application JVM and:

1. **Collects telemetry** automatically (requests, dependencies, logs, metrics, exceptions)
2. **Buffers data** in memory (similar to our MetricsBuffer concept)
3. **Exports to Azure** via OTLP/HTTP protocol (to `localhost:8888` by default)
4. **Never modifies** your application code (zero-code instrumentation)

**Key distinction:** The Azure agent does the heavy lifting (auto-instrumentation); your JDBC driver metrics are just one data source it ingests.

#### 4.7.3 Architecture: JDBC Driver → Azure Agent → Azure Monitor

```
┌──────────────────────────────────────┐
│     Your Java Application            │
│                                      │
│  ┌─────────────────────────────────┐ │
│  │   JDBC Driver                   │ │
│  │  (Our new OTLP exporter)        │ │
│  │                                 │ │
│  │  MetricsBuffer                  │ │
│  │   ├─ db.client.*.duration       │ │
│  │   ├─ db.server.io.logical_reads │ │
│  │   └─ (all our metrics)          │ │
│  └─────────┬───────────────────────┘ │
│            │                         │
│            │ POST /v1/metrics        │
│            │ (gzipped OTLP JSON)     │
│            ▼                         │
│  ┌─────────────────────────────────┐ │
│  │ Application Insights Java Agent │ │
│  │  (Local, in-process)            │ │
│  │                                 │ │
│  │ -javaagent:                     │ │
│  │  applicationinsights-agent.jar  │ │
│  │                                 │ │
│  │ + Auto-instruments:             │ │
│  │   - HTTP requests               │ │
│  │   - SQL queries                 │ │
│  │   - Logs                        │ │
│  │   - Exceptions                  │ │
│  │   - Our JDBC metrics (via POST) │ │
│  │                                 │ │
│  │ Configuration:                  │ │
│  │  applicationinsights.json or    │ │
│  │  env vars                       │ │
│  └─────────┬───────────────────────┘ │
│            │                         │
└────────────┼─────────────────────────┘
             │
             │ OTLP/HTTP
             │ (all telemetry combined)
             │
             â–¼
    ┌─────────────────────┐
    │  Azure Monitor      │
    │  Application        │
    │  Insights           │
    │  (Cloud endpoint)   │
    │                     │
    │  - Ingests metrics  │
    │  - Stores in DB     │
    │  - Dashboards       │
    │  - Alerts           │
    └─────────────────────┘
```

**Key insight:** The Azure agent **collects our JDBC metrics via HTTP POST** (we push to it), then **re-exports everything together** to Azure as one cohesive telemetry stream.

#### 4.7.4 Setup: 5 Simple Steps

##### Step 1: Download the Agent JAR

```bash
cd /opt/javaagent  # or your preferred location

wget https://github.com/microsoft/ApplicationInsights-Java/releases/download/3.7.8/applicationinsights-agent-3.7.8.jar
```

##### Step 2: Create Configuration File

```bash
cat > /opt/javaagent/applicationinsights.json << 'EOF'
{
  "connectionString": "InstrumentationKey=YOUR_KEY;IngestionEndpoint=https://YOUR_REGION.in.applicationinsights.azure.com/;LiveEndpoint=https://YOUR_REGION.livediagnostics.monitor.azure.com/",
  "role": {
    "name": "order-service"
  },
  "sampling": {
    "percentage": 100
  },
  "instrumentation": {
    "jdbc": {
      "enabled": true
    }
  },
  "selfDiagnostics": {
    "level": "INFO",
    "destination": "file",
    "file": {
      "path": "applicationinsights.log"
    }
  }
}
EOF
```

##### Step 3: Get Connection String from Azure

In **Azure Portal** → **Application Insights resource** → **Overview**:
- Copy `Connection String`
- Paste into `applicationinsights.json`

##### Step 4: Add JVM Argument

Start your Java application with:

```bash
java -javaagent:/opt/javaagent/applicationinsights-agent-3.7.8.jar \
     -cp your-app.jar:. \
     com.example.YourMainClass
```

Or in your `application.properties` (Spring Boot):

```properties
spring.application.name=order-service
JAVA_OPTS=-javaagent:/opt/javaagent/applicationinsights-agent-3.7.8.jar
```

##### Step 5: Enable JDBC Metrics in JDBC Connection String

```
jdbc:sqlserver://mydb.database.windows.net:1433;
  databaseName=mydb;
  serverExecutionStatistics=all;
  otelEndpoint=http://localhost:8888/v1/metrics;
  otelExportInterval=10;
  otelServiceName=order-service
```

The Azure agent listens on `localhost:8888/v1/metrics` by default and ingests our JDBC metrics automatically.

#### 4.7.5 Configuration Deep Dive

**Key `applicationinsights.json` settings:**

| Setting | Example | Purpose |
|---------|---------|---------|
| `connectionString` | `InstrumentationKey=..;IngestionEndpoint=..` | Where to send telemetry (Azure endpoint) |
| `role.name` | `"order-service"` | Service identifier in Application Map |
| `sampling.percentage` | `100` | Keep all data (0–100). Reduce to save cost. |
| `instrumentation.jdbc.enabled` | `true` | Auto-instrument SQL queries |
| `selfDiagnostics.level` | `INFO` | Agent's own logging (`DEBUG` for troubleshooting) |
| `jmxMetrics` | See below | Custom JVM metrics (GC, memory, threads) |

**Optional: Collect Custom JMX Metrics**

```json
{
  "jmxMetrics": [
    {
      "name": "JVM Uptime",
      "objectName": "java.lang:type=Runtime",
      "attribute": "Uptime"
    },
    {
      "name": "Active Threads",
      "objectName": "java.lang:type=Threading",
      "attribute": "ThreadCount"
    },
    {
      "name": "Heap Memory Used",
      "objectName": "java.lang:type=Memory",
      "attribute": "HeapMemoryUsage.used"
    }
  ]
}
```

#### 4.7.6 How Azure Agent Receives Our JDBC Metrics

The Azure agent runs an **internal HTTP listener** on `localhost:8888`:

```
Timeline:

t=0s   JDBC Driver initializes
       └─ InternalPerformanceCollector setup
          └─ OtlpHttpExporter target: http://localhost:8888/v1/metrics

t=5s   SQL query executes
       └─ MetricsBuffer enqueues 10 data points

t=15s  ExportScheduler fires
       └─ OtlpHttpExporter.export() sends:
          POST http://localhost:8888/v1/metrics
          Content-Type: application/json
          Content-Encoding: gzip
          Body: { resourceMetrics: [...] }  ◄── JDBC metrics

t=15s  Azure Agent receives on localhost:8888
       └─ Parses OTLP JSON
       └─ Correlates with existing trace context
       └─ Merges with other auto-collected telemetry
          ├─ HTTP requests (from server framework)
          ├─ SQL queries (from JDBC instrumentation)
          ├─ Logs (from log4j/logback)
          ├─ Our JDBC metrics (from POST)
          └─ Exceptions (auto-caught)

t=16s  Azure Agent batches all telemetry
       └─ Compresses again (gzip)
       └─ Posts to Azure Monitor Cloud endpoint
          POST https://westus2.in.applicationinsights.azure.com/v2/track
          Authorization: InstrumentationKey=...

t=17s  Azure Monitor stores
       └─ Metrics table
       └─ Traces table
       └─ Logs table
       └─ Exceptions table
```

**Key detail:** Azure agent receives our OTLP JSON on `localhost:8888`, then **re-serializes it to Azure's native format** before uploading to the cloud. This is transparent to us.

#### 4.7.7 Viewing JDBC Metrics in Azure Portal

After metrics start flowing, navigate in **Azure Portal**:

1. **Application Insights Resource** → **Metrics** (left sidebar)
2. **Metric Namespace:** `Log-based metrics`
3. **Metric:** Search for `db.client.statement.execute.duration` or `db.server.io.logical_reads`
4. **Add dimensions:** Group by `db.connection.id`, `db.operation.name`, `server.address`

**Example query (KQL - Kusto Query Language):**

```kusto
customMetrics
| where name startswith "db."
| where tostring(customDimensions.db_operation_name) == "SELECT"
| summarize AvgDuration = avg(value) by bin(timestamp, 1m), tostring(customDimensions.server_address)
| render timechart
```

**Or via Live Metrics:**

Application Insights → **Live Metrics** → See requests, dependencies, performance counters, and custom metrics in real-time (2-3 second latency).

#### 4.7.8 Trace Context Correlation

The Azure agent automatically correlates telemetry:

```
HTTP Request: GET /api/orders
  └─ TraceId: 4bf92f3577b34da6a3ce929d0e0e4736
     ├─ Span: HTTP handler (10ms)
     │  └─ SQL Query 1: SELECT * FROM orders WHERE status=?
     │     ├─ TraceId: 4bf92f3577b34da6a3ce929d0e0e4736 (same)
     │     ├─ SpanId: child of HTTP span
     │     ├─ Client time: 5ms
     │     ├─ Server stats: 200 logical reads, 2ms CPU
     │     └─ Dependency: JDBC (Azure auto-instrumented)
     │
     │  └─ SQL Query 2: SELECT * FROM order_items WHERE order_id=?
     │     ├─ TraceId: 4bf92f3577b34da6a3ce929d0e0e4736 (same)
     │     ├─ SpanId: sibling of Query 1
     │     └─ (correlated via trace ID)
     │
     └─ Log: "Order processing complete" (INFO level)
        └─ TraceId: 4bf92f3577b34da6a3ce929d0e0e4736 (same)
```

All these telemetry items show up in Azure with the same `TraceId`, allowing drill-down from a single HTTP request → all its SQL queries → their metrics → performance insights.

#### 4.7.9 Sampling for Cost Control

Azure Monitor charges by data ingestion volume. Use sampling to reduce:

```json
{
  "sampling": {
    "percentage": 10
  }
}
```

This samples 10% of traces (keeping 1 in 10). Combined with our JDBC driver's `otelSamplingRate`:

```
Total sampling = JDBC sampling × Azure agent sampling

Example: otelSamplingRate=0.5 × Azure sampling=0.1 = 0.05 (5% of metrics)
```

**Sampling gotchas:**
- ✅ Aggregation (histograms, percentiles) still works on sampled data
- ❌ Cannot see every individual slow query (but alerts still work)
- ✅ Cost reduction: 10x fewer data points = 10x lower ingestion costs

#### 4.7.10 Troubleshooting: Metrics Not Appearing

**Check 1: Agent logs**

```bash
cat applicationinsights.log | grep -i "jdbc\|metric\|error"
```

Look for:
- ✅ `Connected to ingestion endpoint` → Agent reached Azure
- ✅ `Auto-instrumentation of type JDBC is enabled` → JDBC tracking active
- ❌ `Failed to send telemetry` → Network/auth issue
- ❌ `Invalid connection string` → Check InstrumentationKey

**Check 2: JDBC driver logs**

```java
java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver");
logger.setLevel(java.util.logging.Level.FINE);
```

Look for:
- `OTLP export: X data points` → Driver sending metrics
- `Connection refused` → Azure agent not listening on 8888
- `HTTP 401` → Auth header missing/invalid

**Check 3: Network connectivity**

```bash
# Test if Azure agent is listening
curl http://localhost:8888/v1/metrics -X POST -H "Content-Type: application/json" -d '{}' -v

# Should return HTTP 202 Accepted (or 200 OK)
```

**Check 4: Azure Portal**

Go to **Application Insights** → **Logs** (Kusto Query):

```kusto
customMetrics
| where name startswith "db."
| summarize Count=count() by name
| top 10 by Count desc
```

No results? Check if data is flowing for standard metrics first:

```kusto
requests  // Should show HTTP requests
| summarize Count=count() by bin(timestamp, 5m)
```

<a id="5-connection-string-properties-summary"></a>
## 5. Connection String Properties Summary

| Property | Type | Default | Part | Description |
|----------|------|---------|------|-------------|
| `serverExecutionStatistics` | String | `"false"` | I | Request server-side execution stats. Values: `false`, `io`, `time`, `all` |
| `otelEndpoint` | String | `""` | II | OTLP/HTTP endpoint URL. Empty = disabled |
| `otelExportInterval` | Int | `60` | II | Export interval in seconds (5–3600) |
| `otelBatchSize` | Int | `1000` | II | Trigger export when buffer reaches N data points (whichever comes first: time or size) |
| `otelSamplingRate` | Double | `1.0` | II | Sample metrics: 0.1 = 10%, 1.0 = all (no sampling). For high-volume workloads only |
| `otelServiceName` | String | `"mssql-jdbc"` | II | OTLP `service.name` resource attribute |
| `otelHeaders` | String | `""` | II | Extra HTTP headers for OTLP endpoint (comma-separated `key=value` pairs) |
| `otelResourceAttributes` | String | `""` | II | Extra OTLP resource attributes (comma-separated `key=value` pairs) |

**All properties follow existing driver patterns:** defined in `SQLServerDriverStringProperty`/`SQLServerDriverIntProperty`, accessed via `activeConnectionProperties`, validated during connection setup.

---

<a id="6-security-considerations"></a>
## 6. Security Considerations

| Risk | Mitigation |
|------|------------|
| **OTLP endpoint credential leakage** | `otelHeaders` values are never logged; marked as sensitive in connection property metadata |
| **Query text in metrics** | Only `db.operation.name` (first SQL keyword) is exported — never full query text or parameters |
| **SSRF via otelEndpoint** | Endpoint URL is validated at connection time (must be `http://` or `https://`); no redirects followed |
| **Denial-of-service (unbounded metrics)** | `MetricsBuffer` is bounded (default max 10K data points); oldest entries are auto-evicted when full |
| **TLS for OTLP export** | Follows JVM trust store by default; `https://` endpoints use TLS |
| **Server stats data exposure** | Server stats contain only performance counters — no PII, no query text, no data values |
| **Thread safety** | `MetricsBuffer` uses `ConcurrentLinkedQueue`; `ExportScheduler` is a single thread (Solution 2). Solution 4 uses the OTel SDK's lock-free instrument record path. |
| **Transitive CVE surface (Solution 4)** | OTel API, SDK, exporter, OkHttp, and protobuf become direct dependencies of `mssql-jdbc`. Every CVE in those libraries becomes an `mssql-jdbc` advisory. Mitigation: pin to the most recent stable OTel BOM and track its security advisories in the driver release notes. |
| **Dependency version conflict (Solution 4)** | App's OTel version may differ from the driver's. Mitigation: the driver's bridge code uses only the stable `opentelemetry-api` surface (no SDK-internal types) so minor mismatches are tolerated; pin the OTel BOM in the app's `<dependencyManagement>` to override. |
| **Inherited exporter trust (Solution 4)** | When the driver reuses `GlobalOpenTelemetry`, it inherits the app's exporter, TLS trust store, and outbound URL allow-list. This is the desired behavior, but means a misconfigured app SDK can affect where driver metrics are sent. Mitigation: document that `otelEndpoint`, `otelHeaders`, and `otelServiceName` are ignored when a global SDK is already registered. |
| **Classpath shadowing (Solution 4)** | A malicious or shaded `io.opentelemetry.*` class on the application classpath could be picked up by the driver. Mitigation: same as any other transitive dependency - rely on the build system's signature checks; do not relocate OTel packages inside the driver jar. |
| **`otelEndpoint` SSRF / TLS (Solutions 1, 2, 4)** | Solution 2 validates the scheme directly (`http://` or `https://`); Solutions 1 and 4 delegate validation and TLS to the OTel SDK's `OtlpHttpMetricExporter`, which honors the JVM trust store and rejects non-HTTP schemes. |
| **File path traversal (Solution 3)** | The JUL `FileHandler` pattern is taken from a property; the driver does not concatenate user input into the path. Mitigation: documented allow-list of placeholders (`%h`, `%g`, etc.). |

---

<a id="7-testing-strategy"></a>
## 7. Testing Strategy

Unless otherwise noted, the detailed exporter-specific tests below apply primarily to Solution 2 (the built-in OTLP/HTTP JSON exporter). Solution 1 reuses OpenTelemetry SDK exporter behavior, and Solution 3 shifts most parsing/export validation to collector configuration tests.

### Unit Tests

| Test Area | Approach |
|-----------|----------|
| `StreamExecStats` parsing | Feed crafted TDS byte sequences; verify deserialization into `ServerExecutionStats` |
| `MetricsBuffer` | Concurrent data point insertion; FIFO queueing; eviction when full (max 10K default) |
| `MetricDataPoint` creation | Verify attributes are captured correctly; timestamp precision |
| `OtlpJsonFormatter` | Attribute grouping logic; gzip compression; validation against OTLP schema |
| `OtlpHttpExporter` | HTTP POST with gzip, header injection, timeout handling |
| `InternalPerformanceCollector` sampling | Sampling rate application; uniform distribution of samples |
| `ExportScheduler` triggers | Time-based trigger (timer); size-based trigger (buffer reaches threshold) |
| `PerformanceLog` dual callback | Register both user and internal callbacks; verify both receive events |
| Connection string validation | Invalid `otelEndpoint`, out-of-range `otelExportInterval`/`otelBatchSize`, sampling rate 0.0-1.0 |

### Integration Tests

| Test Area | Approach |
|-----------|----------|
| Feature negotiation | Connect to SQL Server (when engine support available); verify stats are received |
| Fallback behavior | Connect to older SQL Server that doesn't support the feature; verify clean degradation |
| OTLP export (Solution 2) | Start local HTTP server; enable `otelEndpoint`; execute queries; verify gzipped OTLP JSON payloads received |
| OTel bridge activation (Solution 1) | Add optional bridge artifact; enable `otelEndpoint`; verify callback events are translated into OTel instrument recordings |
| JUL filelog pipeline (Solution 3) | Emit structured JUL records; run collector parser; verify expected metrics are produced |
| Batching | Execute 100 statements; verify single OTLP POST with all metrics grouped by name/attributes |
| Sampling | Set `otelSamplingRate=0.1`; execute 1000 statements; verify ~100 metrics exported (~10%) |
| Gzip effectiveness | Measure payload size uncompressed vs gzipped; verify ~80% reduction for typical workloads |
| End-to-end | Enable both features; execute mixed workload; verify combined client+server metrics in OTLP output |
| Performance impact | Benchmark with/without features enabled; validate < 1% overhead target; measure gzip CPU cost |
| High-volume workload | 10K statements/sec; verify size-based batching triggers; verify no memory leaks; measure GC impact |

### Mock Testing (Before SQL Server Engine Support)

Until the SQL Server engine implements `TDS_FEATURE_EXT_EXECSTATS`:

1. **Mock TDS responses** in unit tests with crafted `TDS_EXECSTATS` token bytes
2. **Proxy-based integration tests** using a TDS proxy that injects synthetic `TDS_EXECSTATS` tokens into the response stream
3. **Simulated stats** via an internal test hook that pushes `ServerExecutionStats` objects directly into the callback pipeline

---

<a id="8-overhead--optimization-strategies"></a>
## 8. Overhead & Optimization Strategies

The quantitative payload discussion in this section is most directly applicable to Solution 2, where the core driver owns batching, JSON payload construction, and HTTP transport. Solution 1 delegates those concerns to the OpenTelemetry SDK, and Solution 3 delegates them to the collector pipeline.

### Addressing Metric Payload Size Concerns

You're right to be concerned about payload overhead. Here's how we address it:

#### Strategy 1: Gzip Compression (Always Enabled)

OTLP/HTTP standard practice. Typical results:
- **50 KB JSON → 10 KB gzipped** (80% reduction)
- **320 KB JSON → 60 KB gzipped** (with attribute grouping + compression)
- Gzip CPU cost: **<1 ms per POST** on modern hardware

**Example:** 1000 statement metrics per 60-second window

```
Uncompressed:  320 KB  (4 bytes per data point value + 200 bytes attributes)
Gzipped:        64 KB  (with ~20% post-gzip ratio)
Bandwidth:   ~17 KB/min overhead  (negligible for typical networks)
```

#### Strategy 2: Attribute Grouping (Intelligent Batching)

Don't repeat attributes for every metric. Group data points by common attributes:

```json
{
  "name": "db.client.statement.execute.duration",
  "gauge": {
    "dataPoints": [
      { "value": 5.0, "attrs": { "connId": 100, "stmtId": 10 } },
      { "value": 6.0, "attrs": { "connId": 100, "stmtId": 11 } },
      ...  // 98 more points, most sharing same server/db/operation
    ]
  }
}
```

**Impact:** 50% reduction in repetition (attributes sent once at metric level, not per-point)

#### Strategy 3: Size-Based Batching (Smart Triggering)

Export when buffer reaches **1000 data points** OR **60 seconds**, whichever comes first.

| Workload | Behavior |
|----------|----------|
| **Low volume** (10 stmt/sec) | Waits full 60s, one POST = 600 metrics |
| **Medium volume** (100 stmt/sec) | Hits 1000 buffer size in ~10s, exports multiple times/min |
| **High volume** (1000 stmt/sec) | Exports ~100s of times/min, maintains bounded memory |

**Advantage:** Natural back-pressure on export frequency; high-volume apps don't accumulate unbounded buffers.

#### Strategy 4: Optional Sampling (High-Volume Workloads)

For extremely high throughput (>10K stmt/sec), enable sampling:

```
otelSamplingRate=0.1    // Export 10% of metrics
```

**Result:**
- 90% reduction in metrics volume
- Still statistically valid for percentiles, trends, alerting
- Individual executions not visible, but aggregates are accurate

**Typical overhead with all optimizations:**

| Workload | Frequency | Size/POST | Total/Min | Overhead |
|----------|-----------|-----------|-----------|----------|
| 100 stmt/sec | Every 10s | 1000 metrics, 12 KB | 6 POSTs, 72 KB | <0.1% |
| 1000 stmt/sec | Every 1s | 1000 metrics, 12 KB | 60 POSTs, 720 KB | ~1% |
| 10K stmt/sec (10% sampling) | Every 100ms | 1000 metrics, 12 KB | 600 POSTs, 7.2 MB | ~2% |

**Bottom line:**
- Default settings: **<0.5% overhead** for typical workloads (100–1000 stmt/sec)
- High-volume (10K+ stmt/sec): enable sampling to keep overhead <1%
- Network cost: **negligible** at 10–100 KB/min compressed

---

<a id="9-future-work"></a>
## 9. Future Work

| Item | Description |
|------|-------------|
| **OTLP Traces** | Export statement executions as OTLP spans (with parent/child relationships) — requires W3C TraceContext propagation |
| **OTLP/Protobuf** | Add optional Protobuf wire format for OTLP export (smaller payloads, faster serialization) — could use protobuf-lite |
| **Query plan stats** | Extend `StatsMask` bits 2+ to request estimated/actual row counts, query plan hash, etc. |
| **Wait statistics** | Request per-statement wait stats (similar to `sys.dm_exec_session_wait_stats` but per-statement) |
| **Prometheus endpoint** | Optional pull-based metrics endpoint for environments that prefer scraping |
| **Adaptive export** | Dynamically adjust export interval based on workload volume |
| **Metric filtering** | Allow users to configure which metrics are exported (reduce noise) via connection string or callback |

---

<a id="appendix-a-end-to-end-usage-example"></a>
## Appendix A: End-to-End Usage Example

```java
// Minimal setup — just connection string properties, no code changes needed for OTLP
String url = "jdbc:sqlserver://myserver.database.windows.net:1433;"
    + "databaseName=mydb;"
    + "serverExecutionStatistics=all;"           // Enable server stats
    + "otelEndpoint=http://otel-collector:4318/v1/metrics;"  // Enable OTLP
    + "otelServiceName=order-service;"
    + "otelExportInterval=30";

try (Connection conn = DriverManager.getConnection(url, user, password);
     PreparedStatement ps = conn.prepareStatement("SELECT * FROM orders WHERE id = ?")) {

    ps.setInt(1, 42);

    try (ResultSet rs = ps.executeQuery()) {
        while (rs.next()) {
            // process rows
        }
    }

    // Optional: programmatic access to server stats
    ServerExecutionStats stats = ((ISQLServerStatement) ps).getServerExecutionStats();
    if (stats != null) {
        System.out.println("Logical reads: " +
            stats.getIOStats().get(0).getLogicalReads());
        System.out.println("Server CPU time: " +
            stats.getExecutionTime().getCpuTimeMs() + " ms");
    }
}
// On connection close, remaining metrics are flushed to the OTLP endpoint
```

<a id="appendix-b-programmatic-callback-usage-without-otlp"></a>
## Appendix B: Programmatic Callback Usage (Without OTLP)

```java
// Register a single callback for both existing and server-stats events
SQLServerDriver.registerPerformanceLogCallback(new PerformanceLogCallback() {
    @Override
    public void publish(PerformanceActivity activity, int connectionId,
                        long durationMs, Exception exception) {
        // Connection-level metrics
        log.info("Connection {} - {}: {} ms", connectionId, activity, durationMs);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId,
                        int statementId, long durationMs, Exception exception) {
        // Client-side statement metrics
        log.info("Statement {}/{} - {}: {} ms",
                connectionId, statementId, activity, durationMs);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId,
                        int statementId, ServerExecutionStats stats) {
        // Server-side stats — rich structured data
        for (ServerIOStats io : stats.getIOStats()) {
            log.info("Table {}.{} - logical reads: {}, physical reads: {}",
                    io.getSchemaName(), io.getTableName(),
                    io.getLogicalReads(), io.getPhysicalReads());
        }
        log.info("Server execution CPU: {} ms, elapsed: {} ms",
                stats.getExecutionTime().getCpuTimeMs(),
                stats.getExecutionTime().getElapsedTimeMs());
    }
});

// Just enable server stats — no OTLP
String url = "jdbc:sqlserver://host;serverExecutionStatistics=all";
```

<a id="appendix-c-architecture-diagrams"></a>
## Appendix C: Architecture Diagrams

The diagrams below show the data path for each solution. They share the
same Part I foundation (TDS feature negotiation, server stats arrival, the
`PerformanceLog` fan-out) and differ only in what happens *after* `PerformanceLog`
publishes an event.

### C.1 Shared Foundation (All Solutions)

```
                           Connection String
                           +-----------------------------------------+
                           | serverExecutionStatistics=all           |
                           | otelEndpoint=...   (or JUL config only) |
                           +----------------+------------------------+
                                            |
                    +-----------------------+-----------------------+
                    |              SQLServerConnection              |
                    |                                               |
                    |  LOGIN7  -->  TDS_FEATURE_EXT_EXECSTATS (0x11)|
                    |  <--  FEATUREEXTACK (server confirms support) |
                    +-----------------------+-----------------------+
                                            |
              +-----------------------------+----------------------------+
              |                             |                            |
              v                             v                            v
   +------------------+   +----------------------+   +----------------------+
   |   Client-Side    |   |    TDS Response      |   |   Server-Side        |
   |   Metrics        |   |    Stream            |   |   Statistics         |
   |                  |   |                      |   |                      |
   |  REQUEST_BUILD   |   |  COLMETADATA  ---->  |   |  TDS_EXECSTATS       |
   |  SERVER_RESPONSE |   |  ROW tokens   ---->  |   |  token (0xE5)        |
   |  EXECUTE         |   |  DONE         ---->  |   |  +----------------+  |
   |  PREPARE         |   |  TDS_EXECSTATS ---->  |   |  | Binary JSON   |  |
   |  PREPEXEC        |   |                      |   |  |  - IO stats   |  |
   +---------+--------+   +----------------------+   |  |  - Time stats |  |
             |                                       |  +----------------+  |
             |                                       +-----------+----------+
             |                                                   |
             +-------------------+-------------------------------+
                                 |
                                 v
                  +----------------------------+
                  |      PerformanceLog        |
                  |                            |
                  |  fans out every event to:  |
                  |   - user callback          |
                  |   - JUL handler (always)   |
                  |   - bridge/internal cb     |
                  |       (solution-specific)  |
                  +-------------+--------------+
                                |
                                v
                  ---- divergence point ----
                  | each solution takes a    |
                  | different path from here |
                  ----------------------------
```

### C.2 Solution 1: Optional OpenTelemetry Bridge (shared SPI)

```
                  PerformanceLog
                        |
                        v
   +-----------------------------------------------+
   |  TelemetryEvent payload                       |
   |  - activity / connection / statement ids      |
   |  - duration + measurement metadata            |
   |  - SQL / auth context / arbitrary attrs      |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  ServiceLoader<TelemetryBridge>               |
   |  (core driver, JDK SPI, no OTel imports)      |
   +-----------------------+-----------------------+
                           |
          +----------------+----------------+
          |                                 |
          |  provider present?              |
          |  yes                            |
          v                                 |
   +-----------------------------------------------+
   |  mssql-jdbc-telemetry-api (shared contract)   |
   |  - TelemetryBridge                            |
   |  - TelemetryEvent                             |
   |  - PerformanceActivity / StatementType       |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  mssql-jdbc-otel  (optional companion JAR)    |
   |                                               |
   |  OpenTelemetryTelemetryBridge                 |
   |   - creates spans / metrics from event data   |
   |   - applies generic attributes                |
   |   - preserves auth correlation metadata       |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  OpenTelemetry SDK / agent (runtime-provided) |
   |   - GlobalOpenTelemetry / javaagent /         |
   |     applicationinsights-agent / SDK           |
   |                                               |
   |  - Meter / Tracer / exporter wiring           |
   +-----------------------+-----------------------+
                           |
                           v
                +----------------------+
                |  OTLP/HTTP endpoint  |
                |  (collector / SaaS)  |
                +----------------------+

   Core driver depends only on the shared telemetry API.
   The OTel implementation lives in the companion module and is discovered
   lazily when present. If no provider is available, telemetry is skipped.
```

### C.3 Solution 2: Built-In OTLP/HTTP JSON Exporter

```
                  PerformanceLog
                        |
                        v
   +-----------------------------------------------+
   |  InternalPerformanceCollector                 |
   |  (registered when otelEndpoint is set;        |
   |   lives entirely inside mssql-jdbc.jar)       |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  MetricsBuffer  (ConcurrentLinkedQueue)       |
   |  enqueue(OtlpDataPoint)                       |
   +-----------------------+-----------------------+
                           |
              every otelExportInterval seconds
              OR when buffer reaches otelBatchSize
                           |
                           v
   +-----------------------------------------------+
   |  ExportScheduler  (single daemon thread)      |
   |   - drains MetricsBuffer                      |
   |   - groups data points by metric + attrs      |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  OtlpJsonFormatter  (hand-rolled, no deps)    |
   |   - StringBuilder -> OTLP JSON                |
   |   - gzip compression                          |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  OtlpHttpExporter                             |
   |   - java.net.HttpURLConnection                |
   |   - POST /v1/metrics                          |
   |   - Content-Type: application/json            |
   |   - Content-Encoding: gzip                    |
   |   - 1 retry on 5xx                            |
   +-----------------------+-----------------------+
                           |
                           v
                +----------------------+
                |  OTLP/HTTP endpoint  |
                |  (collector / SaaS)  |
                +----------------------+

   Driver owns the entire OTLP wire path. Zero external dependencies.
```

### C.4 Solution 3: JUL File to Collector `filelog`

```
                  PerformanceLog
                        |
                        v
   +-----------------------------------------------+
   |  java.util.logging.FileHandler                |
   |   - rotation: limit + count                   |
   |   - encoding: UTF-8                           |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  JdbcPerformanceJsonFormatter                 |
   |  single-line JSON per JDBC event              |
   |  {"ts":"...","activity":"...","conn":...,     |
   |   "stmt":...,"duration_ms":...,               |
   |   "server":{"logical_reads":...,...},         |
   |   "error":null,"service":"...",...}           |
   +-----------------------+-----------------------+
                           |
                           v
              +-----------------------------+
              |  /var/log/jdbc/performance.log |
              |  performance.log.1 ... .N      |  <-- rotated files
              +--------------+----------------+
                             |
                             |  collector tails by fingerprint
                             v
   +-----------------------------------------------+
   |  OpenTelemetry Collector                      |
   |   - filelog receiver  (json_parser)           |
   |   - count/sum connector  (log -> metric)      |
   |     or transform processor + logs->metrics    |
   |   - batch processor                           |
   |   - otlp exporter                             |
   +-----------------------+-----------------------+
                           |
                           v
                +----------------------+
                |  OTLP/HTTP endpoint  |
                |  (collector / SaaS)  |
                +----------------------+

   Driver writes structured logs only. Collector owns metric semantics.
```


### C.5 Solution 4: Direct OpenTelemetry Dependency in Core Driver

```
                  PerformanceLog
                        |
                        v
   +-----------------------------------------------+
   |  OpenTelemetryPerformanceCallback             |
   |  (inside mssql-jdbc.jar, always loaded)       |
   |   - LongHistogram (execute/prepare duration)  |
   |   - LongCounter   (logical/physical reads)    |
   |   - LongHistogram (server elapsed / cpu)      |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  OtelBootstrap (inside mssql-jdbc.jar)        |
   |                                               |
   |  if GlobalOpenTelemetry is configured:        |
   |     reuse it (app-owned SDK, e.g. javaagent)  |
   |  else:                                        |
   |     build SdkMeterProvider +                  |
   |          PeriodicMetricReader +               |
   |          OtlpHttpMetricExporter(otelEndpoint) |
   +-----------------------+-----------------------+
                           |
                           v
   +-----------------------------------------------+
   |  OpenTelemetry SDK                            |
   |  (compile-time dependency of mssql-jdbc.jar)  |
   |                                               |
   |  - SdkMeterProvider                           |
   |  - PeriodicMetricReader (buffer + interval)   |
   |  - OtlpHttpMetricExporter  (gzip, retry, TLS) |
   +-----------------------+-----------------------+
                           |
                           v
                +----------------------+
                |  OTLP/HTTP endpoint  |
                |  (collector / SaaS)  |
                +----------------------+

   No companion JAR, no ServiceLoader.
   OTel API + SDK + exporter are direct dependencies of mssql-jdbc.
   Same wire-path quality as Solution 1, paid for with a permanent
   OpenTelemetry dependency in the core driver.
```

<a id="appendix-d-implementation-details"></a>
## Appendix D: Implementation Details

This appendix collects the code-level walkthroughs, full Java examples,
configuration mechanics, real-world deployment recipes, and debugging
guidance for each of the four export solutions. The main body of Section 4
keeps only the decision-grade overview of each solution; readers who want
to see "how the code actually looks" should land here.

The first two subsections (D.1 and D.2) are cross-solution: the
implementation plan and the file change inventory. D.3 through D.6 are
per-solution deep dives - each is independent of the others, so you can
read D.3 without reading D.4/D.5/D.6 (and vice versa).


<a id="d1-implementation-plan"></a>
### D.1 Implementation Plan

(Moved from Section 6. Updated to include the Solution 4 track.)


#### Phase 1: Server-Side Statistics (Part I)

| Step | Work Item | Dependencies |
|------|-----------|--------------|
| 1.1 | Add `TDS_FEATURE_EXT_EXECSTATS` constant in `IOBuffer.java` | None |
| 1.2 | Add `serverExecutionStatistics` connection string property | None |
| 1.3 | Implement `writeExecStatsFeatureRequest()` in `SQLServerConnection.java` | 1.1, 1.2 |
| 1.4 | Wire feature request into `sendLogon()` two-pass pattern | 1.3 |
| 1.5 | Add feature ACK handling in `onFeatureExtAck()` | 1.3 |
| 1.6 | Create `ServerExecutionStats`, `ServerIOStats`, `ServerTimeStats` data classes | None |
| 1.7 | Create `StreamExecStats` token parser | 1.6 |
| 1.8 | Add `TDS_EXECSTATS` case to `tdsparser.java` parse loop | 1.7 |
| 1.9 | Override `onExecStats()` in `SQLServerStatement.NextResult` | 1.7, 1.8 |
| 1.10 | Add `SERVER_*` values to `PerformanceActivity` enum | None |
| 1.11 | Add server-stats `publish(...)` default overload to existing `PerformanceLogCallback` | 1.10 |
| 1.12 | Implement `publishServerStats()` in `SQLServerStatement` | 1.9, 1.10, 1.11 |
| 1.13 | Add `PerformanceLog.Scope.overrideDuration()` method | None |
| 1.14 | Add `getServerExecutionStats()` to `ISQLServerStatement` | 1.6 |
| 1.15 | Add error resource strings to `SQLServerResource.java` | None |
| 1.16 | Unit tests + integration tests | All above |

> **Note:** Steps 1.1–1.5 and 1.7–1.8 require a **corresponding SQL Server engine change** to implement the server-side token generation. Until the engine change is available, these steps can be stubbed/mocked for driver-side development.

#### Phase 2: Telemetry Export (Part II)

Common foundation (applies to all four solution tracks below; pick exactly one of A/B/C/D):

| Step | Work Item | Dependencies |
|------|-----------|--------------|
| 2.1 | Add OTLP connection string properties (endpoint, interval, batch size, sampling rate) | None |
| 2.2 | Modify `PerformanceLog` to support dual callbacks (user + internal) | None |
| 2.3 | Wire export lifecycle into `SQLServerConnection.connectInternal()` and `close()` | 2.1, 2.2 |

Solution 1 track: Optional OpenTelemetry bridge

| Step | Work Item | Dependencies |
|------|-----------|--------------|
| 2.4A | Define bridge SPI and discovery mechanism | 2.2 |
| 2.5A | Create optional `mssql-jdbc-opentelemetry` module | 2.4A |
| 2.6A | Implement bridge callback using OTel instruments and SDK exporters | 2.5A |
| 2.7A | Add tests for bridge discovery, absence handling, and event-to-instrument mapping | 2.6A |

Solution 2 track: Built-in OTLP/HTTP JSON exporter

| Step | Work Item | Dependencies |
|------|-----------|--------------|
| 2.4B | Implement `MetricsBuffer` | 2.2 |
| 2.5B | Implement `MetricDataPoint` class | 2.4B |
| 2.6B | Implement `InternalPerformanceCollector` (`PerformanceLogCallback` impl, sampling) | 2.4B, 2.5B |
| 2.7B | Implement `OtlpJsonFormatter` and `OtlpHttpExporter` | 2.5B |
| 2.8B | Implement `ExportScheduler` | 2.4B, 2.6B, 2.7B |
| 2.9B | Add tests for JSON formatting, batching, retries, and endpoint integration | 2.7B, 2.8B |

Solution 3 track: JUL filelog

| Step | Work Item | Dependencies |
|------|-----------|--------------|
| 2.4C | Implement internal JUL callback for structured performance records | 2.2 |
| 2.5C | Add automatic JUL enablement when this solution is selected | 2.4C |
| 2.6C | Publish sample collector parser configuration | 2.4C |
| 2.7C | Add tests for log line shape and parser compatibility | 2.6C |

Solution 4 track: Direct OpenTelemetry dependency

| Step | Work Item | Dependencies |
|------|-----------|--------------|
| 2.4D | Add `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp` as direct compile dependencies in `pom.xml` | 2.1 |
| 2.5D | Implement `OtelBootstrap` (idempotent init, noop detection, JVM shutdown hook) | 2.2, 2.4D |
| 2.6D | Implement `OpenTelemetryPerformanceCallback`, `OtelMetricCatalog`, `OtelAttributeBuilder` | 2.5D |
| 2.7D | Add tests for: (a) driver-owned SDK path, (b) reuse of app's `GlobalOpenTelemetry`, (c) instrument cardinality bounds, (d) shutdown hook flushes pending exports | 2.5D, 2.6D |
| 2.8D | Document the transitive dependency footprint and version compatibility matrix in `CHANGELOG.md` and `README.md` | 2.4D |

#### Phase 3: Hardening & Documentation

| Step | Work Item |
|------|-----------|
| 3.1 | Performance benchmarking (gzip effectiveness, overhead < 1%, sampling behavior) |
| 3.2 | Memory pressure testing (bounded buffer, high-volume workloads, sampling) |
| 3.3 | Update `README.md` with new connection string properties |
| 3.4 | Add sample application demonstrating both features + batching/sampling/compression |
| 3.5 | JavaDoc for all new public API classes |
| 3.6 | Document payload size expectations with/without optimization techniques |

---

<a id="d2-file-change-inventory"></a>
### D.2 File Change Inventory

(Moved from Section 7. Updated to include Solution 4 files.)


#### New Files

| File | Purpose |
|------|---------|
| `ServerExecutionStats.java` | Server stats data class |
| `ServerIOStats.java` | Per-table I/O stats |
| `ServerTimeStats.java` | Server timing stats |
| `StreamExecStats.java` | TDS token parser for EXECSTATS |
| `TelemetryBridgeFactory.java` | SPI for optional telemetry bridge discovery (Solution 1) |
| `MetricDataPoint.java` | Internal metric record for built-in OTLP/HTTP exporter (Solution 2 only) |
| `MetricsBuffer.java` | Thread-safe queue for built-in exporter batching (Solution 2 only) |
| `OtlpJsonFormatter.java` | OTLP JSON serialization via StringBuilder (Solution 2 only) |
| `OtlpHttpExporter.java` | HTTP export to OTLP endpoint (Solution 2 only) |
| `InternalPerformanceCollector.java` | Internal callback for built-in OTLP pipeline (Solution 2 only) |
| `JulPerformanceCollector.java` | Internal callback for JUL/filelog path (Solution 3 only) |
| `ExportScheduler.java` | Timed export daemon for built-in exporter (Solution 2 only) |
| `OtelBootstrap.java` | One-shot SDK detection / private-SDK builder (Solution 4 only) |
| `OpenTelemetryPerformanceCallback.java` | `PerformanceLogCallback` implementation that records OTel instruments (Solution 4 only) |
| `OtelMetricCatalog.java` | Holds cached `Meter` and instrument handles (Solution 4 only) |
| `OtelAttributeBuilder.java` | Central attribute key registry (Solution 4 only) |

#### Modified Files

| File | Changes |
|------|---------|
| `IOBuffer.java` | Add `TDS_FEATURE_EXT_EXECSTATS`, `TDS_EXECSTATS` token constants, token name |
| `SQLServerConnection.java` | Feature request/ack, telemetry lifecycle, new connection fields, internal callback registration |
| `SQLServerStatement.java` | `onExecStats()` override, `publishServerStats()`, `getServerExecutionStats()` |
| `ISQLServerStatement.java` | `getServerExecutionStats()` method signature |
| `PerformanceActivity.java` | New `SERVER_*` enum values |
| `PerformanceLogCallback.java` | Add default server-stats publish overload while preserving existing methods |
| `PerformanceLog.java` | Dual callback support (user + internal), callback chain in Scope.close() |
| `SQLServerDriver.java` | New telemetry connection string property enums |
| `SQLServerResource.java` | New error resource strings |
| `tdsparser.java` | `TDS_EXECSTATS` case in parse loop, default `onExecStats()` in `TDSTokenHandler` |
| `pom.xml` | (Solution 4 only) Add direct dependencies on `opentelemetry-api`, `opentelemetry-sdk`, `opentelemetry-exporter-otlp` and version-align them with the rest of the runtime |
| `TDSTokenHandler.java` | Default no-op `onExecStats()` method |

---

<a id="d3-solution-1-implementation-details"></a>
### D.3 Solution 1 Implementation Details

The following subsections were previously inline in Section 4.1.

#### D.3.1 Example Code Shape

Core driver side:

```java
ServiceLoader<TelemetryBridgeFactory> loader = ServiceLoader.load(TelemetryBridgeFactory.class);
for (TelemetryBridgeFactory factory : loader) {
    if (factory.supports(connectionProperties)) {
        PerformanceLogCallback callback = factory.createCallback(connectionProperties);
        PerformanceLog.setInternalCallback(callback);
        break;
    }
}
```

Optional bridge side:

```java
public final class OpenTelemetryPerformanceCallback implements PerformanceLogCallback {
    private final LongHistogram executeDuration;
    private final LongCounter logicalReads;

    OpenTelemetryPerformanceCallback(OpenTelemetry openTelemetry) {
        Meter meter = openTelemetry.getMeter("mssql-jdbc");
        executeDuration = meter.histogramBuilder("db.client.statement.execute.duration")
                .ofLongs()
                .setUnit("ms")
                .build();
        logicalReads = meter.counterBuilder("db.server.io.logical_reads")
                .build();
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId,
            long durationMs, Exception exception) {
        // map activity to instrument.record(...)
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId,
            ServerExecutionStats stats) {
        // map server stats to counter / histogram updates
    }
}
```

#### D.3.2 Detailed Implementation

##### Companion Artifact Layout

The optional bridge ships as a separate Maven coordinate so the core driver jar stays dependency-free:

```
mssql-jdbc-opentelemetry/
├── pom.xml                              (depends on mssql-jdbc + opentelemetry-api + opentelemetry-sdk
│                                         + opentelemetry-exporter-otlp + opentelemetry-sdk-extension-autoconfigure)
├── src/main/java/
│   └── com/microsoft/sqlserver/jdbc/opentelemetry/
│       ├── OpenTelemetryBridgeFactory.java       (implements TelemetryBridgeFactory SPI)
│       ├── OpenTelemetryPerformanceCallback.java (implements PerformanceLogCallback)
│       ├── InstrumentRegistry.java               (lazy creation/caching of histograms, counters)
│       ├── AttributeMapper.java                  (PerformanceActivity -> OTel Attributes)
│       └── SdkBuilder.java                       (configures OpenTelemetrySdk from JDBC props)
└── src/main/resources/
    └── META-INF/services/
        └── com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory   (single line: fully-qualified factory class)
```

##### Lifecycle and Activation

```
Driver loads class
  │
  ├── SQLServerDriver.<clinit> runs once
  │   └── ServiceLoader<TelemetryBridgeFactory>.load(...) — JDK SPI, no reflection
  │       ├── if bridge JAR absent → empty iterator, driver continues
  │       └── if bridge JAR present → factory instance cached statically
  │
Connection.open()
  │
  ├── parse connection properties
  ├── if otelEndpoint is set:
  │   ├── factory.supports(props) → returns true
  │   ├── factory.createCallback(props) returns OpenTelemetryPerformanceCallback
  │   │   ├── builds OpenTelemetrySdk via SdkBuilder
  │   │   ├── creates Meter("mssql-jdbc", driverVersion)
  │   │   ├── pre-creates instruments (histograms, counters) for known metrics
  │   │   └── returns callback
  │   └── PerformanceLog registers the bridge callback alongside any user callback
  │
  ├── ... statement executions ...
  │   └── PerformanceLog.publish() fans out to both user callback (if any) and bridge callback
  │       └── bridge callback calls Histogram.record() / Counter.add() with attributes
  │           └── OTel SDK batches/exports per its own configuration (independent of driver)
  │
  └── Connection.close()
      ├── no special bridge teardown per connection (Meter / SDK are process-scoped)
      └── on JVM shutdown hook, SdkBuilder calls openTelemetrySdk.close() for clean flush
```

##### OTel SDK Configuration: Two Paths

| Path | Trigger | Who configures the exporter |
|------|---------|-----------------------------|
| **Autoconfigure** | `io.opentelemetry.sdk.autoconfigure.AutoConfiguredOpenTelemetrySdk` already on classpath, or user runs the `opentelemetry-javaagent` | OTel SDK reads `OTEL_*` env vars and system properties; bridge calls `GlobalOpenTelemetry.get()` |
| **Programmatic** | Pure mssql-jdbc-opentelemetry without OTel javaagent | Bridge builds `OpenTelemetrySdk` itself from JDBC connection string properties |

The bridge prefers autoconfigure when available so it composes cleanly with `opentelemetry-javaagent -javaagent:opentelemetry-javaagent.jar` deployments. Programmatic mode is the fallback when the application has no other OTel wiring.

##### Mapping JDBC Connection Properties to OTel SDK Config

| JDBC property | OTel SDK config |
|---------------|-----------------|
| `otelEndpoint` | `OtlpHttpMetricExporterBuilder.setEndpoint(...)` |
| `otelExportInterval` | `PeriodicMetricReaderBuilder.setInterval(Duration)` |
| `otelBatchSize` | not directly mapped — SDK uses its own delta/cumulative buffering. JDBC value is advisory; logged as `info`. |
| `otelSamplingRate` | applied as a pre-filter in the bridge callback (sampling lives in the bridge, not the SDK metric path) |
| `otelServiceName` | `Resource.builder().put(SERVICE_NAME, ...)` |
| `otelHeaders` | `OtlpHttpMetricExporterBuilder.setHeaders(Map)` parsed from `key=value,key=value` |
| `otelResourceAttributes` | `Resource.builder().putAll(...)` parsed from `key=value,key=value` |

##### Failure and Coexistence Behavior

| Scenario | Behavior |
|----------|----------|
| Bridge JAR absent, `otelEndpoint` set | Driver logs one `WARNING`-level message: "otelEndpoint is set but mssql-jdbc-opentelemetry is not on classpath; no telemetry will be exported." No exception. |
| Bridge JAR present, `otelEndpoint` unset | Bridge factory's `supports(props)` returns false; bridge stays inert; zero overhead. |
| OTel SDK throws during exporter setup | Bridge catches, logs `WARNING`, sets callback to no-op; driver continues. |
| Per-record OTel call throws | Bridge callback catches and logs at `FINE` (rate-limited); never propagates to caller. |
| User also registers `PerformanceLogCallback` | Both callbacks receive every event independently; ordering is bridge-first then user (configurable). |

#### D.3.3 SPI and Bridge Classes

##### TelemetryBridgeFactory SPI (lives in core driver)

```java
package com.microsoft.sqlserver.jdbc.spi;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import java.util.Properties;

/**
 * SPI implemented by optional telemetry bridge artifacts.
 * Discovered via ServiceLoader. The core driver depends on this interface only;
 * it never imports OpenTelemetry types directly.
 */
public interface TelemetryBridgeFactory {

    /** Bridge-specific identifier, e.g. "opentelemetry". Used in diagnostic logs. */
    String name();

    /** Returns true if the bridge should activate for this connection. */
    boolean supports(Properties connectionProperties);

    /** Creates a callback. Called at most once per active bridge per JVM. */
    PerformanceLogCallback createCallback(Properties connectionProperties);
}
```

##### OpenTelemetryBridgeFactory (lives in mssql-jdbc-opentelemetry)

```java
package com.microsoft.sqlserver.jdbc.opentelemetry;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import java.util.Properties;

public final class OpenTelemetryBridgeFactory implements TelemetryBridgeFactory {

    @Override
    public String name() {
        return "opentelemetry";
    }

    @Override
    public boolean supports(Properties props) {
        String endpoint = props.getProperty("otelEndpoint", "");
        return !endpoint.isEmpty();
    }

    @Override
    public PerformanceLogCallback createCallback(Properties props) {
        OpenTelemetry otel = SdkBuilder.buildOrReuseGlobal(props);
        return new OpenTelemetryPerformanceCallback(otel, props);
    }
}
```

##### OpenTelemetryPerformanceCallback (the actual bridge)

```java
package com.microsoft.sqlserver.jdbc.opentelemetry;

import com.microsoft.sqlserver.jdbc.*;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

final class OpenTelemetryPerformanceCallback implements PerformanceLogCallback {

    private static final AttributeKey<Long>   CONN_ID  = AttributeKey.longKey("db.connection.id");
    private static final AttributeKey<Long>   STMT_ID  = AttributeKey.longKey("db.statement.id");
    private static final AttributeKey<String> ACTIVITY = AttributeKey.stringKey("db.jdbc.activity");
    private static final AttributeKey<String> ERROR    = AttributeKey.stringKey("error.type");

    private final double samplingRate;
    private final LongHistogram executeDuration;
    private final LongHistogram prepareDuration;
    private final LongCounter   logicalReads;
    private final LongCounter   physicalReads;
    private final LongHistogram serverElapsedMs;
    private final LongHistogram serverCpuMs;

    OpenTelemetryPerformanceCallback(OpenTelemetry otel, Properties props) {
        this.samplingRate = parseDouble(props.getProperty("otelSamplingRate", "1.0"));
        Meter m = otel.getMeter("com.microsoft.sqlserver.jdbc");
        this.executeDuration = m.histogramBuilder("db.client.statement.execute.duration")
                .ofLongs().setUnit("ms").build();
        this.prepareDuration = m.histogramBuilder("db.client.statement.prepare.duration")
                .ofLongs().setUnit("ms").build();
        this.logicalReads    = m.counterBuilder("db.server.io.logical_reads").build();
        this.physicalReads   = m.counterBuilder("db.server.io.physical_reads").build();
        this.serverElapsedMs = m.histogramBuilder("db.server.execution.elapsed_time")
                .ofLongs().setUnit("ms").build();
        this.serverCpuMs     = m.histogramBuilder("db.server.execution.cpu_time")
                .ofLongs().setUnit("ms").build();
    }

    @Override
    public void publish(PerformanceActivity activity, int connId, int stmtId,
                        long durationMs, Exception ex) {
        if (!shouldSample()) return;
        Attributes attrs = baseAttrs(activity, connId, stmtId, ex);
        try {
            switch (activity) {
                case STATEMENT_EXECUTE: executeDuration.record(durationMs, attrs); break;
                case STATEMENT_PREPARE: prepareDuration.record(durationMs, attrs); break;
                default: /* extend as new activities are added */
            }
        } catch (RuntimeException bridgeErr) {
            BridgeDiagnostics.logFine("bridge record failure", bridgeErr);
        }
    }

    @Override
    public void publish(PerformanceActivity activity, int connId, int stmtId,
                        ServerExecutionStats stats) {
        if (stats == null || !shouldSample()) return;
        Attributes attrs = baseAttrs(activity, connId, stmtId, null);
        try {
            if (stats.io != null) {
                logicalReads.add(stats.io.logicalReads, attrs);
                physicalReads.add(stats.io.physicalReads, attrs);
            }
            if (stats.time != null) {
                serverElapsedMs.record(stats.time.elapsedMs, attrs);
                serverCpuMs.record(stats.time.cpuMs, attrs);
            }
        } catch (RuntimeException bridgeErr) {
            BridgeDiagnostics.logFine("bridge server-stats failure", bridgeErr);
        }
    }

    private static Attributes baseAttrs(PerformanceActivity activity, int connId, int stmtId, Exception ex) {
        Attributes.Builder b = Attributes.builder()
                .put(CONN_ID, (long) connId)
                .put(STMT_ID, (long) stmtId)
                .put(ACTIVITY, activity.name());
        if (ex != null) b.put(ERROR, ex.getClass().getName());
        return b.build();
    }

    private boolean shouldSample() {
        return samplingRate >= 1.0 || ThreadLocalRandom.current().nextDouble() < samplingRate;
    }

    private static double parseDouble(String s) {
        try { return Double.parseDouble(s); } catch (Exception e) { return 1.0; }
    }
}
```

##### SdkBuilder (programmatic OTel SDK construction)

```java
package com.microsoft.sqlserver.jdbc.opentelemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import java.time.Duration;
import java.util.Map;
import java.util.Properties;

final class SdkBuilder {

    /** Returns the global OTel instance if one is already registered (autoconfigure / javaagent case),
     *  otherwise builds an SDK from JDBC connection properties. */
    static OpenTelemetry buildOrReuseGlobal(Properties p) {
        try {
            OpenTelemetry existing = GlobalOpenTelemetry.get();
            if (existing != null && existing != OpenTelemetry.noop()) return existing;
        } catch (Throwable ignored) { /* fall through to programmatic */ }

        OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
                .setEndpoint(p.getProperty("otelEndpoint"))
                .setHeaders(() -> parseKv(p.getProperty("otelHeaders", "")))
                .build();

        PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
                .setInterval(Duration.ofSeconds(
                        Long.parseLong(p.getProperty("otelExportInterval", "60"))))
                .build();

        Resource resource = Resource.getDefault().merge(Resource.create(
                Attributes.builder()
                        .put(AttributeKey.stringKey("service.name"),
                             p.getProperty("otelServiceName", "mssql-jdbc"))
                        .putAll(toAttributes(parseKv(p.getProperty("otelResourceAttributes", ""))))
                        .build()));

        SdkMeterProvider meterProvider = SdkMeterProvider.builder()
                .setResource(resource)
                .registerMetricReader(reader)
                .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
                .setMeterProvider(meterProvider)
                .buildAndRegisterGlobal();

        Runtime.getRuntime().addShutdownHook(new Thread(sdk::close, "mssql-jdbc-otel-shutdown"));
        return sdk;
    }

    private static Map<String, String> parseKv(String csv) { /* ... split on ',' then '=' ... */ }
    private static Attributes toAttributes(Map<String, String> kv) { /* ... */ return Attributes.empty(); }
}
```

#### D.3.4 OpenTelemetry SDK Configuration Mechanics

##### Why the Bridge Delegates Batching to the SDK

Solution 2 implements its own `MetricsBuffer`, `ExportScheduler`, gzip path, and retry policy. Solution 1 does **none** of that — these concerns live inside the OTel SDK:

| Concern | Solution 2 (in-driver) | Solution 1 (delegated to OTel SDK) |
|---------|------------------------|-------------------------------------|
| In-memory buffering | `MetricsBuffer` (ConcurrentLinkedQueue) | SDK's internal aggregator + `PeriodicMetricReader` |
| Export trigger | Time-based + size-based dual trigger | `PeriodicMetricReader` interval; no size trigger by default |
| Wire format | Hand-rolled OTLP JSON | `OtlpHttpMetricExporter` / `OtlpGrpcMetricExporter` (Protobuf or JSON) |
| Compression | gzip implemented in driver | `OtlpHttpMetricExporter` enables gzip by default |
| Retry on 5xx | 1 retry, fixed backoff | SDK's retry policy (configurable, exponential) |
| Thread model | Single `ExportScheduler` daemon thread | SDK manages its own executor |
| Shutdown flush | Explicit `Connection.close()` hook | `SdkMeterProvider.close()` triggered by JVM shutdown hook |

##### Coexistence with `opentelemetry-javaagent`

When the user already runs the OpenTelemetry Java instrumentation agent:

```
java -javaagent:opentelemetry-javaagent.jar \
     -Dotel.exporter.otlp.endpoint=http://collector:4318 \
     -Dotel.service.name=order-service \
     -cp mssql-jdbc.jar:mssql-jdbc-opentelemetry.jar:app.jar \
     com.example.App
```

The bridge detects the already-installed `GlobalOpenTelemetry`, reuses its `Meter`, and adds JDBC metrics into the same export pipeline as the rest of the application's telemetry. No double-configuration, no duplicate exporters.

##### Standalone Mode (No javaagent)

```
java -cp mssql-jdbc.jar:mssql-jdbc-opentelemetry.jar:opentelemetry-sdk.jar:opentelemetry-exporter-otlp.jar:app.jar \
     com.example.App
```

The bridge constructs its own `OpenTelemetrySdk` from JDBC connection string properties. The JDBC connection string is the **single source of truth** for telemetry configuration — no separate `OTEL_*` env vars required.

#### D.3.5 Real-World Deployment Examples

##### Example 1: Side-by-side with otel-javaagent (most common)

```bash
java -javaagent:/opt/otel/opentelemetry-javaagent.jar \
     -Dotel.exporter.otlp.endpoint=http://otel-collector:4318 \
     -Dotel.service.name=order-service \
     -cp app.jar:mssql-jdbc.jar:mssql-jdbc-opentelemetry.jar \
     com.example.App
```

JDBC connection string only needs to flag activation:

```
jdbc:sqlserver://db.contoso.com:1433;databaseName=orders;serverExecutionStatistics=all;otelEndpoint=http://otel-collector:4318/v1/metrics
```

JDBC metrics flow through the **same** OTLP pipeline as the agent's auto-instrumented HTTP, gRPC, and JDBC-span telemetry.

##### Example 2: Standalone with Azure Monitor Application Insights agent (see 4.6)

```bash
java -javaagent:/opt/azure/applicationinsights-agent.jar \
     -cp app.jar:mssql-jdbc.jar:mssql-jdbc-opentelemetry.jar \
     com.example.App
```

Azure agent registers a `GlobalOpenTelemetry`; the bridge picks it up; JDBC metrics appear in Application Insights without further wiring.

##### Example 3: Pure programmatic, no javaagent

```bash
java -cp app.jar:mssql-jdbc.jar:mssql-jdbc-opentelemetry.jar:\
opentelemetry-api.jar:opentelemetry-sdk.jar:opentelemetry-exporter-otlp.jar \
     com.example.App
```

Connection string carries all OTel config:

```
jdbc:sqlserver://...;otelEndpoint=https://otlp.eu.example.com/v1/metrics;otelHeaders=Authorization=Bearer abc123;otelServiceName=order-service;otelExportInterval=30
```

#### D.3.6 Debugging the Bridge

| Symptom | First thing to check |
|---------|----------------------|
| No metrics at backend, no driver warnings | Bridge JAR may not be on classpath. Look for `ServiceLoader` debug: `-Djdk.internal.loader.debug=true` or check `META-INF/services/com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory` is present in the JAR. |
| Driver logs `otelEndpoint is set but mssql-jdbc-opentelemetry is not on classpath` | Add the optional artifact. |
| Bridge active but no metrics | Set `java.util.logging.Logger("com.microsoft.sqlserver.jdbc.opentelemetry").level=FINE` to see per-event bridge activity. |
| `NoSuchMethodError` on OTel API | Bridge artifact was built against a different OTel API minor version than runtime. Pin to a tested version. |
| Metrics appear under wrong `service.name` | A `GlobalOpenTelemetry` was registered before the bridge ran. Pass `-Dotel.service.name=...` or use programmatic mode. |
| Bridge throws on shutdown | Confirm `SdkMeterProvider.close()` is reachable from the shutdown hook; check for sealed classloaders in app servers. |

**Bridge-side diagnostics class:**

```java
final class BridgeDiagnostics {
    private static final java.util.logging.Logger LOG =
            java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc.opentelemetry");
    static void logFine(String msg, Throwable t) {
        if (LOG.isLoggable(java.util.logging.Level.FINE)) LOG.log(java.util.logging.Level.FINE, msg, t);
    }
}
```

---

<a id="d4-solution-2-implementation-details"></a>
### D.4 Solution 2 Implementation Details

The following subsections were previously inline in Section 4.2.

#### D.4.1 Example Code Shape

```java
final class InternalPerformanceCollector implements PerformanceLogCallback {
    private final MetricsBuffer buffer;

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId,
            long durationMs, Exception exception) {
        MetricDataPoint point = MetricDataPoint.forDuration(activity, connectionId,
                statementId, durationMs, exception);
        buffer.offer(point);
    }

    @Override
    public void publish(PerformanceActivity activity, int connectionId, int statementId,
            ServerExecutionStats stats) {
        for (MetricDataPoint point : MetricDataPoint.fromServerStats(connectionId, statementId, stats)) {
            buffer.offer(point);
        }
    }
}
```

#### D.4.2 Detailed Implementation

##### Lifecycle and Batching Strategy

The driver uses **intelligent batching** to minimize payload overhead:

```
Connection.open()
  │
  ├── otelEndpoint is set?
  │   ├── No  → normal operation, no export overhead
  │   └── Yes → create InternalPerformanceCollector
  │             ├── register as PerformanceLogCallback (internal, not user-visible)
  │             ├── start ExportScheduler daemon thread
  │             └── if serverExecutionStatistics also set, enable server stats
  │
  ├── ... statement executions ...
  │   └── Each metric occurrence → OtlpDataPoint → MetricsBuffer.enqueue()
  │
  ├── ExportScheduler runs on two triggers (whichever comes first):
  │   ├─ Time-based: every otelExportInterval seconds (default 60s)
  │   └─ Size-based: when MetricsBuffer reaches otelBatchSize data points (default 1000)
  │
  ├── When export is triggered:
  │   ├─ dequeue all buffered data points from MetricsBuffer
  │   ├─ Group data points by metric name + common attribute set
  │   │  (e.g., all "db.client.statement.execute.duration" for connection 100, statement 10)
  │   ├─ OtlpJsonFormatter formats as OTLP JSON
  │   ├─ Compress with gzip
  │   └─ OtlpHttpExporter POSTs to otelEndpoint with Content-Encoding: gzip
  │
  └── Connection.close()
      ├── final flush of remaining buffered metrics (same compression/batching)
      ├── shutdown ExportScheduler
      └── close HttpURLConnection
```

**Batching optimizations:**

| Optimization | Impact | Example |
|--------------|--------|---------|
| **Size-based trigger** | Export immediately if buffer fills up (e.g., 1000 data points) before timer fires | High-volume app with 10,000 statements/sec flushes multiple times per second |
| **Attribute grouping** | Group data points that share the same attributes; reduces JSON repetition | 100 SELECTs from table `dbo.orders` on connection 5 → one metric with 100 dataPoints, attributes sent once |
| **Gzip compression** | OTLP/HTTP standard practice; ~80% reduction for typical metric payloads | 50 KB uncompressed → ~10 KB gzipped |
| **Lazy attribute inclusion** | Omit attributes that are redundant (e.g., don't repeat `server.address` if all points are from same server) | Post contains attributes only when they differ |

##### Payload Size Estimation

**Worst case (no optimization):** 1000 data points, each with 8 attributes, all different values

```
Uncompressed JSON size ≈ 1000 * (120 bytes/metric + 200 bytes/attributes) ≈ 320 KB
Gzipped ≈ 320 KB * 0.2 ≈ 64 KB
```

**Best case (with grouping):** 1000 data points grouped into 10 metric types, 5 different attribute combinations

```
Uncompressed JSON size ≈ 1000 * 50 bytes/point + 10 * 300 bytes/metric_header + 5 * 200 bytes/attribute_set ≈ 60 KB
Gzipped ≈ 60 KB * 0.2 ≈ 12 KB
```

**Practical middle ground:** Enable sampling for high-volume workloads (see the sampling subsection below).

##### Intelligent Data Point Grouping

The `OtlpJsonFormatter` intelligently groups data points to minimize repetition:

```java
// Pseudo-code for smart grouping

Map<String, List<OtlpDataPoint>> groupByMetricName(List<OtlpDataPoint> points) {
    Map<String, List<OtlpDataPoint>> result = new LinkedHashMap<>();
    for (OtlpDataPoint point : points) {
        result.computeIfAbsent(point.metricName, k -> new ArrayList<>())
              .add(point);
    }
    return result;
}

// For each metric name:
//   Group dataPoints by attributes (same attributes = same group)
//   Output metric once with all its groups
// This way:
//   - "db.client.statement.execute.duration" metric appears once
//   - All 100 SELECT statements are in its dataPoints array
//   - Attributes are sent with each point only if they differ
```

**Example:** 100 SELECTs on same table

```json
{
  "name": "db.client.statement.execute.duration",
  "gauge": {
    "dataPoints": [
      { "value": 5.0, "attrs": { "connId": 100, "stmtId": 10 } },
      { "value": 6.0, "attrs": { "connId": 100, "stmtId": 11 } },
      ...
      { "value": 7.0, "attrs": { "connId": 100, "stmtId": 109 } }
    ]
  }
}
```
All 100 data points share `server.address`, `db.namespace`, `db.operation.name="SELECT"` — sent once at metric level, not repeated for each point.

##### How Metrics Flow Through the Buffer and Get Batched

This diagram shows the complete flow for a scenario with 2 connections executing statements concurrently:

```
Timeline (t=0 to t=10s, export interval = 10s)

Connection-100 Events:              Connection-101 Events:
│                                   │
├─ t=1s  STMT-10 exec (5ms)        ├─ t=2s  STMT-20 exec (7ms)
│        → InternalPerformanceCollector.publish()
│        → OtlpDataPoint created:   
│           metric="db.client.statement.execute.duration"
│           value=5.0
│           attrs=[connId=100, stmtId=10]
│        → MetricsBuffer.enqueue()
│                                   │
├─ t=3s  STMT-11 exec (8ms)        ├─ t=4s  STMT-21 exec (3ms)
│        → OtlpDataPoint            → OtlpDataPoint
│           value=8.0               value=3.0
│           attrs=[connId=100, stmtId=11]  attrs=[connId=101, stmtId=21]
│        → enqueue()                → enqueue()
│                                   │
├─ t=5s  Server stats arrive       ├─ t=6s  Server stats arrive
│        → OtlpDataPoint            → OtlpDataPoint
│           metric="db.server.execution.elapsed_time"
│           value=20.0 (server CPU)  value=25.0 (server CPU)
│        → enqueue()                → enqueue()
│                                   │
├─ ...  (more events)              ├─ ...
│                                   │
└─ t=10s ExportScheduler fires      └─ t=10s ExportScheduler fires
         dequeue all buffered data points:
         - STMT-10 client exec: 5ms
         - STMT-11 client exec: 8ms
         - STMT-20 client exec: 7ms
         - STMT-21 client exec: 3ms
         - STMT-10 server exec: 20ms
         - ... (all other buffered points)
         
         OtlpJsonFormatter.format()
         → Single OTLP JSON payload with:
           - One "db.client.statement.execute.duration" metric
             with 4 dataPoints (one per statement, all connections mixed)
           - One "db.server.execution.elapsed_time" metric
             with 4 dataPoints (one per statement, all connections mixed)
           - ... (other metrics)
         
         OtlpHttpExporter.export()
         → POST to "http://collector:4318/v1/metrics"
         
         MetricsBuffer cleared for next window
```

**Key points:**
1. **MetricsBuffer is shared** across all active connections in the process
2. **OtlpDataPoint** is created immediately when a metric occurs, carries full context
3. **Attributes on each data point** (`db.connection.id`, `db.statement.id`) provide the grouping — not separate containers
4. **One POST every N seconds** contains all buffered points; connection/statement context is in the attributes
5. **Collector-side queries** can then filter/group by attributes:
   - `db.connection.id = 100` → all metrics for that connection
   - `db.statement.id = 10` → all metrics for that statement
   - `db.operation.name = "SELECT"` → all SELECTs regardless of connection

##### Coexistence with User Callback

The driver supports **both** a user-registered callback and the internal OTLP collector simultaneously. The existing `PerformanceLog` infrastructure is modified to support a callback chain:

```java
// PerformanceLog.java — modified to support dual callbacks

private static volatile PerformanceLogCallback userCallback;
private static volatile PerformanceLogCallback internalCallback;  // OTLP collector

static void publishToCallbacks(PerformanceActivity activity, int connectionId,
                                int statementId, long durationMs, Exception exception) {
    PerformanceLogCallback uc = userCallback;
    if (uc != null) {
        uc.publish(activity, connectionId, statementId, durationMs, exception);
    }
    PerformanceLogCallback ic = internalCallback;
    if (ic != null) {
        ic.publish(activity, connectionId, statementId, durationMs, exception);
    }
}
```

##### Optional Metric Sampling (for High-Volume Workloads)

For applications with extremely high query throughput (>10K statements/sec), sampling can reduce overhead:

```
otelSamplingRate=0.1  // Sample 10% of metrics (1 in every 10 statements)
```

**Sampling implementation:**

```java
// InternalPerformanceCollector

private final Random samplingRandom = new Random();

void publish(PerformanceActivity activity, int connId, int stmtId, long durationMs, Exception ex) {
    if (otelSamplingRate < 1.0 && samplingRandom.nextDouble() > otelSamplingRate) {
        return;  // Skip this metric
    }
    // ... proceed to create OtlpDataPoint and enqueue
}
```

**Sampling trade-off:**
- ✅ 90% reduction in metrics volume (at 0.1 sampling rate)
- ✅ Still statistically valid for aggregation (percentiles, averages computed on sample)
- ❌ Cannot see outlier individual executions (but can see them via monitoring alerts if needed)

Default: `otelSamplingRate=1.0` (no sampling). Only enable if profiling shows OTLP export is a bottleneck.

##### HTTP Export Details

| Aspect | Implementation |
|--------|---------------|
| HTTP client | `java.net.HttpURLConnection` (JDK built-in, no dependency) |
| Content-Type | `application/json` |
| Content-Encoding | **`gzip`** (always enabled; ~80% size reduction) |
| Method | `POST` |
| Path | Configured endpoint URL (must include `/v1/metrics`) |
| Timeout | Connect: 10s, Read: 30s |
| Retry | 1 retry with exponential backoff on 5xx or IOException |
| Auth | Via `otelHeaders` property (e.g., `Authorization=Bearer ...`) |
| TLS | Follows JVM trust store; respects `trustStore`/`trustStorePassword` if configured |
| Typical payload size | 1000 metrics statements: ~320 KB uncompressed → ~60 KB gzipped (with grouping) |

##### Failure Handling

| Scenario | Behavior |
|----------|----------|
| Endpoint unreachable | Log warning via `java.util.logging`, drop batch, continue |
| HTTP 4xx response | Log warning, drop batch (configuration error — don't retry) |
| HTTP 5xx response | Retry once after 1 second; if still failing, drop and continue |
| Payload too large | Split into smaller batches (max 1 MB per POST) |
| Connection closed | Final flush attempt; if it fails, metrics are lost (acceptable for best-effort) |

**Critical design principle:** Export failures must never throw exceptions to the application or affect SQL query execution in any way.

---

#### D.4.3 MetricsBuffer and OtlpDataPoint

##### OtlpDataPoint Class

```java
package com.microsoft.sqlserver.jdbc.metrics.otel;

import java.util.HashMap;
import java.util.Map;

/**
 * Immutable OTLP data point representing a single metric observation.
 * Created immediately when a metric event occurs (no aggregation in driver).
 */
public final class OtlpDataPoint {
    private final String metricName;           // e.g., "db.client.statement.execute.duration"
    private final String unit;                 // e.g., "ms", "{pages}"
    private final long timeUnixNano;           // Nanosecond timestamp
    private final double value;                // Numeric value (gauge)
    private final Map<String, String> attributes;  // Context: connId, stmtId, server, etc.
    
    public OtlpDataPoint(String metricName, String unit, long timeUnixNano,
                         double value, Map<String, String> attributes) {
        this.metricName = metricName;
        this.unit = unit;
        this.timeUnixNano = timeUnixNano;
        this.value = value;
        this.attributes = new HashMap<>(attributes);  // Defensive copy
    }
    
    public String getMetricName() { return metricName; }
    public String getUnit() { return unit; }
    public long getTimeUnixNano() { return timeUnixNano; }
    public double getValue() { return value; }
    public Map<String, String> getAttributes() { return new HashMap<>(attributes); }
    
    // Builder for convenience
    public static class Builder {
        private String metricName;
        private String unit = "";
        private long timeUnixNano;
        private double value;
        private Map<String, String> attributes = new HashMap<>();
        
        public Builder metricName(String name) { this.metricName = name; return this; }
        public Builder unit(String u) { this.unit = u; return this; }
        public Builder timeUnixNano(long t) { this.timeUnixNano = t; return this; }
        public Builder value(double v) { this.value = v; return this; }
        public Builder attribute(String key, String val) { attributes.put(key, val); return this; }
        public Builder attributes(Map<String, String> attrs) { attributes.putAll(attrs); return this; }
        
        public OtlpDataPoint build() {
            return new OtlpDataPoint(metricName, unit, timeUnixNano, value, attributes);
        }
    }
}
```

##### MetricsBuffer Implementation

```java
package com.microsoft.sqlserver.jdbc.metrics.otel;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.logging.Logger;

/**
 * Thread-safe, bounded ring buffer for OTLP data points.
 * 
 * Key features:
 * - Lock-free enqueue via ConcurrentLinkedQueue
 * - Bounded capacity: auto-evicts oldest points if full
 * - Single-threaded dequeue (ExportScheduler only)
 * - No external synchronization needed for enqueue
 */
public class MetricsBuffer {
    private static final Logger logger = Logger.getLogger(MetricsBuffer.class.getName());
    
    private final ConcurrentLinkedQueue<OtlpDataPoint> queue;
    private final int maxCapacity;
    private volatile int currentSize = 0;
    
    /**
     * @param maxCapacity Maximum number of data points to buffer (default: 10,000)
     */
    public MetricsBuffer(int maxCapacity) {
        this.maxCapacity = maxCapacity;
        this.queue = new ConcurrentLinkedQueue<>();
    }
    
    /**
     * Enqueue a metric data point. If buffer is full, evicts oldest point.
     * Non-blocking; safe to call from concurrent statement execution threads.
     * 
     * @param dataPoint The metric observation to buffer
     */
    public void enqueue(OtlpDataPoint dataPoint) {
        if (dataPoint == null) {
            return;  // Silently ignore null
        }
        
        queue.offer(dataPoint);
        int newSize = currentSize + 1;
        
        // Check if we exceeded capacity
        if (newSize > maxCapacity) {
            OtlpDataPoint evicted = queue.poll();
            if (evicted != null) {
                logger.fine("MetricsBuffer full: evicted oldest data point. " +
                           "Consider increasing otelBatchSize or reducing query volume.");
                currentSize = newSize - 1;
            }
        } else {
            currentSize = newSize;
        }
    }
    
    /**
     * Dequeue all buffered data points (drain the buffer).
     * Called only by ExportScheduler thread; no external locking needed.
     * 
     * @return List of all buffered data points; empty if buffer is empty
     */
    public List<OtlpDataPoint> drainAll() {
        List<OtlpDataPoint> result = new ArrayList<>();
        OtlpDataPoint point;
        while ((point = queue.poll()) != null) {
            result.add(point);
        }
        currentSize = 0;
        return result;
    }
    
    /**
     * Get current size without draining. For monitoring/debugging only.
     */
    public int size() {
        return currentSize;
    }
    
    /**
     * Clear all buffered data points.
     */
    public void clear() {
        queue.clear();
        currentSize = 0;
    }
    
    /**
     * Check if buffer has reached the given threshold (for size-based batching).
     */
    public boolean hasReachedThreshold(int threshold) {
        return currentSize >= threshold;
    }
}
```

##### Why ConcurrentLinkedQueue?

| Characteristic | ConcurrentLinkedQueue | ConcurrentHashMap | ArrayBlockingQueue | LinkedBlockingQueue |
|---|---|---|---|---|
| **Thread-safe enqueue** | ✅ Yes (lock-free) | N/A | ✅ Yes (lock) | ✅ Yes (lock) |
| **Lock-free** | ✅ Yes (CAS loops) | ✅ Yes | ❌ No (intrinsic lock) | ❌ No (ReentrantLock) |
| **Bounded support** | ❌ No (unbounded) | N/A | ✅ Yes | ✅ Yes |
| **Single-threaded dequeue fast** | ✅ Yes | N/A | ✅ Yes | ✅ Yes |
| **Memory overhead** | Low per-node | N/A | Pre-allocated | Pre-allocated |
| **Best for our use case** | ✅ Excellent | N/A | Good but has lock | Good but has lock |

**Our choice:** `ConcurrentLinkedQueue` + manual bounded logic gives us:
- **Lock-free insertion** from concurrent statement threads (no contention)
- **Simple manual eviction** when capacity is exceeded
- **Single-threaded drain** in ExportScheduler (no lock needed)

We **don't need blocking semantics** (ConcurrentHashMap-style backpressure) because:
- SQL execution must never block waiting for metrics buffer
- If buffer is full, we evict oldest (acceptable loss for non-critical metrics)

##### Usage Pattern: InternalPerformanceCollector

```java
package com.microsoft.sqlserver.jdbc.metrics;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.metrics.otel.MetricsBuffer;
import com.microsoft.sqlserver.jdbc.metrics.otel.OtlpDataPoint;

/**
 * Internal metrics collector that publishes to OTLP buffer.
 * Implements PerformanceLogCallback to receive all metric events.
 */
public class InternalPerformanceCollector implements PerformanceLogCallback {
    
    private final MetricsBuffer metricsBuffer;
    private final String serverAddress;
    private final int serverPort;
    private final String databaseName;
    private final double samplingRate;  // 1.0 = no sampling, 0.1 = 10% sampling
    
    public InternalPerformanceCollector(MetricsBuffer metricsBuffer,
                                        String serverAddress, int serverPort,
                                        String databaseName, double samplingRate) {
        this.metricsBuffer = metricsBuffer;
        this.serverAddress = serverAddress;
        this.serverPort = serverPort;
        this.databaseName = databaseName;
        this.samplingRate = samplingRate;
    }
    
    /**
     * Called by PerformanceLog whenever a metric event completes.
     * Must not throw exceptions or block.
     */
    @Override
    public void publish(PerformanceActivity activity, int connectionId,
                       int statementId, long durationMs, Exception exception) {
        
        // Apply sampling if configured
        if (!shouldSample()) {
            return;
        }
        
        String metricName = getMetricName(activity);
        String unit = getUnit(activity);
        
        OtlpDataPoint point = new OtlpDataPoint.Builder()
            .metricName(metricName)
            .unit(unit)
            .timeUnixNano(System.nanoTime())  // or System.currentTimeMillis() * 1_000_000
            .value((double) durationMs)
            .attribute("db.connection.id", String.valueOf(connectionId))
            .attribute("server.address", serverAddress)
            .attribute("server.port", String.valueOf(serverPort))
            .attribute("db.namespace", databaseName)
            .attribute("error.type", exception != null ? exception.getClass().getSimpleName() : null)
            .build();
        
        // Non-blocking enqueue; will auto-evict if buffer full
        metricsBuffer.enqueue(point);
    }
    
    private boolean shouldSample() {
        if (samplingRate >= 1.0) {
            return true;  // No sampling
        }
        // Simple random sampling: 10% sampling rate = true 10% of the time
        return Math.random() < samplingRate;
    }
    
    private String getMetricName(PerformanceActivity activity) {
        switch (activity) {
            case CONNECTION: return "db.client.connection.duration";
            case PRELOGIN: return "db.client.connection.prelogin.duration";
            case LOGIN: return "db.client.connection.login.duration";
            case TOKEN_ACQUISITION: return "db.client.connection.token_acquisition.duration";
            case STATEMENT_REQUEST_BUILD: return "db.client.statement.request_build.duration";
            case STATEMENT_FIRST_SERVER_RESPONSE: return "db.client.statement.server_response.duration";
            case STATEMENT_PREPARE: return "db.client.statement.prepare.duration";
            case STATEMENT_PREPEXEC: return "db.client.statement.prepexec.duration";
            case STATEMENT_EXECUTE: return "db.client.statement.execute.duration";
            default: return "db.client.unknown.duration";
        }
    }
    
    private String getUnit(PerformanceActivity activity) {
        return "ms";  // All timing metrics are in milliseconds
    }
}
```

##### ExportScheduler Usage

```java
package com.microsoft.sqlserver.jdbc.metrics.otel;

import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.logging.Logger;

/**
 * Daemon thread that periodically exports buffered metrics via OTLP/HTTP.
 * Uses dual-trigger: time-based OR size-based, whichever comes first.
 */
public class ExportScheduler {
    private static final Logger logger = Logger.getLogger(ExportScheduler.class.getName());
    
    private final MetricsBuffer metricsBuffer;
    private final OtlpHttpExporter exporter;
    private final ScheduledExecutorService scheduler;
    private final long exportIntervalSeconds;
    private final int batchSizeThreshold;
    
    public ExportScheduler(MetricsBuffer metricsBuffer, OtlpHttpExporter exporter,
                          ScheduledExecutorService scheduler,
                          long exportIntervalSeconds, int batchSizeThreshold) {
        this.metricsBuffer = metricsBuffer;
        this.exporter = exporter;
        this.scheduler = scheduler;
        this.exportIntervalSeconds = exportIntervalSeconds;
        this.batchSizeThreshold = batchSizeThreshold;
    }
    
    /**
     * Start the export scheduler.
     */
    public void start() {
        scheduler.scheduleAtFixedRate(this::exportIfNeeded,
                                     exportIntervalSeconds, exportIntervalSeconds,
                                     TimeUnit.SECONDS);
    }
    
    /**
     * Export metrics if:
     * 1. Time-based trigger: export interval elapsed, OR
     * 2. Size-based trigger: buffer has reached batchSizeThreshold
     */
    private void exportIfNeeded() {
        try {
            // Check size-based trigger
            if (metricsBuffer.hasReachedThreshold(batchSizeThreshold)) {
                logger.fine("Size-based export triggered: buffer reached " +
                           batchSizeThreshold + " data points");
                export();
                return;
            }
            
            // Time-based trigger
            if (metricsBuffer.size() > 0) {
                logger.fine("Time-based export triggered: " +
                           metricsBuffer.size() + " buffered data points");
                export();
            }
        } catch (Exception e) {
            // Never propagate exceptions; log and continue
            logger.warning("Failed to export metrics: " + e.getMessage());
        }
    }
    
    private void export() {
        // Drain all buffered data points
        List<OtlpDataPoint> dataPoints = metricsBuffer.drainAll();
        
        if (dataPoints.isEmpty()) {
            return;
        }
        
        // Format as OTLP JSON
        String otlpJson = exporter.format(dataPoints);
        
        // POST to OTLP endpoint (with retry logic)
        exporter.send(otlpJson);
    }
    
    /**
     * Gracefully shutdown the scheduler (called on application close or connection close).
     */
    public void shutdown() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
            // Final flush before shutdown
            export();
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
```

##### Thread Safety Analysis

```
Enqueue Path (Multiple Threads):
├─ Thread-1: ps.executeQuery()
│   └─ PerformanceLog.publish(EXECUTE, 15ms)
│      └─ InternalPerformanceCollector.publish()
│         └─ MetricsBuffer.enqueue(OtlpDataPoint)  ◄── ConcurrentLinkedQueue (lock-free)
│
├─ Thread-2: ps.executeQuery()
│   └─ PerformanceLog.publish(EXECUTE, 8ms)
│      └─ InternalPerformanceCollector.publish()
│         └─ MetricsBuffer.enqueue(OtlpDataPoint)  ◄── ConcurrentLinkedQueue (lock-free)
│
└─ ... (more concurrent threads)

Dequeue Path (Single Thread):
└─ ExportScheduler thread
   └─ ExportScheduler.exportIfNeeded()
      └─ MetricsBuffer.drainAll()  ◄── No lock; only reader
         └─ ConcurrentLinkedQueue.poll()  ◄── Safe against concurrent enqueue
         └─ OtlpJsonFormatter.format(dataPoints)
         └─ OtlpHttpExporter.send()
```

**Safety guarantee:** `ConcurrentLinkedQueue` uses CAS (Compare-And-Swap) loops internally, so:
- Multiple threads can `enqueue()` simultaneously without contention
- Single ExportScheduler thread drains safely without locks
- No cross-thread visibility issues (happens-before relationships enforced by queue)

##### Memory and Performance Characteristics

| Scenario | Metrics/sec | Buffer Size | Memory (bytes) | Eviction Rate |
|----------|-------------|-------------|----------------|----------------|
| **Light load** | 100 | 100–500 | ~50–250 KB | 0% |
| **Normal load** | 1,000 | 1,000 | ~500 KB | 0% |
| **High load** | 10,000 | 10,000 | ~5 MB | 0% (export every 1s) |
| **Extreme load (no sampling)** | 100,000 | 10,000 | ~5 MB | 99% (oldest evicted immediately) |
| **Extreme load (10% sampling)** | 100,000 → 10,000 | ~1,000 | ~500 KB | 0% |

**Per OtlpDataPoint memory:** ~500–600 bytes (HashMap overhead + 10–15 attributes)

---

#### D.4.4 OTLP Endpoint and HTTP POST Mechanics

##### What is an OTLP Endpoint?

An **OTLP endpoint** is an HTTP server that accepts OpenTelemetry metric data in a standard JSON format. It's the **destination** where the JDBC driver sends its buffered metrics.

**OTLP endpoint examples:**

| Endpoint Type | URL Pattern | Provider |
|---|---|---|
| **OTel Collector (self-hosted)** | `http://localhost:4318/v1/metrics` | Open source OTEL project |
| **Azure Monitor (AMA)** | `http://localhost:8888/metrics` | Azure Application Insights |
| **Datadog Agent** | `http://localhost:4318/v1/metrics` | Datadog observability platform |
| **Grafana Loki/Tempo** | `http://loki-distributor:3100/otlp/v1/metrics` | Grafana Cloud |
| **New Relic** | `https://otlp.nr-data.net:4318/v1/metrics` | New Relic APM |
| **Honeycomb** | `https://api.honeycomb.io/v1/metrics` | Honeycomb SaaS |
| **Custom collector** | Any HTTP server implementing OTLP spec | Your application |

**Key characteristic:** An OTLP endpoint is just an **HTTP POST endpoint** that:
- Listens on an HTTP port (typically 4318 for gRPC→HTTP bridge, or 8888 for native HTTP)
- Accepts `POST /v1/metrics` requests with `Content-Type: application/json`
- Optionally accepts `Content-Encoding: gzip` (compression)
- Returns HTTP 200 on success, 4xx/5xx on failure
- Can optionally require authentication (Bearer token, API key, mTLS, etc.)

##### Configuring the OTLP Endpoint in the JDBC Driver

**Connection string format:**

```
jdbc:sqlserver://myserver.database.windows.net:1433;
  databaseName=mydb;
  serverExecutionStatistics=all;
  otelEndpoint=http://otel-collector:4318/v1/metrics;
  otelExportInterval=30;
  otelServiceName=order-service;
  otelHeaders=Authorization=Bearer my-token-123
```

**Property breakdown:**

| Property | Example | Meaning |
|---|---|---|
| `otelEndpoint` | `http://otel-collector:4318/v1/metrics` | Full URL (scheme + host + port + path). Enables OTLP export when set. |
| `otelExportInterval` | `30` | Seconds between exports (time-based trigger). Range: 5–3600 seconds. Default: 60. |
| `otelBatchSize` | `1000` | Size-based trigger: export when buffer reaches N data points. Default: 1000. |
| `otelSamplingRate` | `1.0` | Sampling: 1.0 = all metrics, 0.5 = 50%, 0.1 = 10% for high-volume workloads. Default: 1.0. |
| `otelServiceName` | `"order-service"` | OTLP `service.name` resource attribute. Default: `"mssql-jdbc"`. |
| `otelHeaders` | `Authorization=Bearer token,X-Custom=value` | Extra HTTP headers. Comma-separated `key=value` pairs. |
| `otelResourceAttributes` | `environment=production,dc=us-east` | Extra OTLP resource attributes (global metadata). Comma-separated. |

**Activation:** OTLP export is enabled if and only if `otelEndpoint` is set to a non-empty URL. All other properties are optional and have sensible defaults.

#### D.4.5 HTTP POST Mechanism: Step-by-Step

The driver performs these steps every export interval (or when buffer size threshold is reached):

```
Timeline:

t=0s   Statement 1 executes
       └─ MetricsBuffer.enqueue(OtlpDataPoint1)

t=5s   Statement 2 executes
       └─ MetricsBuffer.enqueue(OtlpDataPoint2)

t=15s  ExportScheduler fires (every otelExportInterval seconds)
       │
       ├─ Step 1: Check conditions
       │  ├─ Has time-based trigger fired? (t >= last_export + otelExportInterval)
       │  └─ Has size-based trigger fired? (buffer.size() >= otelBatchSize)
       │
       ├─ Step 2: Drain buffer
       │  └─ List<OtlpDataPoint> points = MetricsBuffer.drainAll()
       │     (Dequeues all buffered data points; buffer is now empty)
       │
       ├─ Step 3: Format as OTLP JSON
       │  └─ String jsonPayload = OtlpJsonFormatter.format(points)
       │     (Converts list of OtlpDataPoint objects to OTLP wire format)
       │
       ├─ Step 4: Compress (optional)
       │  └─ byte[] gzipPayload = GzipUtil.compress(jsonPayload.getBytes("UTF-8"))
       │     (Reduces size by ~80% for typical workloads)
       │
       ├─ Step 5: Build HTTP request
       │  └─ HttpURLConnection conn = (HttpURLConnection) new URL(otelEndpoint).openConnection()
       │     conn.setRequestMethod("POST")
       │     conn.setRequestProperty("Content-Type", "application/json")
       │     conn.setRequestProperty("Content-Encoding", "gzip")
       │     conn.setConnectTimeout(10000)  // 10 seconds
       │     conn.setReadTimeout(30000)     // 30 seconds
       │     (if otelHeaders is set)
       │     conn.setRequestProperty("Authorization", "Bearer my-token-123")
       │
       ├─ Step 6: Send payload
       │  └─ try (OutputStream os = conn.getOutputStream()) {
       │         os.write(gzipPayload)
       │         os.flush()
       │     }
       │
       ├─ Step 7: Receive response
       │  └─ int responseCode = conn.getResponseCode()
       │     if (responseCode == 200) {
       │         success!
       │     } else if (responseCode >= 500) {
       │         retry once
       │     } else {
       │         log warning, discard batch
       │     }
       │
       └─ Step 8: Handle errors (never throw to application)
          ├─ Connection refused → log, continue
          ├─ HTTP 4xx → log configuration error, continue
          ├─ HTTP 5xx → retry once, then log and continue
          └─ IOException → log, continue
```

#### D.4.6 OtlpHttpExporter

```java
package com.microsoft.sqlserver.jdbc.metrics.otel;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import java.util.zip.GZIPOutputStream;

/**
 * Exports buffered OTLP data points to an OTLP/HTTP endpoint.
 * Uses java.net.HttpURLConnection (no external dependency).
 */
public class OtlpHttpExporter {
    private static final Logger logger = Logger.getLogger(OtlpHttpExporter.class.getName());
    
    private final String endpoint;           // e.g., "http://otel-collector:4318/v1/metrics"
    private final int connectTimeoutMs;      // e.g., 10000
    private final int readTimeoutMs;         // e.g., 30000
    private final String authHeader;         // e.g., "Bearer token" or null
    private final OtlpJsonFormatter formatter;
    
    public OtlpHttpExporter(String endpoint, int connectTimeoutMs, int readTimeoutMs,
                           String authHeader) {
        this.endpoint = endpoint;
        this.connectTimeoutMs = connectTimeoutMs;
        this.readTimeoutMs = readTimeoutMs;
        this.authHeader = authHeader;
        this.formatter = new OtlpJsonFormatter();
    }
    
    /**
     * Export data points to the OTLP endpoint.
     * Failures are logged but never thrown to the caller.
     */
    public void export(List<OtlpDataPoint> dataPoints) {
        if (dataPoints == null || dataPoints.isEmpty()) {
            return;
        }
        
        try {
            // Step 1: Format as OTLP JSON
            String jsonPayload = formatter.format(dataPoints);
            byte[] jsonBytes = jsonPayload.getBytes(StandardCharsets.UTF_8);
            
            logger.fine("OTLP export: " + dataPoints.size() + " data points, " +
                       jsonBytes.length + " bytes (uncompressed)");
            
            // Step 2: Compress with gzip
            byte[] compressedBytes = gzip(jsonBytes);
            logger.fine("After gzip compression: " + compressedBytes.length + " bytes");
            
            // Step 3: Send HTTP POST
            sendPost(compressedBytes);
            
        } catch (Exception e) {
            // Never propagate exceptions; export failures are non-critical
            logger.log(Level.WARNING, "Failed to export OTLP metrics: " + e.getMessage(), e);
        }
    }
    
    private byte[] gzip(byte[] data) throws IOException {
        java.io.ByteArrayOutputStream baos = new java.io.ByteArrayOutputStream();
        try (GZIPOutputStream gzip = new GZIPOutputStream(baos)) {
            gzip.write(data);
        }
        return baos.toByteArray();
    }
    
    private void sendPost(byte[] payload) throws IOException {
        HttpURLConnection conn = null;
        try {
            // Step 1: Open connection
            URL url = new URL(endpoint);
            conn = (HttpURLConnection) url.openConnection();
            
            // Step 2: Configure request
            conn.setRequestMethod("POST");
            conn.setDoOutput(true);
            conn.setConnectTimeout(connectTimeoutMs);
            conn.setReadTimeout(readTimeoutMs);
            
            // Step 3: Set headers
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setRequestProperty("Content-Encoding", "gzip");
            conn.setRequestProperty("Content-Length", String.valueOf(payload.length));
            
            if (authHeader != null && !authHeader.isEmpty()) {
                conn.setRequestProperty("Authorization", authHeader);
            }
            
            // Step 4: Write payload
            try (OutputStream os = conn.getOutputStream()) {
                os.write(payload);
                os.flush();
            }
            
            // Step 5: Read response
            int responseCode = conn.getResponseCode();
            String responseMessage = conn.getResponseMessage();
            
            if (responseCode == 200 || responseCode == 204) {
                // Success
                logger.fine("OTLP export succeeded: HTTP " + responseCode);
                return;
            }
            
            if (responseCode >= 500 && responseCode < 600) {
                // Server error: retry once
                logger.warning("OTLP export server error (HTTP " + responseCode + 
                              "); retrying once...");
                retryPost(payload);
                return;
            }
            
            if (responseCode >= 400 && responseCode < 500) {
                // Client error: log and drop
                logger.warning("OTLP export client error (HTTP " + responseCode + 
                              " " + responseMessage + "); check endpoint URL and auth");
                return;
            }
            
            // Other errors
            logger.warning("OTLP export unexpected response: HTTP " + responseCode);
            
        } finally {
            if (conn != null) {
                conn.disconnect();
            }
        }
    }
    
    private void retryPost(byte[] payload) {
        try {
            // Wait 1 second before retry
            Thread.sleep(1000);
            
            HttpURLConnection conn = null;
            try {
                URL url = new URL(endpoint);
                conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setDoOutput(true);
                conn.setConnectTimeout(connectTimeoutMs);
                conn.setReadTimeout(readTimeoutMs);
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setRequestProperty("Content-Encoding", "gzip");
                conn.setRequestProperty("Content-Length", String.valueOf(payload.length));
                
                if (authHeader != null) {
                    conn.setRequestProperty("Authorization", authHeader);
                }
                
                try (OutputStream os = conn.getOutputStream()) {
                    os.write(payload);
                    os.flush();
                }
                
                int responseCode = conn.getResponseCode();
                if (responseCode == 200 || responseCode == 204) {
                    logger.fine("OTLP export retry succeeded: HTTP " + responseCode);
                } else {
                    logger.warning("OTLP export retry failed: HTTP " + responseCode + 
                                  "; giving up");
                }
            } finally {
                if (conn != null) {
                    conn.disconnect();
                }
            }
        } catch (Exception e) {
            logger.warning("OTLP export retry interrupted: " + e.getMessage());
        }
    }
}
```

#### D.4.7 Real-World Endpoint Examples

##### Example 1: Docker OTel Collector (Local Development)

**Docker Compose:**

```yaml
version: '3.8'
services:
  otel-collector:
    image: otel/opentelemetry-collector:latest
    ports:
      - "4318:4318"  # OTLP/HTTP
    volumes:
      - ./otel-collector-config.yaml:/etc/otel-collector-config.yaml
    command:
      - "--config=/etc/otel-collector-config.yaml"
  
  jaeger:
    image: jaegertracing/all-in-one:latest
    ports:
      - "16686:16686"  # Jaeger UI
```

**OTel Collector Config (`otel-collector-config.yaml`):**

```yaml
receivers:
  otlp:
    protocols:
      http:
        endpoint: 0.0.0.0:4318

exporters:
  jaeger:
    endpoint: jaeger:14250
    tls:
      insecure: true

service:
  pipelines:
    metrics:
      receivers: [otlp]
      exporters: [jaeger]
```

**JDBC Connection String:**

```
jdbc:sqlserver://sqlserver:1433;
  databaseName=testdb;
  otelEndpoint=http://otel-collector:4318/v1/metrics;
  otelExportInterval=10
```

##### Example 2: Azure Monitor via Application Insights

**Prerequisites:** Azure Monitor OpenTelemetry Distro running as sidecar

**JDBC Connection String:**

```
jdbc:sqlserver://myserver.database.windows.net:1433;
  databaseName=mydb;
  otelEndpoint=http://localhost:8888/metrics;
  otelServiceName=order-service;
  otelResourceAttributes=app.name=order-api,app.version=1.2.0
```

(Azure Monitor Agent translates OTLP to Azure Monitor format internally)

##### Example 3: Datadog (SaaS)

**Prerequisites:** Datadog Agent running locally

**JDBC Connection String:**

```
jdbc:sqlserver://prod-db.internal:1433;
  databaseName=orders;
  otelEndpoint=http://localhost:4318/v1/metrics;
  otelServiceName=order-processor;
  otelHeaders=DD-API-KEY=YOUR_API_KEY
```

#### D.4.8 Debugging OTLP Export Issues

**Enable verbose logging:**

```java
java.util.logging.Logger logger = java.util.logging.Logger.getLogger("com.microsoft.sqlserver");
logger.setLevel(java.util.logging.Level.FINE);

// Add console handler
java.util.logging.ConsoleHandler handler = new java.util.logging.ConsoleHandler();
handler.setLevel(java.util.logging.Level.FINE);
logger.addHandler(handler);
```

**Check logs for these patterns:**

| Log Message | Meaning | Remediation |
|---|---|---|
| `OTLP export: 10 data points, 5000 bytes (uncompressed)` | ✅ Metrics are flowing | Normal |
| `After gzip compression: 1200 bytes` | ✅ Compression working (76% reduction) | Normal |
| `OTLP export succeeded: HTTP 200` | ✅ Server accepted batch | Normal |
| `Failed to export OTLP metrics: Connection refused` | ❌ Endpoint not reachable | Check endpoint URL, firewall, server running |
| `OTLP export client error (HTTP 401)` | ❌ Authentication failure | Check `otelHeaders` (Bearer token, API key) |
| `OTLP export client error (HTTP 404)` | ❌ Wrong endpoint path | Verify path includes `/v1/metrics` |
| `OTLP export server error (HTTP 500)` | ⚠️ Endpoint error; retrying | Check server logs; driver will retry once |

**Test connectivity from command line:**

```bash
# Simple test POST to endpoint
curl -X POST \
  -H "Content-Type: application/json" \
  -H "Content-Encoding: gzip" \
  --data-binary @payload.json.gz \
  http://otel-collector:4318/v1/metrics
```
---

<a id="d5-solution-3-implementation-details"></a>
### D.5 Solution 3 Implementation Details

The following subsections were previously inline in Section 4.3.

#### D.5.1 Detailed Implementation

##### Log Record Format Design

The driver's existing `PerformanceLog` JUL emission is the source. For Solution 3 the recommendation is to format each record as a **single-line JSON object** so the collector can use a `json_parser` operator (more robust than regex) and so records survive log rotation cleanly.

Example record (one per line in the log file):

```json
{"ts":"2026-05-18T14:32:11.412Z","activity":"STATEMENT_EXECUTE","conn":100,"stmt":10,"duration_ms":5,"server":{"logical_reads":200,"physical_reads":0,"elapsed_ms":3,"cpu_ms":2},"error":null,"service":"order-service","driver_version":"13.2.0"}
```

Key design rules:

1. **Single line per record.** No pretty-printing; newline terminator only between records.
2. **Always-present fields.** `ts`, `activity`, `conn`, `stmt` are always emitted; absent metrics become `null` rather than missing keys.
3. **ISO-8601 UTC timestamp.** Milliseconds precision, `Z` suffix, so collector ingestion is timezone-safe.
4. **Stable field names.** Treated as a public contract; changes require driver version bump and parallel-emit period.
5. **No PII.** SQL text, parameter values, server hostnames, and user names are never written.

##### File Layout and Rotation

```
/var/log/jdbc/
├── performance.log              ← active file the driver writes to
├── performance.log.1            ← rotated by java.util.logging.FileHandler
├── performance.log.2
└── performance.log.3
```

Driver-side rotation via standard `FileHandler` settings (configured in `logging.properties`):

| Setting | Recommended value | Reason |
|---------|-------------------|--------|
| `pattern` | `/var/log/jdbc/performance.log` (or `%t/jdbc/performance.log`) | Predictable path collector can `include:` glob |
| `limit` | `52428800` (50 MB) | Bound disk per file; collector keeps up easily |
| `count` | `5` | Five rotations = 250 MB ceiling per JVM |
| `append` | `true` | Preserve records across restarts |
| `encoding` | `UTF-8` | Match collector default |
| `formatter` | Custom JSON-line formatter (see below) | Deterministic single-line output |

The collector's `filelog` receiver is rotation-aware: it tracks the file by inode/fingerprint, not by name, so renaming `performance.log` → `performance.log.1` during rotation does not lose records.

##### JUL Formatter (driver-side)

```java
package com.microsoft.sqlserver.jdbc.diagnostics;

import java.time.Instant;
import java.util.logging.Formatter;
import java.util.logging.LogRecord;

public final class JdbcPerformanceJsonFormatter extends Formatter {

    @Override
    public String format(LogRecord r) {
        // The driver puts a structured payload object into LogRecord.parameters[0].
        // The formatter only renders it; it does not compute any field values.
        PerformanceLogPayload p = (PerformanceLogPayload) r.getParameters()[0];
        StringBuilder b = new StringBuilder(256);
        b.append('{');
        appendStr(b, "ts", Instant.ofEpochMilli(r.getMillis()).toString()); b.append(',');
        appendStr(b, "activity", p.activity.name());                       b.append(',');
        appendNum(b, "conn", p.connectionId);                              b.append(',');
        appendNum(b, "stmt", p.statementId);                               b.append(',');
        appendNum(b, "duration_ms", p.durationMs);                         b.append(',');
        appendServerStats(b, p.serverStats);                               b.append(',');
        appendErr(b, "error", p.exception);                                b.append(',');
        appendStr(b, "service", p.serviceName);                            b.append(',');
        appendStr(b, "driver_version", p.driverVersion);
        b.append('}').append('\n');
        return b.toString();
    }

    // append* helpers escape values per RFC 8259.
}
```

The formatter is deliberately allocation-light and avoids any third-party JSON library so it can live in the core driver with zero dependencies.

##### Lifecycle End-to-End

```
JDBC work completes
  │
  ├── PerformanceLog.Scope closes
  │   └── builds PerformanceLogPayload (already part of the driver)
  │       └── LOG.log(Level.INFO, "perf", new Object[]{payload})
  │
  ├── JUL FileHandler receives LogRecord
  │   └── JdbcPerformanceJsonFormatter.format() → single-line JSON
  │       └── flushed to /var/log/jdbc/performance.log
  │
  ├── OTel Collector (separate process)
  │   ├── filelog receiver tails /var/log/jdbc/performance.log*
  │   ├── json_parser operator parses each line into structured body
  │   ├── (optional) metricsgeneration / transform processor extracts numeric values
  │   └── otlp exporter ships to chosen backend (Tempo, Mimir, App Insights, Datadog, etc.)
  │
  └── Backend stores metrics, applies dashboards, alerts.
```

#### D.5.2 JUL Logger Configuration

##### `logging.properties` (driver-side)

```properties
# Activate the dedicated perf logger
com.microsoft.sqlserver.jdbc.PerformanceLog.level=INFO
com.microsoft.sqlserver.jdbc.PerformanceLog.useParentHandlers=false
com.microsoft.sqlserver.jdbc.PerformanceLog.handlers=java.util.logging.FileHandler

# File handler for the perf log only
java.util.logging.FileHandler.pattern=/var/log/jdbc/performance.log
java.util.logging.FileHandler.limit=52428800
java.util.logging.FileHandler.count=5
java.util.logging.FileHandler.append=true
java.util.logging.FileHandler.encoding=UTF-8
java.util.logging.FileHandler.formatter=com.microsoft.sqlserver.jdbc.diagnostics.JdbcPerformanceJsonFormatter
```

Start the JVM with `-Djava.util.logging.config.file=/etc/jdbc/logging.properties`.

##### Programmatic Equivalent

```java
Logger perf = Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceLog");
perf.setUseParentHandlers(false);
FileHandler fh = new FileHandler("/var/log/jdbc/performance.log", 52_428_800, 5, true);
fh.setEncoding("UTF-8");
fh.setFormatter(new JdbcPerformanceJsonFormatter());
perf.addHandler(fh);
perf.setLevel(Level.INFO);
```

#### D.5.3 Collector `filelog` Receiver Configuration

##### Minimal Config (JSON-line records)

```yaml
receivers:
  filelog/jdbc:
    include: [ /var/log/jdbc/performance.log* ]
    start_at: end                # tail; do not re-ingest historical lines on collector restart
    include_file_name: false
    include_file_path: false
    operators:
      - type: json_parser
        timestamp:
          parse_from: attributes.ts
          layout: '%Y-%m-%dT%H:%M:%S.%LZ'
        severity:
          parse_from: attributes.error
          mapping:
            error: { not_equals: null }
      - type: move
        from: attributes.activity
        to: attributes["db.jdbc.activity"]
      - type: move
        from: attributes.conn
        to: attributes["db.connection.id"]
      - type: move
        from: attributes.stmt
        to: attributes["db.statement.id"]
```

##### Turning Log Records into Metrics

The driver emits a log record per JDBC operation; the collector turns those into OTLP metrics. Two common patterns:

**Pattern A — `count`/`sum` connector (recommended for OTel Collector ≥ 0.96):**

```yaml
connectors:
  count/jdbc:
    logs:
      jdbc.client.statement.execute.count:
        description: Count of executed JDBC statements
        conditions:
          - attributes["db.jdbc.activity"] == "STATEMENT_EXECUTE"

  sum/jdbc:
    logs:
      jdbc.client.statement.execute.duration_ms.total:
        value: attributes.duration_ms
        conditions:
          - attributes["db.jdbc.activity"] == "STATEMENT_EXECUTE"

service:
  pipelines:
    logs/jdbc:
      receivers:  [filelog/jdbc]
      processors: []
      exporters:  [count/jdbc, sum/jdbc]
    metrics/jdbc:
      receivers:  [count/jdbc, sum/jdbc]
      processors: [batch]
      exporters:  [otlp]
```

**Pattern B — `transform` processor + `logs-to-metrics` (broader collector compatibility):**

```yaml
processors:
  transform/jdbc:
    log_statements:
      - context: log
        statements:
          - set(attributes["metric.name"], "jdbc.client.statement.execute.duration_ms")
          - set(attributes["metric.value"], attributes["duration_ms"])
```

##### Attribute Mapping (driver field → OTel attribute)

| Driver JSON field | OTel attribute key | Notes |
|-------------------|--------------------|-------|
| `conn` | `db.connection.id` | Long |
| `stmt` | `db.statement.id` | Long |
| `activity` | `db.jdbc.activity` | Enum string (`STATEMENT_EXECUTE`, ...) |
| `duration_ms` | metric value (histogram) | Long |
| `server.logical_reads` | metric value (counter) | Long |
| `server.physical_reads` | metric value (counter) | Long |
| `server.elapsed_ms` | metric value (histogram) | Long |
| `server.cpu_ms` | metric value (histogram) | Long |
| `error` | `error.type` | Java class name or `null` |
| `service` | `service.name` (resource) | Promoted to resource via `resource` processor |
| `driver_version` | `library.version` (resource) | Promoted via `resource` processor |

#### D.5.4 Real-World Pipeline Examples

##### Example 1: Docker Compose, Local Dev

```yaml
# docker-compose.yml
services:
  app:
    image: my-java-app:dev
    volumes:
      - jdbc-logs:/var/log/jdbc
    environment:
      JAVA_OPTS: "-Djava.util.logging.config.file=/etc/jdbc/logging.properties"

  collector:
    image: otel/opentelemetry-collector-contrib:latest
    volumes:
      - jdbc-logs:/var/log/jdbc:ro
      - ./otel-config.yaml:/etc/otelcol/config.yaml:ro
    command: ["--config=/etc/otelcol/config.yaml"]

volumes:
  jdbc-logs:
```

The app writes; the collector reads the same shared volume read-only. Restarting either side does not lose records (collector tracks file fingerprint).

##### Example 2: Kubernetes DaemonSet

```yaml
# Per-node collector tails any pod's /var/log/jdbc/*.log
apiVersion: apps/v1
kind: DaemonSet
metadata: { name: otel-jdbc-tailer }
spec:
  template:
    spec:
      containers:
        - name: otelcol
          image: otel/opentelemetry-collector-contrib:latest
          volumeMounts:
            - name: varlog
              mountPath: /var/log
              readOnly: true
      volumes:
        - name: varlog
          hostPath: { path: /var/log }
```

Each application pod writes JDBC perf records to a known emptyDir or hostPath; the DaemonSet collector aggregates across the node.

##### Example 3: Sidecar Pattern

```yaml
# In the same Pod as the app
- name: app
  image: my-java-app:1.0
  volumeMounts:
    - { name: jdbc-logs, mountPath: /var/log/jdbc }

- name: otel-sidecar
  image: otel/opentelemetry-collector-contrib:latest
  volumeMounts:
    - { name: jdbc-logs, mountPath: /var/log/jdbc, readOnly: true }
    - { name: otel-config, mountPath: /etc/otelcol }

volumes:
  - name: jdbc-logs
    emptyDir: {}
```

Tight coupling, low operational scope, ideal when one team owns both app and observability.

#### D.5.5 Debugging the File Pipeline

| Symptom | First thing to check |
|---------|----------------------|
| File created but empty | Driver-side: confirm `PerformanceLog` level is `INFO` and the perf logger is not inheriting `useParentHandlers=true` (which would route to console only). |
| File grows but no metrics at backend | Collector-side: run `otelcol validate --config=...`; check `filelog` receiver is including the path and `start_at` is not `end` on first run. |
| Some records missing | Confirm `FileHandler.limit`/`count` are not too small for throughput; check `inode_marker` storage extension is enabled so the collector survives restarts. |
| Parse errors in collector | Tail collector logs for `error parsing log line`; verify driver formatter is producing one record per line and not splitting JSON across lines. |
| Duplicate metrics | Collector restarted with `start_at: beginning` instead of `end`; or two collectors tailing the same file without coordination. |
| Timestamp skew | Verify `json_parser.timestamp.layout` matches `%Y-%m-%dT%H:%M:%S.%LZ` exactly (especially the `Z` literal). |
| High CPU on collector | Switch from `regex_parser` to `json_parser` (typically 5–10× faster); enable `batch` processor before the exporter. |

##### Inspecting a Record Manually

```bash
# Last record
tail -n 1 /var/log/jdbc/performance.log | jq

# Count records by activity in the last rotation
jq -r '.activity' /var/log/jdbc/performance.log | sort | uniq -c

# Validate one line is well-formed JSON
tail -n 1 /var/log/jdbc/performance.log | python -m json.tool
```

##### Driver-Side Self-Diagnostic

```java
Logger.getLogger("com.microsoft.sqlserver.jdbc.PerformanceLog").setLevel(Level.FINE);
// Logs the in-memory PerformanceLogPayload before the FileHandler emits it,
// useful for confirming the driver is producing records when the file appears stale.
```

---

<a id="d6-solution-4-implementation-details"></a>
### D.6 Solution 4 Implementation Details

Solution 4 keeps Section 4.4 short because it shares its overall data path
with Solution 1 (see [Appendix D.1](#d3-solution-1-implementation-details)
for the full bridge walkthrough). This appendix focuses on what is
*different* in Solution 4: the absence of an SPI, the direct dependency on
the OTel SDK, and the bootstrap rules that let the driver coexist with an
application-owned `GlobalOpenTelemetry`.

#### D.6.1 Example Code Shape

`OpenTelemetryPerformanceCallback` lives in `com.microsoft.sqlserver.jdbc.telemetry`
and is compiled as part of `mssql-jdbc.jar`. There is no SPI lookup -
`OtelBootstrap` instantiates it directly.

```java
package com.microsoft.sqlserver.jdbc.telemetry;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.PerformanceLogPayload;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.metrics.LongCounter;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.api.metrics.Meter;

final class OpenTelemetryPerformanceCallback implements PerformanceLogCallback {

    private static final String SCOPE = "com.microsoft.sqlserver.jdbc";

    private final LongHistogram execDuration;
    private final LongHistogram prepDuration;
    private final LongHistogram serverElapsed;
    private final LongHistogram serverCpu;
    private final LongCounter   logicalReads;
    private final LongCounter   physicalReads;
    private final LongCounter   writes;
    private final LongCounter   errors;

    OpenTelemetryPerformanceCallback(OpenTelemetry otel) {
        Meter meter = otel.getMeter(SCOPE);
        this.execDuration   = meter.histogramBuilder("db.client.execute.duration").ofLongs().setUnit("ms").build();
        this.prepDuration   = meter.histogramBuilder("db.client.prepare.duration").ofLongs().setUnit("ms").build();
        this.serverElapsed  = meter.histogramBuilder("db.server.elapsed_time").ofLongs().setUnit("ms").build();
        this.serverCpu      = meter.histogramBuilder("db.server.cpu_time").ofLongs().setUnit("ms").build();
        this.logicalReads   = meter.counterBuilder("db.server.io.logical_reads").build();
        this.physicalReads  = meter.counterBuilder("db.server.io.physical_reads").build();
        this.writes         = meter.counterBuilder("db.server.io.writes").build();
        this.errors         = meter.counterBuilder("db.client.errors").build();
    }

    @Override
    public void onEvent(PerformanceLogPayload p) {
        Attributes attrs = OtelAttributeBuilder.build(p);
        switch (p.activity) {
            case EXECUTE:
                execDuration.record(p.durationMs, attrs);
                if (p.exception != null) {
                    errors.add(1, attrs.toBuilder()
                            .put(AttributeKey.stringKey("error.type"),
                                 p.exception.getClass().getSimpleName())
                            .build());
                }
                break;
            case PREPARE:
            case PREPEXEC:
                prepDuration.record(p.durationMs, attrs);
                break;
            case TDS_EXECSTATS:
                if (p.server != null) {
                    serverElapsed.record(p.server.elapsedMs, attrs);
                    serverCpu.record(p.server.cpuMs, attrs);
                    logicalReads.add(p.server.logicalReads, attrs);
                    physicalReads.add(p.server.physicalReads, attrs);
                    writes.add(p.server.writes, attrs);
                }
                break;
            default:
                // Other activities ignored for metrics; trace correlation handled separately.
        }
    }
}
```

The `OtelAttributeBuilder` centralizes attribute keys:

```java
final class OtelAttributeBuilder {
    private static final AttributeKey<String> DB_SYSTEM       = AttributeKey.stringKey("db.system");
    private static final AttributeKey<Long>   CONNECTION_ID   = AttributeKey.longKey("db.connection_id");
    private static final AttributeKey<Long>   STATEMENT_ID    = AttributeKey.longKey("db.statement.id");
    private static final AttributeKey<String> STATEMENT_KIND  = AttributeKey.stringKey("db.statement.kind");
    private static final AttributeKey<String> SERVER_ADDRESS  = AttributeKey.stringKey("server.address");

    static Attributes build(PerformanceLogPayload p) {
        return Attributes.builder()
            .put(DB_SYSTEM, "mssql")
            .put(CONNECTION_ID, p.connectionId)
            .put(STATEMENT_ID, p.statementId)
            .put(STATEMENT_KIND, p.statementKind == null ? "unknown" : p.statementKind)
            .put(SERVER_ADDRESS, p.serverAddress)
            .build();
    }
}
```

#### D.6.2 Detailed Implementation

`OtelBootstrap` is invoked at most once per JVM the first time a
`SQLServerConnection` is opened with `otelEndpoint` set. It decides whether
to reuse an existing `GlobalOpenTelemetry` or build a private SDK.

```text
SQLServerConnection.connect()
   |
   v
if (urlProps.containsKey("otelEndpoint")) {
    OtelBootstrap.ensureInitialized(urlProps);
}

OtelBootstrap.ensureInitialized(props):
   if INITIALIZED.compareAndSet(false, true):
       OpenTelemetry global = GlobalOpenTelemetry.get();
       if (isNoop(global)) {
           // Build private SDK
           Resource resource = Resource.getDefault().merge(buildResourceFromProps(props));

           OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
               .setEndpoint(props.getProperty("otelEndpoint"))
               .setHeaders(parseHeaders(props.getProperty("otelHeaders")))
               .setCompression("gzip")
               .setTimeout(Duration.ofSeconds(10))
               .build();

           PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
               .setInterval(Duration.ofSeconds(intOrDefault(props, "otelExportInterval", 60)))
               .build();

           SdkMeterProvider mp = SdkMeterProvider.builder()
               .setResource(resource)
               .registerMetricReader(reader)
               .build();

           OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
               .setMeterProvider(mp)
               .buildAndRegisterGlobal();

           Runtime.getRuntime().addShutdownHook(new Thread(() -> {
               mp.shutdown().join(10, TimeUnit.SECONDS);
           }, "mssql-jdbc-otel-shutdown"));

           CURRENT = sdk;
       } else {
           // Reuse app/agent-owned global
           CURRENT = global;
       }

       PerformanceLog.addCallback(new OpenTelemetryPerformanceCallback(CURRENT));
```

Detection rule for "app already owns OTel" (`isNoop`):

```java
private static boolean isNoop(OpenTelemetry o) {
    // GlobalOpenTelemetry.get() returns OpenTelemetry.noop() until something registers.
    return o == OpenTelemetry.noop()
        || o.getClass().getName().contains("Noop");
}
```

This is identical to how `opentelemetry-instrumentation-api` decides whether
its auto-instrumentation should bootstrap an SDK, so the driver inherits the
same composition rules used by `opentelemetry-javaagent`.

#### D.6.3 OtelBootstrap Class Details

Full class for reference:

```java
package com.microsoft.sqlserver.jdbc.telemetry;

import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributeKey;

import java.time.Duration;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.sqlserver.jdbc.PerformanceLog;

public final class OtelBootstrap {

    private static final Logger LOG = Logger.getLogger(OtelBootstrap.class.getName());
    private static final AtomicBoolean INITIALIZED = new AtomicBoolean(false);
    private static volatile OpenTelemetry CURRENT;

    private OtelBootstrap() {}

    public static void ensureInitialized(Properties props) {
        if (!INITIALIZED.compareAndSet(false, true)) return;
        try {
            OpenTelemetry global = GlobalOpenTelemetry.get();
            if (isNoop(global)) {
                CURRENT = buildAndRegister(props);
                LOG.fine("mssql-jdbc registered its own OpenTelemetry SDK");
            } else {
                CURRENT = global;
                LOG.fine("mssql-jdbc is reusing the application's GlobalOpenTelemetry");
            }
            PerformanceLog.addCallback(new OpenTelemetryPerformanceCallback(CURRENT));
        } catch (Throwable t) {
            INITIALIZED.set(true); // do not retry
            LOG.log(Level.SEVERE,
                    "OTel bootstrap failed; JDBC will continue without metrics export", t);
        }
    }

    private static OpenTelemetrySdk buildAndRegister(Properties props) {
        OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(props.getProperty("otelEndpoint"))
            .setHeaders(parseHeaders(props.getProperty("otelHeaders")))
            .setCompression("gzip")
            .setTimeout(Duration.ofSeconds(10))
            .build();

        PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
            .setInterval(Duration.ofSeconds(parseLong(props, "otelExportInterval", 60)))
            .build();

        SdkMeterProvider mp = SdkMeterProvider.builder()
            .setResource(buildResource(props))
            .registerMetricReader(reader)
            .build();

        OpenTelemetrySdk sdk = OpenTelemetrySdk.builder()
            .setMeterProvider(mp)
            .buildAndRegisterGlobal();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try { mp.shutdown().join(10, TimeUnit.SECONDS); } catch (Throwable ignored) {}
        }, "mssql-jdbc-otel-shutdown"));

        return sdk;
    }

    private static Resource buildResource(Properties props) {
        Attributes.Builder b = Attributes.builder()
            .put(AttributeKey.stringKey("service.name"),
                 props.getProperty("otelServiceName", "mssql-jdbc"));
        String extra = props.getProperty("otelResourceAttributes");
        if (extra != null) {
            for (String kv : extra.split(",")) {
                int eq = kv.indexOf('=');
                if (eq > 0) b.put(AttributeKey.stringKey(kv.substring(0, eq).trim()),
                                  kv.substring(eq + 1).trim());
            }
        }
        return Resource.getDefault().merge(Resource.create(b.build()));
    }

    private static Map<String, String> parseHeaders(String s) {
        return s == null ? Map.of()
            : java.util.Arrays.stream(s.split(","))
                .map(p -> p.split("=", 2))
                .filter(a -> a.length == 2)
                .collect(java.util.stream.Collectors.toMap(a -> a[0].trim(), a -> a[1].trim()));
    }

    private static long parseLong(Properties p, String key, long dflt) {
        try { return Long.parseLong(p.getProperty(key, Long.toString(dflt))); }
        catch (NumberFormatException e) { return dflt; }
    }

    private static boolean isNoop(OpenTelemetry o) {
        return o == OpenTelemetry.noop() || o.getClass().getName().contains("Noop");
    }
}
```

Notes:

- `INITIALIZED` is set even on failure to avoid a hot-path retry storm.
- The shutdown hook is registered only when the driver itself owns the SDK;
  an app-supplied SDK is left untouched (the application is responsible for
  its own lifecycle).
- `parseHeaders` accepts `Key1=Value1,Key2=Value2`. Bearer tokens are not
  echoed in any log output (the JUL bootstrap logger never logs the parsed
  map).

#### D.6.4 OpenTelemetry SDK Configuration Mechanics

When the driver owns the SDK (no `GlobalOpenTelemetry` set by the app):

| Connection Property | Effect | OTel SDK Equivalent |
|---|---|---|
| `otelEndpoint` | Required. URL of the OTLP/HTTP metrics endpoint. | `OtlpHttpMetricExporter.setEndpoint(...)` |
| `otelHeaders` | Comma-separated `K=V` pairs added to every request. | `OtlpHttpMetricExporter.setHeaders(map)` |
| `otelExportInterval` | Seconds between exports. Default 60. | `PeriodicMetricReader.setInterval(...)` |
| `otelBatchSize` | Soft cap on points per export (advisory; OTel SDK does not honor a hard cap, but the reader respects the interval). | n/a (informational) |
| `otelSamplingRate` | View-level sampling for histograms. | `View.builder().setAggregation(ExplicitBucketHistogramAggregation.create(...))` + custom `Filter` |
| `otelServiceName` | Sets `service.name` resource attribute. | `Resource.create(Attributes.of(service.name, ...))` |
| `otelResourceAttributes` | Adds arbitrary resource attributes. | `Resource.create(...)` |

When the app already owns the SDK (e.g., `opentelemetry-javaagent` is on the
CLI, or the app called `OpenTelemetrySdk.builder().buildAndRegisterGlobal()`):

- `otelEndpoint`, `otelHeaders`, `otelExportInterval` are **ignored**. The
  driver does not try to mutate the app's exporter.
- `otelServiceName`, `otelResourceAttributes` are also ignored; resource
  attributes come from the app's SDK configuration.
- The driver still uses its own `Meter("com.microsoft.sqlserver.jdbc")`
  scope, so metric names are deterministic regardless of who owns the SDK.

A warning is logged once at `INFO` level enumerating which properties were
ignored, to help users debug "I set otelEndpoint and nothing happens but
metrics still show up at a different URL" scenarios.

#### D.6.5 Real-World Deployment Examples

##### Example 1: Plain JVM, driver-owned SDK

```bash
# No javaagent, just the driver on the classpath
java -cp mssql-jdbc-13.x.jar:app.jar com.example.App
```

```text
jdbc:sqlserver://prod-sql;serverExecutionStatistics=all;otelEndpoint=https://otel.example.com/v1/metrics;otelHeaders=Authorization=Bearer eyJ...;otelServiceName=order-service
```

Driver bootstraps its own SDK and ships metrics directly to
`otel.example.com`.

##### Example 2: Spring Boot app with `opentelemetry-javaagent`

```bash
java -javaagent:/opt/otel/opentelemetry-javaagent-2.x.jar      -Dotel.exporter.otlp.endpoint=https://collector.svc:4318      -Dotel.service.name=order-service      -jar order-service.jar
```

```text
jdbc:sqlserver://prod-sql;serverExecutionStatistics=all
```

`otelEndpoint` is **not** set on the JDBC URL. The agent already created a
`GlobalOpenTelemetry`. The driver detects it, reuses it, and emits metrics
under `service.name=order-service` to the collector configured for the
agent. No driver-side OTLP HTTP traffic.

##### Example 3: Azure Spring Apps with `applicationinsights-agent`

```bash
java -javaagent:/agents/applicationinsights-agent.jar      -Dapplicationinsights.connection.string="InstrumentationKey=..."      -jar order-service.jar
```

```text
jdbc:sqlserver://prod-sql;serverExecutionStatistics=all
```

`applicationinsights-agent` publishes a `GlobalOpenTelemetry` that routes
metrics to Azure Monitor. The driver reuses it and JDBC metrics appear
alongside HTTP and JVM metrics under the same App Insights resource.

##### Example 4: Kubernetes sidecar collector

```text
jdbc:sqlserver://sql-prod.default.svc;serverExecutionStatistics=all;otelEndpoint=http://localhost:4318/v1/metrics;otelExportInterval=30
```

Driver writes to a local OTel Collector sidecar via loopback; the collector
fans out to Prometheus + Loki + a SaaS backend.

#### D.6.6 Debugging

##### Q: Driver bootstrapped its own SDK but I have a javaagent

Cause: the agent loaded *after* the driver's first connection. `OtelBootstrap`
already captured `OpenTelemetry.noop()` and committed to building a private
SDK.

Fix: ensure the `-javaagent` argument is present (the JVM loads agents
before main()). If you cannot guarantee load order, set
`otelEndpoint` to the same URL the agent uses; both SDKs will write to the
same backend with duplicate metrics (acceptable for some pipelines, but
prefer fixing the load order).

##### Q: Metrics never arrive

Steps:

1. Enable JUL fine logging:
   ```text
   com.microsoft.sqlserver.jdbc.telemetry.OtelBootstrap.level = FINE
   ```
   Look for either "registered its own OpenTelemetry SDK" or "reusing the
   application's GlobalOpenTelemetry".
2. If neither line appears, `otelEndpoint` is not set on the JDBC URL.
3. If the driver registered its own SDK, enable OTel SDK debug:
   ```text
   -Dotel.java.global-autoconfigure.enabled=false
   -Dio.opentelemetry.exporter.otlp.internal.debug=true
   ```
   Errors will appear with prefix `OtlpHttpMetricExporter`.

##### Q: `NoClassDefFoundError: io.opentelemetry.api.OpenTelemetry`

Cause: a user excluded OTel transitively (`<exclusions>` in their POM). With
Solution 4, OTel is a required dependency of the driver; excluding it
breaks the driver.

Fix: either remove the exclusion, or switch to Solution 1's
`mssql-jdbc-opentelemetry-all` artifact which makes the OTel dependency
explicit at the app level.

##### Q: Dependency conflict: my app uses OTel 1.30 but mssql-jdbc pulled 1.40

Cause: Maven nearest-wins resolution chose the driver's transitive
version.

Fix: pin OTel in the app's `<dependencyManagement>` to the version the app
needs. The driver's bridge code uses only the stable `opentelemetry-api`
surface (no SDK-internal APIs), so minor version mismatches between
`api` and `sdk` are tolerated.

##### Q: Memory growth on long-lived JVMs

Cause: high-cardinality attributes (e.g. raw SQL text). The driver never
emits raw SQL as an attribute, but a user-registered callback might.

Fix: review any custom `PerformanceLogCallback` for attribute cardinality;
use `db.statement.kind` (a small enum) instead of `db.statement.text`.



