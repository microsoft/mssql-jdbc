# Statement Performance Metrics

This document describes the performance metrics implemented for statement execution in the Microsoft JDBC Driver for SQL Server.

---

## Table of Contents

1. [Overview](#overview)
2. [All Performance Activities](#all-performance-activities)
3. [Detailed Activity Descriptions](#detailed-activity-descriptions)
4. [Coverage by Class](#coverage-by-class)
5. [SQL Server Stored Procedures](#sql-server-stored-procedures)
6. [Implementation Details](#implementation-details)
7. [Legend](#legend)

---

## Overview

Performance metrics help developers and DBAs understand where time is spent during statement execution. The driver tracks multiple activities at different phases of execution:

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                        Statement Execution Timeline                          │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                              │
│  [Statement Created]                                                         │
│        │                                                                     │
│        ▼                                                                     │
│  ┌─────────────────────────────────────┐                                    │
│  │ STATEMENT_CREATION_TO_FIRST_PACKET  │  Build SQL, parameters, TDS packet │
│  └─────────────────────────────────────┘                                    │
│        │                                                                     │
│        ▼                                                                     │
│  ┌─────────────────────────────────────┐                                    │
│  │ STATEMENT_FIRST_PACKET_TO_FIRST_    │  Network round-trip to server      │
│  │ RESPONSE                            │                                    │
│  └─────────────────────────────────────┘                                    │
│        │                                                                     │
│        ▼                                                                     │
│  ┌─────────────────────────────────────┐                                    │
│  │ STATEMENT_PREPARE / PREPEXEC /      │  Server-side execution             │
│  │ EXECUTE                             │                                    │
│  └─────────────────────────────────────┘                                    │
│        │                                                                     │
│        ▼                                                                     │
│  [Results Available]                                                         │
│                                                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## All Performance Activities

| Activity | Description | When Triggered |
|----------|-------------|----------------|
| `STATEMENT_CREATION_TO_FIRST_PACKET` | Time from statement execution call until TDS packet is ready to send | All statement executions |
| `STATEMENT_FIRST_PACKET_TO_FIRST_RESPONSE` | Network time: packet sent → first response received | All statement executions |
| `STATEMENT_PREPARE` | Time for `sp_prepare` only (no execution) | Only when `prepareMethod=prepare` |
| `STATEMENT_PREPEXEC` | Combined prepare+execute time using `sp_prepexec` | Default prepare method when statement needs preparation |
| `STATEMENT_EXECUTE` | Execution time only (no prepare) | When using `sp_execute`, `sp_executesql`, or direct SQL |

---

## Detailed Activity Descriptions

### 1. STATEMENT_CREATION_TO_FIRST_PACKET

**What it measures:** Time spent on the client side preparing the request before sending to server.

**Includes:**
- Parameter binding and type resolution
- SQL string preparation
- TDS packet construction
- Encryption metadata handling (if AE enabled)

**Where tracked:**
- `SQLServerStatement.doExecuteStatement()` - Regular statements
- `SQLServerStatement.doExecuteCursored()` - Cursor-based execution
- `SQLServerStatement.doExecuteStatementBatch()` - Batch statements
- `SQLServerPreparedStatement.doExecutePreparedStatement()` - Prepared statements
- `SQLServerPreparedStatement.doExecutePreparedStatementBatch()` - Prepared batch
- `SQLServerPreparedStatement.doExecuteExecMethodBatchCombined()` - Temp table batch

---

### 2. STATEMENT_FIRST_PACKET_TO_FIRST_RESPONSE

**What it measures:** Network round-trip time from sending the request to receiving the first response.

**Includes:**
- Network latency (client → server → client)
- Server queuing time
- Initial server processing

**Where tracked:** Same locations as `STATEMENT_CREATION_TO_FIRST_PACKET`

---

### 3. STATEMENT_PREPARE

**What it measures:** Time to prepare a statement using `sp_prepare` stored procedure.

**When used:** Only when connection property `prepareMethod=prepare` is set (NOT the default).

**Where tracked:**
- `SQLServerPreparedStatement.doPrep()` - The ONLY location

**Why only here?** This is the only place where pure `sp_prepare` (prepare-only, no execution) is called. The default `sp_prepexec` combines prepare and execute, making separation impossible.

---

### 4. STATEMENT_PREPEXEC

**What it measures:** Combined prepare+execute time when using `sp_prepexec`.

**When used:**
- `prepareMethod=prepexec` (default) AND
- Statement needs preparation (`needsPrepare=true`) AND
- Not first execution with `enablePrepareOnFirstPreparedStatementCall=false`

**Where tracked:**
- `SQLServerPreparedStatement.doExecutePreparedStatement()` - When `usedPrepExec=true`
- `SQLServerPreparedStatement.doExecutePreparedStatementBatch()` - When `usedPrepExec=true`

**Implementation:** A `usedPrepExec` flag is set when `buildPrepExecParams()` is called in `doPrepExec()`.

---

### 5. STATEMENT_EXECUTE

**What it measures:** Pure execution time when prepare is either separate or not needed.

**When used:**
- `sp_execute` - Execute with existing prepared handle
- `sp_executesql` - Direct execution without preparation
- `sp_cursoropen` - Cursor-based execution
- `PKT_QUERY` - Direct SQL text execution

**Where tracked:**
- All statement execution methods when `usedPrepExec=false`
- `SQLServerStatement` methods (always use STATEMENT_EXECUTE)

---

## Coverage by Class

### SQLServerStatement.java

| Method | Scenario | CREATION_TO_FIRST_PACKET | FIRST_PACKET_TO_FIRST_RESPONSE | EXECUTE/PREPEXEC |
|--------|----------|:------------------------:|:-----------------------------:|:----------------:|
| `doExecuteStatement()` | Regular statement | ✅ | ✅ | EXECUTE |
| `doExecuteCursored()` | Cursor (sp_cursoropen) | ✅ | ✅ | EXECUTE |
| `doExecuteStatementBatch()` | Batch statements | ✅ | ✅ | EXECUTE |

### SQLServerPreparedStatement.java

| Method | Scenario | CREATION_TO_FIRST_PACKET | FIRST_PACKET_TO_FIRST_RESPONSE | EXECUTE/PREPEXEC |
|--------|----------|:------------------------:|:-----------------------------:|:----------------:|
| `doExecutePreparedStatement()` | Prepared statement | ✅ | ✅ | Dynamic¹ |
| `doExecutePreparedStatementBatch()` main | Batch prepared | ✅ | ✅ | Dynamic¹ |
| `doExecutePreparedStatementBatch()` sp_execute | After sp_prepare | N/A² | ✅ | EXECUTE |
| `doExecuteExecMethodBatchCombined()` | Temp table batch | ✅ | ✅ | EXECUTE |
| `doPrep()` | sp_prepare only | N/A | N/A | PREPARE |

**Notes:**
- ¹ Dynamic: Uses STATEMENT_PREPEXEC if `usedPrepExec=true`, otherwise STATEMENT_EXECUTE
- ² N/A: Mid-batch execution, not a new statement creation

---

## SQL Server Stored Procedures

Understanding which stored procedure is used helps interpret the metrics:

| Stored Procedure | JDBC Method | What It Does | Activity |
|------------------|-------------|--------------|----------|
| `sp_prepare` | `buildPrepParams()` | Prepare only, returns handle | STATEMENT_PREPARE |
| `sp_prepexec` | `buildPrepExecParams()` | Prepare + Execute combined | STATEMENT_PREPEXEC |
| `sp_execute` | `buildExecParams()` | Execute with existing handle | STATEMENT_EXECUTE |
| `sp_executesql` | `buildExecSQLParams()` | Direct execution, no handle | STATEMENT_EXECUTE |
| `sp_cursoropen` | `doExecuteCursored()` | Open server cursor | STATEMENT_EXECUTE |
| (PKT_QUERY) | Direct SQL | Raw SQL text | STATEMENT_EXECUTE |

---

## Implementation Details

### The `usedPrepExec` Flag

The `usedPrepExec` flag in `SQLServerPreparedStatement` determines which activity to report:

```java
// In doPrepExec():
if (isPrepareMethodSpPrepExec) {
    buildPrepExecParams(tdsWriter);
    usedPrepExec = true;   // → STATEMENT_PREPEXEC
} else {
    // sp_prepare path or sp_execute path
    usedPrepExec = false;  // → STATEMENT_EXECUTE (or STATEMENT_PREPARE in doPrep)
}

// In doExecutePreparedStatement():
PerformanceActivity activity = usedPrepExec 
    ? PerformanceActivity.STATEMENT_PREPEXEC 
    : PerformanceActivity.STATEMENT_EXECUTE;
```

### Decision Flow

```
doPrepExec() called
    │
    ├─► needsPrepare=false? ──► buildExecParams() ──► STATEMENT_EXECUTE
    │
    ├─► First execution + !enablePrepareOnFirstCall?
    │   └─► buildExecSQLParams() ──► STATEMENT_EXECUTE
    │
    ├─► prepareMethod=prepexec?
    │   └─► buildPrepExecParams() ──► STATEMENT_PREPEXEC
    │
    └─► prepareMethod=prepare?
        └─► doPrep() ──► STATEMENT_PREPARE
            then buildExecParams() ──► STATEMENT_EXECUTE
```

---

## Paths NOT Tracked

| Location | Reason |
|----------|--------|
| `closeInternal()` sp_unprepare | Cleanup operation, not meaningful for execution metrics |
| `wasInterrupted()` path | Cancel processing, not actual execution |

---

## Legend

| Symbol | Meaning |
|--------|---------|
| ✅ | Metric is tracked |
| N/A | Not applicable for this path |
| Dynamic¹ | Activity selected based on `usedPrepExec` flag |

---

## Configuration Impact

| Connection Property | Effect on Metrics |
|---------------------|-------------------|
| `prepareMethod=prepexec` (default) | Uses STATEMENT_PREPEXEC for prepared statements |
| `prepareMethod=prepare` | Uses STATEMENT_PREPARE + STATEMENT_EXECUTE separately |
| `enablePrepareOnFirstPreparedStatementCall=false` | First call uses sp_executesql (STATEMENT_EXECUTE) |
| `enablePrepareOnFirstPreparedStatementCall=true` | First call uses sp_prepexec (STATEMENT_PREPEXEC) |

---

## Appendix: Complete Audit of All `startResponse()` Calls

This section documents every `startResponse()` call in both classes and confirms tracking status.

### SQLServerStatement.java (3 execution paths)

| Line | Method | Path | CREATION_TO_FIRST_PACKET | FIRST_PACKET_TO_FIRST_RESPONSE | STATEMENT_EXECUTE |
|------|--------|------|:------------------------:|:-----------------------------:|:-----------------:|
| 1060 | `doExecuteStatement()` | Non-cursor execution | ✅ | ✅ | ✅ |
| 1159 | `doExecuteStatementBatch()` | Batch execution | ✅ | ✅ | ✅ |
| 2319 | `doExecuteCursored()` | Cursor-based execution | ✅ | ✅ | ✅ |

### SQLServerPreparedStatement.java (7 `startResponse()` calls)

| Line | Method | Path | CREATION_TO_FIRST_PACKET | FIRST_PACKET_TO_FIRST_RESPONSE | Activity |
|------|--------|------|:------------------------:|:-----------------------------:|:--------:|
| 351 | `closeInternal()` | sp_unprepare cleanup | ❌ N/A | ❌ N/A | ❌ N/A |
| 715 | `doExecutePreparedStatement()` | Main execution | ✅ | ✅ | Dynamic |
| 1354 | `doPrep()` | sp_prepare only | N/A | N/A | PREPARE |
| 3220 | `doExecutePreparedStatementBatch()` | Interrupted/cancel path | ❌ N/A | ❌ N/A | ❌ N/A |
| 3266 | `doExecutePreparedStatementBatch()` | Main batch path | ✅ | ✅ | Dynamic |
| 3314 | `doExecutePreparedStatementBatch()` | sp_execute after sp_prepare | N/A¹ | ✅ | EXECUTE |
| 3461 | `doExecuteExecMethodBatchCombined()` | Temp table batch | ✅ | ✅ | EXECUTE |

**Notes:**
- ¹ N/A: Mid-batch execution, not a new statement creation
- Dynamic: Uses STATEMENT_PREPEXEC when `usedPrepExec=true`, otherwise STATEMENT_EXECUTE
- ❌ N/A: Intentionally not tracked (cleanup or cancel operations)

---

## Summary Statistics

| Category | Count | Status |
|----------|:-----:|:------:|
| Total `startResponse()` calls | 10 | ✅ All accounted for |
| Tracked execution paths | 9 | ✅ Properly tracked |
| Intentionally NOT tracked | 2 | ✅ Correct (cleanup & cancel) |

### Activities Distribution

| Activity | Usage Count | Locations |
|----------|:-----------:|-----------|
| `STATEMENT_CREATION_TO_FIRST_PACKET` | 6 | All main execution paths |
| `STATEMENT_FIRST_PACKET_TO_FIRST_RESPONSE` | 7 | All main execution paths + sp_execute after sp_prepare |
| `STATEMENT_PREPARE` | 1 | `doPrep()` only |
| `STATEMENT_PREPEXEC` | 2 | `doExecutePreparedStatement()` and `doExecutePreparedStatementBatch()` when `usedPrepExec=true` |
| `STATEMENT_EXECUTE` | 7 | SQLServerStatement methods + PreparedStatement when `usedPrepExec=false` |
