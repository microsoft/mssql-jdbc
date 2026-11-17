# `prepareMethod=none`: wrong `executeBatch()` update counts — analysis, test gaps, test plan, and fix plan

## 1. Summary

With `prepareMethod=none` (and `scopeTempTablesToConnection` when it takes the direct path), `PreparedStatement.executeBatch()` / `executeLargeBatch()` report **`1` for every batch entry** that succeeds, regardless of the real row count. The server sends the correct count; the driver discards it.

The customer report covers multi-row DML (`DELETE TOP(5)` → `[1]` instead of `[5]`). Probing the current `main` source shows that the same defect has broader consequences:

| Scenario (one batch) | `prepexec` (correct) | `none` (actual) | Consequence |
|---|---|---|---|
| Multi-row `DELETE TOP(5)` | `[5]` | `[1]` | Under-reported count (customer report) |
| Multi-row `UPDATE … WHERE id > ?` | `[5]` | `[1]` | Under-reported count |
| **Zero-row `UPDATE … WHERE id = ?` (no match)** | **`[0]`** | **`[1]`** | **Hides optimistic-lock / stale-row failures** |
| `SET NOCOUNT ON; UPDATE …` | `[-2]` (`SUCCESS_NO_INFO`) | `[1]` | Fabricated count |
| Compound entry `INSERT …; UPDATE …` ×2 | `[1, 2]` | `[1, 1]` | Counts misaligned with entries |
| PK violation in entry 2 of 3 | `BatchUpdateException`, `[1, -3, 1]` | `SQLServerException` (cause `SQLServerException`), no counts | `catch (BatchUpdateException)` is bypassed; per-entry status is lost |

Probe: `NoneBatchProbe.java` (temporary, not committed), run against `target/classes` built from the current base, on the configured test server.

### Severity

The zero-row case is the most serious. ORMs and data-access layers check batch counts to detect concurrent modification. For example, Hibernate expects `1` for a versioned `UPDATE … WHERE id = ? AND version = ?`, and Spring `batchUpdate` callers inspect the returned array. Under `none`, a stale update that changed **no** rows reports `1`, so the application can treat a lost update as successful. This goes beyond the reported issue that multi-row counts are wrong.

## 2. Root cause

Routing: [`doExecutePreparedStatementBatch`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L3765-L3774) sends every direct-SQL batch (`isDirectSqlExecution`) to `doExecuteExecMethodBatchCombined`. That method concatenates all entries, with parameters inlined as literals, into one `PKT_QUERY` language batch.

The faulty result loop is in [`doExecuteExecMethodBatchCombined`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java#L4105-L4122):

```java
boolean isResultSet = getNextResult(true);   // true = "a result exists" (ResultSet OR update count)
if (isResultSet) {
    batchCommand.updateCounts[batchIdx] = 1;  // real count discarded
    ...
} else {
    long updateCount = getUpdateCount();      // only reached when there are NO more results
    batchCommand.updateCounts[batchIdx] = updateCount;
}
```

[`SQLServerStatement.getNextResult`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2085-L2088) returns `true` for an update count after storing it in `updateCount`. The loop interprets `true` as "ResultSet" and writes `1`. The `else` branch runs only when no results remain, and then writes `-1`. That value is not valid in a JDBC batch array, which should contain `EXECUTE_FAILED` or `SUCCESS_NO_INFO`.

Related defects in the same method (found during this analysis):

| # | Defect | Effect |
|---|---|---|
| R1 | Boolean misread (above) | Every successful entry reports `1` |
| R2 | `-1` (no count) is not mapped to `SUCCESS_NO_INFO` | `NOCOUNT` entries report a fabricated value (`1` today, `-1` if only R1 is fixed) |
| R3 | When results are exhausted, the loop keeps going and writes `getUpdateCount()` (`-1`) | Unexecuted entries after a batch-aborting error report `-1`, not `EXECUTE_FAILED` |
| R4 | Batch errors are thrown as `SQLServerException` wrapping a `BatchUpdateException`, then re-wrapped by the outer `catch (SQLException)` | Callers do not get `BatchUpdateException` or `getUpdateCounts()`; behavior differs from `prepexec` |
| R5 | No `isSessionUnAvailable()` / `rolledBackTransaction()` check | Unlike the RPC and `Statement` batch paths, the loop continues after a fatal error |
| R6 | Uses `getUpdateCount()` (int, throws `R_updateCountOutofRange`) | `executeLargeBatch()` cannot report counts > `Integer.MAX_VALUE` |
| R7 | Exactly one `getNextResult` per entry and no draining of leftover results | Compound SQL (N statements per entry) produces N counts per entry, so counts are misaligned and trailing results are left on the statement |

The correct pattern already exists in [`SQLServerStatement.executeBatch`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2286-L2305). It also reads a language batch: `if (!getNextResult(true)) break;`, rejects `resultSet`, and maps `-1 → SUCCESS_NO_INFO`.

## 3. Why the existing tests missed it

The batch tests on the direct path all use fixtures where the hard-coded `1` happens to be correct, and their assertions are too weak to detect a wrong value.

| Gap | Evidence | Why it hid the bug |
|---|---|---|
| **G1. Every direct-path batch fixture is a one-row `INSERT`** | `PrepStmtDirectSQLExecutionTests` (`testBatchExecutionWithPrepareMethodNone`, `testLargeBatchWithNone`, `testExplicitNullInDirectBatchIsSet`, …), `BatchExecutionWithBulkCopyTest.testLineCommentsPreserveBatchEntries`, `PrepareMethodLiteralTest.testLiteralBindingsThroughExecution` | The true count is always `1`, so the hard-coded `1` passes. No direct-path batch uses multi-row, zero-row, `NOCOUNT`, or trigger DML. |
| **G2. Assertions check shape, not value** | Most direct-path tests assert only `counts.length`, `count >= -2`, or `count >= 0` | A wrong count still passes. |
| **G3. Error test asserts only `SQLException`** | `testBatchCombinedWithErrorsWithNone` (named "verifies update counts reflect failures") uses `assertThrows(SQLException.class, …)` | Hides R4: no `BatchUpdateException` and no per-entry counts. The equivalent `scopeTempTablesToConnection` test uses a permanent table, so it falls back to `prepexec` and never reaches the combined path. |
| **G4. Differential `none` vs `prepexec` checks cover only procedure batches** | `PrepareMethodNoneProcedureRpcTest.testProcedureCallBatchMatchesTypedBinding` / `…FailingEntry…` | Procedure SQL is routed to unprepared RPC, so the combined path was never compared with `prepexec`. |
| **G5. Count-semantics suite is not parameterized by `prepareMethod`** | `MultiStatementResultTraversalTest` batch section (`…WithNoCountItem`, `…OnTriggerTable`, `preparedStatementExecuteBatchWithCompoundPreparedSql` asserting `[1, 2]`) runs only with the default `prepexec` | The best count-contract tests already exist, but they never ran under `none`. Running `preparedStatementExecuteBatchWithCompoundPreparedSql` under `none` would fail (`[1, 1]`). |
| **G6. No `executeUpdate` oracle for batches** | `executeUpdate` under `none` is well covered and correct | No test checked that a single-entry batch equals `executeUpdate` for the same statement. That one comparison catches R1–R3. |
| **G7. Recent `none` hardening focused elsewhere** | Literal inlining (#3083/#3086), RPC routing/transition, streams, metadata, recovery (#3091) | These areas cover how values are rendered and how execution is routed, not how results are mapped back. Their batch tests reuse one-row `INSERT` fixtures (G1). |
| **G8. Review/test design** | New execution mode added without a mode-parameterized JDBC batch contract | `executeBatch` return values are a JDBC contract. Adding a new execution path should have required running the existing contract tests across all modes. |

**Root cause of the miss:** a fixture bias toward one-row inserts (G1), combined with shape-only assertions (G2/G3), and no differential parity check on the direct path (G4–G6).

## 4. Test plan

### 4.1 Principles

- **Differential oracle:** for every case, compare `none` (and `scopeTempTablesToConnection` with temp-table SQL) with `prepexec` on identically seeded data. Do not hard-code `1`.
- **Exact values:** assert the full `int[]` / `long[]` and the persisted rows. Do not assert only the length.
- **Both APIs:** cover `executeBatch()` and `executeLargeBatch()` for every case.
- **No new test classes:** add to existing suites, following the #3091 review feedback.

### 4.2 New parity cases: `PrepStmtDirectSQLExecutionTests` (direct-path suite)

A single parameterized test `testBatchUpdateCountsMatchPrepexec(mode, shape, largeBatch)` with mode ∈ {`none`, `scopeTempTablesToConnection`}, control `prepexec`. All cases use `#temp` tables, so scope mode also takes the direct path.

| ID | Shape (per entry) | Entries | Expected (= `prepexec`) | Catches |
|---|---|---|---|---|
| C1 | `DELETE TOP(5) … WHERE id <= ?` | 1 | `[5]` | R1 (customer case) |
| C2 | `UPDATE … WHERE id > ?` (multi-row) | 1 | `[k]` | R1 |
| C3 | `INSERT … SELECT … WHERE id <= ?` | 1 | `[k]` | R1 |
| C4 | `MERGE` affecting k rows | 1 | `[k]` | R1 |
| C5 | **Zero-row `UPDATE … WHERE id = ?`** | 1 | `[0]` | R1 (optimistic locking) |
| C6 | Mixed counts in one batch | 4 | e.g. `[0, 5, 1, 0]` | R1 + per-entry alignment |
| C7 | Versioned update, stale version | 2 | `[1, 0]` | Lost-update scenario |
| C8 | `SET NOCOUNT ON; UPDATE …; SET NOCOUNT OFF` | 2 | `[-2, -2]` | R2 |
| C9 | DML on table with `AFTER` trigger doing its own DML | 2 | outer counts only (`[1, 2]`) | Trigger `DONEINPROC` filtering |
| C10 | Compound `INSERT …; UPDATE …` | 2 | `[1, 2]` | R7 |
| C11 | Single-row `INSERT` (regression guard) | 5 | `[1,1,1,1,1]` | No regression |
| C12 | Second batch reuse on same statement after C10 | 1 | correct; no stale results | R7 draining |

### 4.3 Error and status cases (same suite)

| ID | Scenario | Expected (parity with `prepexec`) | Catches |
|---|---|---|---|
| E1 | PK violation in entry 2 of 3 | `BatchUpdateException`, `[1, -3, 1]`, rows 1 and 3 persisted | R4 |
| E2 | Same as E1 via `executeLargeBatch` | `BatchUpdateException.getLargeUpdateCounts()` `[1, -3, 1]` | R4, R6 |
| E3 | Batch-aborting error (`SET XACT_ABORT ON`, or a conversion error) mid-batch | `BatchUpdateException`; unexecuted entries `EXECUTE_FAILED`, never `-1`. Document any divergence from `prepexec`, because one language batch aborts all later entries. | R3 |
| E4 | Error in manual transaction that the server rolls back | `SQLServerException` propagated immediately; connection state correct | R5 |
| E5 | Severity ≥ 20 / killed session mid-batch | Propagated immediately, no further parsing | R5 |
| E6 | Existing `testBatchCombinedWithErrorsWithNone` | Strengthen to assert `BatchUpdateException` and exact counts | G3 |

### 4.4 Parameterize existing count-contract tests

- `MultiStatementResultTraversalTest`: run the PreparedStatement batch tests (`preparedStatementExecuteBatchReturnsOneCountPerParameterSet`, `preparedStatementExecuteBatchWithCompoundPreparedSql`, and trigger and `NOCOUNT` variants) over `prepexec` / `none`. This closes G5.
- Strengthen length-only assertions in the existing `…WithNone` tests in `PrepStmtDirectSQLExecutionTests` to exact arrays (G2).

### 4.5 Fast unit-level guard (no server)

Add a Mockito/stub test in which `getNextResult` returns `true` with `updateCount = 5`, `0`, or `-1`, and `false` (exhausted). Assert that the mapped array is `[5]`, `[0]`, `[SUCCESS_NO_INFO]`, and `EXECUTE_FAILED`. This locks the classification logic independently of the server.

### 4.6 Validation matrix

Run the six existing `none` suites plus the updated `MultiStatementResultTraversalTest` on:

- `jre17` (JDK 25)
- `jre8` (JDK 11)
- SQL Server 2019+
- Azure SQL, if available

## 5. Fix plan

### Phase 1: correct result mapping (hotfix candidate for 13.6.x)

Rewrite the loop in `doExecuteExecMethodBatchCombined` to follow `SQLServerStatement.executeBatch` and the RPC batch path:

```java
for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
    try {
        if (!getNextResult(true))
            break;                                   // R3: remaining entries stay EXECUTE_FAILED
        if (null != resultSet) {
            resultSet.close();
            resultSet = null;
            SQLServerException.makeFromDriverError(connection, this,
                    SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null, false);
        }
        batchCommand.updateCounts[batchIdx] = (-1 == updateCount) ? Statement.SUCCESS_NO_INFO
                                                                   : updateCount;   // R1, R2, R6
    } catch (SQLServerException e) {
        if (connection.isSessionUnAvailable() || connection.rolledBackTransaction())
            throw e;                                 // R5
        batchCommand.updateCounts[batchIdx] = Statement.EXECUTE_FAILED;
        if (null == batchCommand.batchException)
            batchCommand.batchException = e;
    }
}
// R4: return normally; executeBatch/executeLargeBatch already convert batchException into BatchUpdateException
```

Details:

- Read the `updateCount` field (`long`) directly, not `getUpdateCount()` (R6).
- Remove the local `BatchUpdateException` construction and the outer `catch (SQLException)` re-wrap. Leave `batchCommand.batchException` set so that the existing code in `executeBatch` / `executeLargeBatch` throws a `BatchUpdateException` with counts, as `prepexec` does (R4).
- Make sure returning normally does not interact with the `retryRejectedDirectSqlAsRpc()` fallback. That fallback must still apply only to pre-send literal rejection.
- **Compatibility note:** under `none`, batch failures will throw `BatchUpdateException` instead of `SQLServerException`. Both are `SQLException`, but code that catches `SQLServerException` specifically will no longer catch them. This change follows the JDBC specification and matches `prepexec`. Mention it in the CHANGELOG.

### Phase 2: compound SQL per entry (R7) — design decision required

Under a language batch, each top-level DML statement emits its own `DONE`. This is unlike RPC, where inner `DONEINPROC` tokens are folded into one count per entry. Options:

| Option | Description | Trade-off |
|---|---|---|
| A | Route `PreparedStatement` SQL that contains multiple statements to unprepared RPC (`sp_executesql`) under `none`, as is already done for procedure calls | Exact `prepexec` parity. Temp tables created inside the SQL would no longer outlive the call, which conflicts with the `scopeTempTablesToConnection` purpose; apply only when no temp-table DDL is present. |
| B | Keep the direct path and drain leftover results after the loop; document the mapping as implementation-defined (as for `Statement.executeBatch` compound items) | Minimal; counts for compound entries remain misaligned |
| C | Count top-level statements per entry with the existing SQL parser and consume N results per entry (report last) | Exact counts, but depends on parser correctness (comments, `GO`-less control flow, `IF … BEGIN … END`) |

**Recommendation:** ship Phase 1 with B's drain step in the hotfix. Choose between A and C for the next minor release after team discussion.

### Phase 3: hardening and process

- Add the `prepareMethod` dimension to the JDBC batch count-contract tests (§4.4), so any future execution path inherits them.
- Add a review checklist item: "new execution path ⇒ run batch/update-count contract suite across all `prepareMethod` values".
- Add a CHANGELOG entry (Fixed: incorrect `executeBatch`/`executeLargeBatch` update counts and missing `BatchUpdateException` under `prepareMethod=none`).
- Customer guidance until the fix ships:
  - Use `prepareMethod=prepexec` (default) for batches whose counts matter.
  - Otherwise, use `executeUpdate` per entry.

## 6. Work items

| # | Item | Owner | Depends on |
|---|---|---|---|
| 1 | Failing tests C1–C12, E1–E6, unit guard (§4) | — | — |
| 2 | Phase 1 fix + drain | — | 1 |
| 3 | Parameterize `MultiStatementResultTraversalTest` batch section | — | 2 |
| 4 | CHANGELOG + 13.6.x backport | — | 2 |
| 5 | Phase 2 design decision (A vs C) | Team | 2 |
| 6 | Validate with customer harness | — | 2 |

## 7. Local fix (Phase 1)

Implemented in the local worktree `C:\work\jdbc\repos\mssql-jdbc-nonebatchcounts`. It is not committed or pushed. It addresses R1–R6. R7 (compound SQL per entry) remains Phase 2.

### 7.1 Driver diff: `SQLServerPreparedStatement.doExecuteExecMethodBatchCombined`

Most of the line churn comes from removing the outer `try { … } catch (SQLException e)` wrapper, which re-indents the method. The behavioral change is the result loop at the end.

```diff
@@ -4050,105 +4050,82 @@
         final int numBatches = batchParamValues.size();
         batchCommand.updateCounts = new long[numBatches];
 
-        try {
-            // Build the combined SQL script by repeating the entire userSQL for each batch
-            StringBuilder sqlScript = new StringBuilder();
+        // Build the combined SQL script by repeating the entire userSQL for each batch
+        StringBuilder sqlScript = new StringBuilder();
 
-            for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
-                Parameter[] batchParams = batchParamValues.get(batchIdx);
+        for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
+            Parameter[] batchParams = batchParamValues.get(batchIdx);
 
-                // Replace all parameter markers in the entire userSQL with values from this
-                // batch
-                String expandedSQL = replaceParameterMarkersWithValues(
-                        userSQL, userSQLParamPositions, batchParams, false);
+            // Replace all parameter markers in the entire userSQL with values from this
+            // batch
+            String expandedSQL = replaceParameterMarkersWithValues(userSQL, userSQLParamPositions, batchParams, false);
 
-                sqlScript.append(expandedSQL);
+            sqlScript.append(expandedSQL);
 
-                // End any trailing line comment before the separator and the next batch entry.
-                if (batchIdx < numBatches - 1) {
-                    sqlScript.append("\n;");
-                }
+            // End any trailing line comment before the separator and the next batch entry.
+            if (batchIdx < numBatches - 1) {
+                sqlScript.append("\n;");
             }
+        }
 
-            // Execute the combined SQL script
-            TDSWriter tdsWriter = batchCommand.startRequest(TDS.PKT_QUERY);
-            tdsWriter.writeString(sqlScript.toString());
-
-            // End request build time tracking
-            endCreationToFirstPacketTracking();
-
-            // Track batch execution time
-            try (PerformanceLog.Scope executeScope = PerformanceLog.createScope(
-                    PerformanceLog.perfLoggerStatement,
-                    connection,
-                    getStatementID(),
-                    this,
-                    userSQL,
-                    PerformanceActivity.STATEMENT_EXECUTE)) {
-                try {
-                    // Track server roundtrip time
-                    startFirstPacketToFirstResponseTracking();
-                    try {
-                        ensureExecuteResultsReader(batchCommand.startResponse(getIsResponseBufferingAdaptive()));
-                    } finally {
-                        endFirstPacketToFirstResponseTracking();
-                    }
+        // Execute the combined SQL script
+        TDSWriter tdsWriter = batchCommand.startRequest(TDS.PKT_QUERY);
+        tdsWriter.writeString(sqlScript.toString());
 
-                    startResults();
+        // End request build time tracking
+        endCreationToFirstPacketTracking();
 
-                    // Initialize update counts to failed by default
-                    for (int i = 0; i < numBatches; i++) {
-                        batchCommand.updateCounts[i] = Statement.EXECUTE_FAILED;
-                    }
+        // Track batch execution time
+        try (PerformanceLog.Scope executeScope = PerformanceLog.createScope(PerformanceLog.perfLoggerStatement,
+                connection, getStatementID(), this, userSQL, PerformanceActivity.STATEMENT_EXECUTE)) {
+            try {
+                // Track server roundtrip time
+                startFirstPacketToFirstResponseTracking();
+                try {
+                    ensureExecuteResultsReader(batchCommand.startResponse(getIsResponseBufferingAdaptive()));
+                } finally {
+                    endFirstPacketToFirstResponseTracking();
+                }
 
-                    // Process results for each batch entry
-                    for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
-                        try {
-                            boolean isResultSet = getNextResult(true);
+                startResults();
 
-                            if (isResultSet) {
-                                batchCommand.updateCounts[batchIdx] = 1;
-                                if (null != resultSet) {
-                                    resultSet.close();
-                                    resultSet = null;
-                                }
-                            } else {
-                                long updateCount = getUpdateCount();
-                                batchCommand.updateCounts[batchIdx] = updateCount;
-                            }
-                        } catch (SQLException e) {
-                            batchCommand.updateCounts[batchIdx] = Statement.EXECUTE_FAILED;
+                // Initialize update counts to failed by default
+                for (int i = 0; i < numBatches; i++) {
+                    batchCommand.updateCounts[i] = Statement.EXECUTE_FAILED;
+                }
 
-                            if (null == batchCommand.batchException) {
-                                batchCommand.batchException = new SQLServerException(
-                                        e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
-                            }
-                        }
-                    }
-                    // If we had any batch failures, throw BatchUpdateException with update counts
-                    if (null != batchCommand.batchException) {
-                        int[] updateCounts = new int[batchCommand.updateCounts.length];
-                        for (int i = 0; i < batchCommand.updateCounts.length; i++) {
-                            updateCounts[i] = (int) batchCommand.updateCounts[i];
+                // Process results for each batch entry. Entries left without a result, for example after a
+                // batch-aborting error, keep EXECUTE_FAILED.
+                for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
+                    try {
+                        // getNextResult returns true for an update count as well as for a ResultSet
+                        if (!getNextResult(true))
+                            break;
+
+                        if (null != resultSet) {
+                            resultSet.close();
+                            resultSet = null;
+                            SQLServerException.makeFromDriverError(connection, this,
+                                    SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null, false);
                         }
 
-                        BatchUpdateException batchException = new BatchUpdateException(
-                                batchCommand.batchException.getMessage(),
-                                batchCommand.batchException.getSQLState(),
-                                batchCommand.batchException.getErrorCode(), updateCounts);
+                        batchCommand.updateCounts[batchIdx] = (-1 == updateCount) ? Statement.SUCCESS_NO_INFO
+                                                                                  : updateCount;
+                    } catch (SQLServerException e) {
+                        // A failure that closed the session or rolled back the transaction ends the batch.
+                        if (connection.isSessionUnAvailable() || connection.rolledBackTransaction())
+                            throw e;
 
-                        throw new SQLServerException(batchException.getMessage(),
-                                batchException.getSQLState(),
-                                batchException.getErrorCode(), batchException);
+                        batchCommand.updateCounts[batchIdx] = Statement.EXECUTE_FAILED;
+                        if (null == batchCommand.batchException)
+                            batchCommand.batchException = e;
                     }
-                } catch (SQLServerException e) {
-                    executeScope.setException(e);
-                    throw e;
                 }
+                // executeBatch and executeLargeBatch turn batchCommand.batchException into a BatchUpdateException.
+            } catch (SQLServerException e) {
+                executeScope.setException(e);
+                throw e;
             }
-
-        } catch (SQLException e) {
-            throw new SQLServerException(e.getMessage(), e.getSQLState(), e.getErrorCode(), e);
         }
     }
```

How the change maps to the defects:

| Defect | Change |
|---|---|
| R1 | `true` from `getNextResult` no longer means "ResultSet"; the count comes from the `updateCount` field |
| R2 | `-1` maps to `SUCCESS_NO_INFO` |
| R3 | `break` when results are exhausted, so remaining entries keep `EXECUTE_FAILED` |
| R4 | The local `BatchUpdateException` wrapping is removed; `executeBatch`/`executeLargeBatch` raise a real `BatchUpdateException` with counts, as for `prepexec` |
| R5 | Session-unavailable / rolled-back-transaction errors propagate immediately |
| R6 | Reads the `long` `updateCount` field, not `getUpdateCount()` |
| — | A `ResultSet` in a batch is an entry error (`R_resultsetGeneratedForUpdate`), as on the RPC path |

### 7.2 Test diff: `PrepStmtDirectSQLExecutionTests`

- `testBatchCombinedWithErrorsWithNone` now asserts `BatchUpdateException`, error code `2627`, and counts `[1, -3, 1]`. Previously it asserted only a generic `SQLException`.
- `testDirectBatchUpdateCountsMatchPrepexec` (16 cases) covers 8 shapes × `executeBatch`/`executeLargeBatch`. Each case asserts exact arrays for both `prepexec` and `none`:
  - multi-row `DELETE`, `UPDATE`, and `INSERT … SELECT`
  - zero-row update
  - mixed `[0, 5, 1, 0]`
  - optimistic-lock stale version `[1, 0]`
  - `NOCOUNT` `[-2, -2]`
  - one-row inserts
- `testDirectBatchErrorReportsBatchUpdateExceptionLikePrepexec` (2 cases): a PK violation produces `BatchUpdateException` with `[1, -3, 1]` and two persisted rows in both modes.
- `testDirectBatchAbortingErrorReportsExecuteFailed` (2 cases): after a conversion error, unexecuted entries report `EXECUTE_FAILED` and never `-1`, consistent with the persisted rows.

### 7.3 Validation

| Profile | Suite | Result |
|---|---|---|
| `jre17` / JDK 25 | `PrepStmtDirectSQLExecutionTests` | 86 run, 0 failures, 0 errors, 0 skipped |
| `jre8` / JDK 11 (clean) | `PrepStmtDirectSQLExecutionTests`, `PrepareMethodNoneProcedureRpcTest`, `PrepareMethodLiteralTest`, `ParameterDirectSqlRetryTest`, `PrepareMethodNoneCallableStatementTest`, `SQLServerPreparedStatementTempTableTest`, `BatchExecutionWithBulkCopyTest` | 1,557 run, 0 failures, 0 errors, 0 skipped |

Remaining before a PR:

- Run the full `none` suite set on `jre17`.
- Run `MultiStatementResultTraversalTest`. Its `@Nested` classes were not selected by the `-Dtest` filter.
- Check whether the `SEVERE ... unexpected TDS_RETURN_VALUE` lines logged during the `jre8` run also appear without this fix. All tests in that run passed.
- Add a CHANGELOG entry noting that `none` batch failures now throw `BatchUpdateException`.
