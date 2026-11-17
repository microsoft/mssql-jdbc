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

Two alternative proposals are under consideration: **(1) fix direct-path update counts while documenting the limits of concatenated result attribution**, or **(2) route batches through batched unprepared `sp_executesql` RPCs**, reusing the execution boundaries and response handling already used for procedure calls and unsupported parameter types. Both preserve batching; neither proposal executes each entry in a separate client/server round trip. See §5 for the trade-offs. The local patch and validation recorded in §7 apply only to the initial proposal 1 prototype, not to a complete implementation of either proposal.

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
| R7 | Exactly one `getNextResult` per entry, without reliable parameter-set boundaries | Compound and conditional SQL can produce a variable number of results per entry, so counts and errors can be assigned to the wrong entry |
| R8 | Result processing stops after filling the count array, without error-aware response completion | Trailing errors, such as `INSERT …; THROW …`, can be missed before the API reports success |

The basic count-mapping pattern already exists in [`SQLServerStatement.executeBatch`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerStatement.java#L2286-L2305). It also reads a language batch: `if (!getNextResult(true)) break;`, rejects `resultSet`, and maps `-1 → SUCCESS_NO_INFO`. Reusing that pattern alone does not establish parameter-set boundaries or ensure trailing errors are reported.

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

- **Differential oracle:** compare successful execution with `prepexec` on identically seeded data. For proposal 1, record unsupported result-attribution cases explicitly rather than claiming general parity. For either proposal, document legitimate error-path continuation/abort differences instead of requiring identical outcomes in every scenario.
- **Verify routing:** a reference to an existing `#temp` table does not select direct execution under `scopeTempTablesToConnection`. Its current detector recognizes local-temp-table creation (`CREATE TABLE #…` or `SELECT … INTO #…`). Assert the actual execution path, and verify RPC routing for proposal 2.
- **Exact values:** assert the full `int[]` / `long[]` and the persisted rows. Do not assert only the length.
- **Both APIs:** cover `executeBatch()` and `executeLargeBatch()` for every case.
- **No new test classes:** add to existing suites, following the #3091 review feedback.

### 4.2 New parity cases: `PrepStmtDirectSQLExecutionTests` (direct-path suite)

A parameterized test `testBatchUpdateCountsMatchPrepexec(mode, shape, largeBatch)` with mode ∈ {`none`, `scopeTempTablesToConnection`}, control `prepexec`. For `none`, existing `#temp` tables can be seeded separately. For scope-mode direct-path coverage, use SQL that actually creates a local temporary table and verify routing; ordinary DML against an existing `#temp` table falls back to prepared RPC execution. Keep these routing-specific fixtures separate from simple-DML cases so incidental DDL counts do not hide incorrect attribution.

The expected values below are parity targets, not a claim that proposal 1 can satisfy every shape. In particular, C10 and variable-result cases require reliable entry boundaries, which the limited direct-path fix does not provide.

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
| C9 | DML on table with `AFTER` trigger doing its own DML, with trigger counts different from outer counts | 2 | outer counts only (`[1, 2]`) | Trigger `DONEINPROC` filtering |
| C10 | Compound `INSERT …; UPDATE …` | 2 | `[1, 2]` | R7 |
| C11 | Single-row `INSERT` (regression guard) | 5 | `[1,1,1,1,1]` | No regression |
| C12 | Second batch reuse on same statement after C10 | 1 | correct; no stale results | Response completion |
| C13 | `IF ? = 1 UPDATE …; UPDATE … WHERE id = ?` with both branch outcomes | 2 | One count per parameter set according to the RPC oracle; document proposal 1 limitation | R7 variable-result attribution |
| C14 | Create a local `#temp` table in an entry, then access it after the batch | 1 | Proposal 1 preserves connection scope; proposal 2 drops the RPC-created table at RPC completion | Scope compatibility |
| C15 | DML against a local `#temp` table created beforehand on the connection | 2 | Exact counts and table remains accessible under either proposal | Existing-table visibility |

### 4.3 Error and status cases (same suite)

| ID | Scenario | Expected / comparison with `prepexec` | Catches |
|---|---|---|---|
| E1 | PK violation in entry 2 of 3 | `BatchUpdateException`, `[1, -3, 1]`, rows 1 and 3 persisted | R4 |
| E2 | Same as E1 via `executeLargeBatch` | `BatchUpdateException.getLargeUpdateCounts()` `[1, -3, 1]` | R4, R6 |
| E3 | Batch-aborting error (`SET XACT_ABORT ON`, or a conversion error) mid-batch | `BatchUpdateException`; unexecuted entries `EXECUTE_FAILED`, never `-1`. Document any divergence from `prepexec`, because one language batch aborts all later entries. | R3 |
| E4 | Error in manual transaction that the server rolls back | `SQLServerException` propagated immediately; connection state correct | R5 |
| E5 | Severity ≥ 20 / killed session mid-batch | Propagated immediately, no further parsing | R5 |
| E6 | Existing `testBatchCombinedWithErrorsWithNone` | Strengthen to assert `BatchUpdateException` and exact counts | G3 |
| E7 | A count followed by `THROW`, including after the count array would be full | Report the trailing failure before returning; do not claim exact entry attribution without boundaries | R7, R8 |
| E8 | DML with `OUTPUT`, or compound DML followed by `SELECT` | Unexpected result set is an error; remaining results must not silently become the next entry's outcome | R7, R8 |
| E9 | Cancellation and query timeout during the batch | Respect existing cancellation/timeout behavior, process required attention acknowledgement, and verify connection reuse | Completion and interrupt handling |
| E10 | Recoverable and aborting errors in first, middle, and last entries | Compare counts, execution of later entries, and persisted/rolled-back state; document mode differences | Error continuation |

### 4.4 Parameterize existing count-contract tests

- `MultiStatementResultTraversalTest`: run the PreparedStatement batch tests (`preparedStatementExecuteBatchReturnsOneCountPerParameterSet`, `preparedStatementExecuteBatchWithCompoundPreparedSql`, and trigger and `NOCOUNT` variants) over `prepexec` / `none`. This closes G5.
- Strengthen length-only assertions in the existing `…WithNone` tests in `PrepStmtDirectSQLExecutionTests` to exact arrays (G2).

### 4.5 Fast unit-level guard (no server)

Add a Mockito/stub test in which `getNextResult` returns `true` with `updateCount = 5`, `0`, or `-1`, and `false` (exhausted). Assert that the mapped array is `[5]`, `[0]`, `[SUCCESS_NO_INFO]`, and `EXECUTE_FAILED`. Also exercise a count greater than `Integer.MAX_VALUE` through `executeLargeBatch()` and `BatchUpdateException.getLargeUpdateCounts()`. These guards cover count preservation, not server-side entry attribution. Add a response sequence with an error after the expected counts to ensure completion does not suppress it.

### 4.6 Validation matrix

Run the six existing `none` suites plus the updated `MultiStatementResultTraversalTest` on:

- `jre17` (JDK 25)
- `jre8` (JDK 11)
- SQL Server 2019+
- Azure SQL, if available

## 5. Two alternative proposals

**Preserve batching in both proposals.** Do not execute one client/server round trip per entry. The decision is whether to retain one concatenated language batch with explicitly limited attribution, or send one unprepared `sp_executesql` RPC per parameter set grouped into a batched RPC request.

### Proposal 1: fix direct-path update counts and highlight limitations

Keep literal expansion and the combined `PKT_QUERY` request. Correct basic result mapping in `doExecuteExecMethodBatchCombined` using the existing Statement/RPC batch patterns:

- Read the `updateCount` field (`long`) directly, not `getUpdateCount()` (R6).
- Replace the hard-coded `1` with the actual count (R1), and map a successful unknown count (`-1`) to `SUCCESS_NO_INFO` (R2).
- Leave entries without execution results as `EXECUTE_FAILED` when processing reaches exhaustion (R3). Validate terminal-token/error sequences; absence of a count alone does not establish that an entry was unexecuted.
- Reject actual result sets with `R_resultsetGeneratedForUpdate` rather than silently treating them as success.
- Remove the local `BatchUpdateException` construction and the outer `catch (SQLException)` re-wrap. Leave `batchCommand.batchException` set so that the existing code in `executeBatch` / `executeLargeBatch` throws a `BatchUpdateException` with counts, as `prepexec` does (R4).
- Immediately propagate session-unavailable and rolled-back-transaction errors (R5), and preserve cancellation/timeout handling.
- Preserve `retryRejectedDirectSqlAsRpc()` only for rejection detected before any SQL is sent. Never retry a partially executed batch to recover counts.
- **Process the remaining response before reporting completion (R8).** Filling the count array does not complete the batch. Observe trailing errors, transaction/session changes, and required attention acknowledgements. A cleanup-only drain is insufficient: `processResults()` intentionally suppresses some database errors. Completion must report errors even when the expected count slots are already filled.

**Limitations:** this is a limited correction, not general `prepexec` parity. Statement completion tokens do not label JDBC parameter sets. Compound SQL, conditional execution, loops, triggers, and unexpected result sets can make the number of results per entry vary. Neither draining leftover results nor statically counting SQL statements establishes reliable entry boundaries.

For example, concatenating two executions of `IF ? = 1 UPDATE t SET v = 10; UPDATE t SET v = 20 WHERE id = ?` can yield counts `1, 5, 1` when the first entry skips the conditional update and the second executes it. The counts do not encode the grouping `[1]`, `[5, 1]`. A later error is similarly not necessarily attributable to the next output-array slot. Document these limitations explicitly; do not claim that a better consumption loop alone resolves R7.

**Trade-off:** preserves current direct-SQL scope and minimizes execution-model changes, but cannot promise correct per-entry counts/error attribution for arbitrary compound or conditional SQL. A combined language batch can also abort later entries differently from batched RPC execution.

### Proposal 2: use batched unprepared `sp_executesql` whenever it is a batch

Route `executeBatch()` and `executeLargeBatch()` away from combined direct SQL and into the existing unprepared RPC batch path, including statements that would otherwise be direct under `none` or `scopeTempTablesToConnection`. Keep non-batch routing unchanged.

- Send **one `sp_executesql` RPC per parameter set, grouped into a batched RPC request**, using the existing RPC batch delimiters. This is not a separate round trip per entry, and it is not one `sp_executesql` wrapper around the whole concatenated script; the latter would retain the attribution problem.
- Reuse `useUnpreparedRpc`, typed parameter restoration/binding, and existing RPC result/count/error handling. Do not prepare or cache server handles. RPC completion supplies the per-entry boundaries needed to handle compound SQL consistently with the existing RPC path.
- Select this routing **before sending anything**, when batch execution is known. A fallback-style transition like unsupported-type handling is acceptable only if it occurs before submission; do not first execute direct SQL and then retry it as RPC.
- Preserve single-statement execution behavior when the same PreparedStatement is later used outside a batch. Define whether the batch-only routing state is temporary rather than blindly copying the unsupported-type fallback's lifetime-long transition.
- Verify full response completion, compound/count-selection behavior, unexpected result sets, errors, cancellation, and large counts through the existing RPC machinery; reuse is not a claim that every corner case has already been validated.

**Limitation: RPC scope, the same class of limitation already accepted for procedure calls under `none` and for parameters that cannot be inlined.** Existing connection-scoped temporary tables remain accessible inside `sp_executesql`. A local temporary table created inside an RPC can be used within that entry, but is dropped when the RPC ends: later entries or statements cannot use it. Other constructs relying on shared language-batch scope, such as local variables spanning entries, also change behavior.

For example:

```sql
-- One batch entry, followed by an application query on the same connection:
CREATE TABLE #items (id INT);
INSERT INTO #items VALUES (42);

-- Executed after executeBatch() returns:
SELECT id FROM #items;
```

With direct execution, the final query returns `42`. If the entry runs inside `sp_executesql`, the final query fails because `#items` was dropped at RPC completion. Creating `#items` beforehand on the connection avoids this scope issue.

This is not a new kind of fallback restriction, but routing every batch through RPC expands where it applies, including batches under `scopeTempTablesToConnection`. Document batch execution as an exception to connection-scoped temp-table creation rather than silently promising the old lifetime. Compare error continuation/abort behavior with the RPC baseline; do not promise identical execution outcomes to the old combined language batch.

### Proposal comparison and decision

| Aspect | Proposal 1: direct-path count fix | Proposal 2: batched `sp_executesql` |
|---|---|---|
| Batching | Retains the combined language request | Retains grouped RPC batching; no per-entry round trips |
| Simple DML counts | Correct actual counts instead of hard-coded `1` | Uses existing RPC batch handling |
| Compound/conditional entry boundaries | Unresolved in the general case; explicit limitation | RPC execution boundaries identify each parameter set |
| Response completion | Requires error-aware completion beyond the initial loop | Reuse and verify existing RPC completion/error handling |
| Local temp tables created by an entry | Retain connection lifetime | RPC-scoped, like existing procedure/unsupported-type routing |
| Main trade-off | Smaller execution change, limited count/error attribution | Reliable entry boundaries, broader application of the existing scope limitation |

These are alternatives, not sequential phases. Proposal 1 is the limited compatibility-preserving option. Proposal 2 is the stronger entry-boundary solution if batch-wide acceptance of the existing RPC-scope limitation is chosen. Do not implement per-entry round trips, static SQL-statement counting, or post-execution retry as a substitute. See §8 for proposed code-change excerpts for each alternative.

For either choice, add a CHANGELOG entry describing corrected counts, `BatchUpdateException` handling for recoverable batch failures, and the selected proposal's limitations. Fatal/session/rollback and timeout handling retain their appropriate exception behavior; not every failure becomes a `BatchUpdateException`. Until a fix ships, use `prepexec` for batches whose counts matter, or `executeUpdate` per entry where appropriate.

## 6. Work items

| # | Item | Owner | Depends on |
|---|---|---|---|
| 1 | Choose proposal 1 with explicit attribution limits, or proposal 2 accepting existing RPC-scope limitations for all batches | Team | — |
| 2 | Add failing tests from §4, with verified routing and proposal-specific expectations | — | 1 |
| 3 | Implement the chosen proposal, including error-aware response completion | — | 2 |
| 4 | Extend `MultiStatementResultTraversalTest` and run the validation matrix, including same-statement batch/non-batch reuse | — | 3 |
| 5 | Update CHANGELOG and compatibility guidance; consider a 13.6.x backport after validation | — | 4 |
| 6 | Validate with customer harness | — | 3 |

## 7. Historical local prototype (proposal 1 count mapping only)

The original analysis reports a prototype in the local worktree `C:\work\jdbc\repos\mssql-jdbc-nonebatchcounts`, uncommitted and unpushed at that time. The diff and results below are retained as historical evidence, not as implementation in this documentation PR or fresh validation of the revised proposals. The prototype corrects basic mapping/error handling for the tested shapes, but does not establish general R3–R5 correctness without entry boundaries. R7 remains unresolved, and the diff does not implement the error-aware completion required for R8.

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

How the prototype changes map to the defects (subject to the entry-attribution and completion limitations above):

| Defect | Change |
|---|---|
| R1 | `true` from `getNextResult` no longer means "ResultSet"; the count comes from the `updateCount` field |
| R2 | `-1` maps to `SUCCESS_NO_INFO` |
| R3 | Exhaustion leaves remaining slots as `EXECUTE_FAILED`; terminal-token sequences and entry attribution still need verification |
| R4 | Removes wrapping for errors observed by the loop; trailing errors are not covered by this diff |
| R5 | Session-unavailable / rolled-back-transaction errors propagate if observed; response completion is still required |
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

### 7.3 Previously reported prototype validation

These are the original author's reported results for the limited prototype. They were not rerun for this document revision and do not establish coverage for proposal 2, verified scope-mode direct routing, general compound/conditional attribution, trailing-error completion, cancellation, or counts above `Integer.MAX_VALUE`.

| Profile | Suite | Result |
|---|---|---|
| `jre17` / JDK 25 | `PrepStmtDirectSQLExecutionTests` | 86 run, 0 failures, 0 errors, 0 skipped |
| `jre8` / JDK 11 (clean) | `PrepStmtDirectSQLExecutionTests`, `PrepareMethodNoneProcedureRpcTest`, `PrepareMethodLiteralTest`, `ParameterDirectSqlRetryTest`, `PrepareMethodNoneCallableStatementTest`, `SQLServerPreparedStatementTempTableTest`, `BatchExecutionWithBulkCopyTest` | 1,557 run, 0 failures, 0 errors, 0 skipped |

Remaining before an implementation PR, in addition to the revised work items in §6:

- Run the full `none` suite set on `jre17`.
- Run `MultiStatementResultTraversalTest`. Its `@Nested` classes were not selected by the `-Dtest` filter.
- Check whether the `SEVERE ... unexpected TDS_RETURN_VALUE` lines logged during the `jre8` run also appear without this fix. All tests in that run passed.
- Add a CHANGELOG entry noting that `none` batch failures now throw `BatchUpdateException`.

## 8. Proposed code-change diffs for review

These are **illustrative implementation diffs, not compiled or server-validated patches**. Only this Markdown document is changed by this PR. The excerpts use method-context hunk headers rather than apply-ready line ranges. Proposal 1 builds on the historical diff in §7.1; proposal 2 is an alternative against the current driver source, not a change to apply on top of proposal 1. Neither excerpt includes the tests required by §4.

### 8.1 Proposal 1: actual counts plus error-aware completion

First apply the count-mapping/exception-unwrapping changes shown in §7.1. Those changes remove the outer `catch (SQLException)` wrapper, retain the combined SQL request, and let the public APIs construct `BatchUpdateException`. Replace that prototype's bounded result loop with the following review draft.

The important distinction is between the number of output slots and response completion: keep consuming after all slots have been filled. Restart result traversal when necessary to reach token EOF, rather than treating an intermediate logical end-of-results as physical response completion. Record late database errors without inventing an entry index for results beyond the array. Preserve additional observed errors in the exception chain.

```diff
--- a/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java
+++ b/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java
@@ doExecuteExecMethodBatchCombined: replace the result loop from the §7.1 prototype
-                for (int batchIdx = 0; batchIdx < numBatches; batchIdx++) {
+                int resultIndex = 0;
+                SQLServerException interruptException = null;
+                while (!batchCommand.getProcessedResponse()) {
+                    boolean unexpectedResultSet = false;
+                    startResults();
                     try {
                         if (!getNextResult(true))
-                            break;
+                            continue;

                         if (null != resultSet) {
-                            resultSet.close();
-                            resultSet = null;
+                            unexpectedResultSet = true;
                             SQLServerException.makeFromDriverError(connection, this,
                                     SQLServerException.getErrString("R_resultsetGeneratedForUpdate"), null, false);
                         }

-                        batchCommand.updateCounts[batchIdx] = (-1 == updateCount) ? Statement.SUCCESS_NO_INFO
-                                                                                  : updateCount;
+                        if (resultIndex < numBatches) {
+                            batchCommand.updateCounts[resultIndex] = (-1 == updateCount)
+                                    ? Statement.SUCCESS_NO_INFO : updateCount;
+                        }
+                        resultIndex++;
                     } catch (SQLServerException e) {
                         if (connection.isSessionUnAvailable() || connection.rolledBackTransaction())
                             throw e;

-                        batchCommand.updateCounts[batchIdx] = Statement.EXECUTE_FAILED;
-                        if (null == batchCommand.batchException)
+                        if (SQLState.STATEMENT_CANCELED.getSQLStateCode().equals(e.getSQLState())) {
+                            interruptException = e;
+                            continue;
+                        }
+                        if (!unexpectedResultSet
+                                && SQLServerException.DRIVER_ERROR_FROM_DATABASE != e.getDriverErrorCode()) {
+                            throw e;
+                        }
+                        if (resultIndex < numBatches) {
+                            batchCommand.updateCounts[resultIndex] = Statement.EXECUTE_FAILED;
+                        }
+                        resultIndex++;
+                        if (null == batchCommand.batchException) {
                             batchCommand.batchException = e;
+                        } else {
+                            batchCommand.batchException.setNextException(e);
+                        }
                     }
                 }
+                clearLastResult();
+                if (null != interruptException)
+                    throw interruptException;
+                if (null != batchCommand.batchException)
+                    executeScope.setException(batchCommand.batchException);
                 // executeBatch and executeLargeBatch turn batchCommand.batchException into a BatchUpdateException.
```

**What this draft does not solve:** `resultIndex` is a result position, not a recovered parameter-set identity. It provides exact counts only when the observed result sequence corresponds to entries. Extra counts from compound SQL are consumed but not correctly grouped; an error after an intermediate count can still have ambiguous per-entry attribution. A trailing error must fail the batch even if every returned count slot is non-negative; do not arbitrarily overwrite the last slot as if its origin were known.

**Completion review required before implementation:** reaching token EOF must be verified alongside the command's attention-acknowledgement lifecycle, transaction/session changes, and result-set cleanup. Cancellation/timeout must not be converted into normal success. Test the `startResults()`/EOF progression with intermediate DONE tokens and interrupts so the loop neither stops early nor reprocesses tokens. The existing public exception construction must also be reviewed if preserving the complete exception chain is required; recording a chain on `batchException` alone does not guarantee callers receive it.

This is a larger completion change than the historical §7 prototype. Its correctness and the proposal 1 attribution limitations must be assessed separately; the historical test results do not validate this draft.

### 8.2 Proposal 2: select unprepared RPC for direct-mode batches before submission

The following draft routes statements that would otherwise use direct SQL into the existing grouped RPC batch path. Modes already using prepared RPCs retain their existing behavior. Within the affected modes, routing is based on the batch command, so the statement's direct/non-batch flags do not need a lifetime-long switch.

For typed binding, restore RPC types on execution-local parameter clones. Do not change the original current bindings: `restoreRpcType()` clears literal-formatting metadata, and the same PreparedStatement may later execute directly without rebinding. Reuse the existing `cloneForBatch()` and `restoreRpcType()` facilities rather than temporarily changing flags and assuming that restores parameter state too.

```diff
--- a/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java
+++ b/src/main/java/com/microsoft/sqlserver/jdbc/SQLServerPreparedStatement.java
@@ doPrepExec: do not render literal SQL for a batch command
-        if (isDirectSqlExecution) {
+        if (isDirectSqlExecution && !(command instanceof PrepStmtBatchExecCmd)) {
             // Build direct SQL using enhanced replaceParameterMarkersWithValues with direct values
             String directSQL = replaceParameterMarkersWithValues(userSQL, userSQLParamPositions, params, false);
@@ doPrepExec: reuse unprepared RPC construction for each direct-mode batch entry
-        // A stored procedure call under prepareMethod=none is sent as an unprepared sp_executesql RPC with typed
-        // parameters. No server handle is created or cached. In a batch nothing has to be read back before the next
-        // entry, so the entries are combined into one request.
-        if (useUnpreparedRpc) {
+        // Direct-mode batch entries use the same unprepared RPC framing as procedure calls.
+        if (useUnpreparedRpc || (isDirectSqlExecution && command instanceof PrepStmtBatchExecCmd)) {
             isExecuteSqlUsed = true;
             buildExecSQLParams(tdsWriter);
             sendParamsByRPC(tdsWriter, params);
             return !(command instanceof PrepStmtBatchExecCmd);
         }
@@ doExecutePreparedStatementBatch: bypass combined language execution
-        // Direct SQL execution handles the batch with literal parameter substitution. This is prepareMethod=none, or
-        // scopeTempTablesToConnection when temp table operations are detected in the SQL. A stored procedure call is
-        // never direct: under none each entry is sent as an unprepared RPC. If a parameter cannot be inlined as a
-        // literal, nothing has been sent, and the batch is sent as unprepared RPCs instead.
-        if (isDirectSqlExecution) {
-            try {
-                doExecuteExecMethodBatchCombined(batchCommand);
-                return;
-            } catch (SQLServerException e) {
-                if (!retryRejectedDirectSqlAsRpc())
-                    throw e;
-                Arrays.fill(batchCommand.updateCounts, Statement.EXECUTE_FAILED);
-            }
-        } else if (isScopeTempTablesToConnectionMethod) {
+        // Direct-mode entries proceed through the grouped, unprepared RPC path below.
+        if (!isDirectSqlExecution && isScopeTempTablesToConnectionMethod) {
             // scopeTempTablesToConnection is set but no temp table operations detected in
             // SQL, fall back to default prepexec method
@@ doExecutePreparedStatementBatch: restore typed bindings without mutating retained direct bindings
                     Parameter[] paramValues = batchParamValues.get(numBatchesPrepared);
                     assert paramValues.length == batchParam.length;
-                    System.arraycopy(paramValues, 0, batchParam, 0, paramValues.length);
+                    if (isDirectSqlExecution) {
+                        for (int i = 0; i < paramValues.length; i++) {
+                            batchParam[i] = paramValues[i].cloneForBatch();
+                            batchParam[i].restoreRpcType(connection);
+                        }
+                    } else {
+                        System.arraycopy(paramValues, 0, batchParam, 0, paramValues.length);
+                    }
```

**Why batching is retained:** the existing `doExecutePreparedStatementBatch` loop writes `NBATCH_STATEMENT_DELIMITER` between RPC entries. `doPrepExec` returns `false` for unprepared batch RPCs, so this path does not force a preparation response or a round trip after every entry. The existing `reuseCachedHandle` guard already excludes `isDirectSqlExecution` and `useUnpreparedRpc`; the draft does not introduce handle caching.

**Follow-through before implementation:** remove the now-unused combined-execution helper if no caller remains, update direct-path comments/Javadoc and scope guidance, and exercise the existing RPC result/completion handling rather than assuming it covers every case. In particular, verify compound entries with trailing errors and unexpected result sets before claiming parity. Verify generated keys, statement pooling, encryption metadata, typed character conversion with `sendStringParametersAsUnicode=false`, explicit national-character setters, streams/unsupported types, and batch-to-single-execution reuse without rebinding. Execution-local clones preserve the originals' literal metadata but do not make consumable streams reusable.

**Accepted scope trade-off to document:** local temporary tables created inside an entry are RPC-scoped, just as in existing procedure-call/unsupported-type routing. Applying this routing to all otherwise-direct batches broadens that existing limitation, including `scopeTempTablesToConnection` batches. Tables created beforehand on the connection remain accessible. Do not retry direct SQL after submission; this proposal chooses RPC before execution.
