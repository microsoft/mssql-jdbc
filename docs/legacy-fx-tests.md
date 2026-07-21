# Legacy FX Test Suite

Reference for the ported legacy-FX behavioural conformance suite that lives under
`src/test/java/com/microsoft/sqlserver/jdbc/**`. Use this document to find which
tag owns a scenario before adding a new test, and to understand how the suite is
sliced in CI.

---

## 1. Pipeline architecture

Every legacy-FX test class carries **two** JUnit 5 `@Tag` values:

| Layer | Constant | Value | Purpose |
|---|---|---|---|
| Umbrella | `Constants.legacyFx` | `legacyFx` | Excluded from `junit-test.yml` (`-DexcludedGroups=...,legacyFx,...`). Lets the mainline suite skip FX. |
| Slice | `Constants.legacyFx<Area>` | `LegacyFX_<Area>` | Drives parallel fan-out in `legacyfx-junit-test.yml` (one job per slice via `-Dgroups=LegacyFX_<Area>`). |

Run a single slice locally:

```bash
mvn test -Pjre11 -Dgroups=LegacyFX_Cursor      # one area
mvn test -Pjre11 -Dgroups=legacyFx             # full FX corpus
```

---

## 2. Tag → tests → scenarios

Each section below documents one parallelization slice. **Test classes** lists
the JUnit classes carrying that slice tag. **Scenarios** lists the behaviours
asserted — not method names.

### `LegacyFX_Cursor` — Cursor matrix

- **Test classes:** `CursorMatrixTest`, `HoldabilityTest`, `CursorDowngradeTest`.
- **Scenarios:**
  - Every legal `TYPE_* × CONCUR_*` combination through both `createStatement` and `prepareStatement`.
  - `SCROLL_INSENSITIVE + UPDATABLE` throws at `createStatement` time (no silent downgrade).
  - `HOLD_CURSORS_OVER_COMMIT` vs `CLOSE_CURSORS_AT_COMMIT` honoured across `commit()`/`rollback()`; `setHoldability` overrides connection default.
  - `setMaxRows` honoured under `SCROLL_INSENSITIVE`; custom fetch size honoured under `SCROLL_SENSITIVE`.
  - Driver throws `SQLServerException` (not silent downgrade) when server cannot honour requested type/concurrency.

### `LegacyFX_ResultSet` — ResultSet navigation, getters, updaters

- **Test classes:** `ResultSetNavigationTest`, `ResultSetRowOpsTest`.
- **Scenarios:**
  - Forward + scroll navigation: `next`/`previous`/`first`/`last`/`absolute`/`relative` (incl. negative, zero, `MIN_VALUE`, out-of-range); `isBeforeFirst`/`isAfterLast`/`isFirst`/`isLast` transitions; `getRow()` ordinal correctness.
  - Every primitive getter by ordinal and by column name; conversions (BigDecimal→String, INT→String, BIT→Boolean, SMALLINT→Short); `wasNull()` after NULL.
  - Stream + LOB getters (`BinaryStream`, `AsciiStream`, `CharacterStream`, `Blob`, `Clob`, `NClob`) round-trip byte/char equality with direct getters.
  - Updaters: `updateString`/`updateInt`/`updateNull` by ordinal and name + `updateRow`; `insertRow` (incl. defaults-only); `deleteRow` then re-access throws (regression SQLBU#413368); second `deleteRow` throws; `cancelRowUpdates` reverts in-memory state.
  - Cursor close cascade: closing ResultSet, Statement *or* Connection closes ResultSet; reflective sweep verifies every public method throws afterwards.
  - `refreshRow` after PK update does not throw `ArrayIndexOutOfBoundsException` (regression).

### `LegacyFX_Statement` — Statement / PreparedStatement / CallableStatement

- **Test classes:** `StatementCTSTest`, `StatementPropertiesTest`, `RaiserrorTest`.
- **Scenarios:**
  - All execution forms (`executeQuery`/`executeUpdate`/`execute` + `getResultSet`/`getUpdateCount`) on all three statement kinds.
  - DDL returns count 0; DML returns affected rows; `SELECT INTO` correctly does not return a ResultSet.
  - JDBC escape syntax (`{call ...}`, `{fn ...}`, `{?= call ...}`); four-part stored-procedure names with OUT + return values.
  - Property semantics: `maxFieldSize`, `maxRows`, `queryTimeout`, `fetchSize`, `fetchDirection`, `escapeProcessing`, `cursorName`, `poolable`, `holdability`, `responseBuffering`. `setMaxRows` does **not** affect `executeUpdate`/`executeBatch`.
  - `CallableStatement` OUT: every primitive getter after `registerOutParameter` + `execute`; reading OUT before `execute` throws; INOUT semantics; random-order multi-OUT reads stable.
  - RAISERROR severity: 1–9 → `SQLWarning`, 11+ → `SQLException`, 15 (fatal) → statement still re-executable on same connection; nested 2/3/4-deep procs chain via `SQLException.getNextException()`.
  - Closed-state contract: reflective sweep across every public method on Statement / PreparedStatement / CallableStatement asserts `SQLException` after `close()` (and after the *Connection* closes).
  - `closeOnCompletion`: closing last open ResultSet auto-closes the parent Statement.
  - `SET NOCOUNT ON` suppresses row count but preserves generated keys; `FOR XML AUTO` round-trips; `getMoreResults(CLOSE_*)` flags close prior ResultSets.
  - Bracket-escape regression: identifiers with spaces/brackets work for reads and updatable inserts.

### `LegacyFX_CTS` — JDBC CTS conformance

- **Test classes:** `ConnectionCTSTest`, `MetadataCTSTest`, `ResultSetCTSTest`, plus the CTS-style sections of the Statement classes above.
- **Scenarios:**
  - Connection lifecycle: `isValid(5)` true on open, false after `close()`; subsequent ops throw cleanly. Default `autoCommit` true; all four isolation levels toggle and persist. `getCatalog`/`getSchema` non-null. `nativeSQL` non-empty for SELECT and `{fn ...}`. `getNetworkTimeout` non-negative. `setReadOnly` is a deliberate no-op (`isReadOnly()` always false).
  - DatabaseMetaData: product/driver name + versions, JDBC spec major/minor, URL, user. Every JDBC capability flag asserted (procedures, batch, savepoints, named params, multiple open results, generated keys, holdability, GROUP BY, ORDER BY unrelated, UNION/UNION ALL, outer/full outer joins, subqueries in IN/EXISTS/comparisons, all four isolation levels, all three ResultSet types, SQL grammar tiers).
  - Catalog/schema introspection: `getCatalogs`, `getSchemas`, `getTables`, `getTableTypes`, `getColumns`, `getPrimaryKeys`, `getImportedKeys`, `getExportedKeys`, `getIndexInfo`, `getBestRowIdentifier`, `getVersionColumns`, `getTypeInfo` each return non-empty well-formed ResultSet.
  - ResultSetMetaData: column count, name, label (honours `AS`), JDBC type code, server type name, NULLABLE, autoincrement, precision/scale, display size, isReadOnly/Writable/DefinitelyWritable, isSearchable, isSigned, isCaseSensitive, Java class name, schema/table/catalog (catalog may be empty on Azure SQL — pinned).
  - `VARCHAR(MAX)`/`NVARCHAR(MAX)` report `2,147,483,647` precision; computed columns flagged read-only.

### `LegacyFX_Security` — Credential hygiene & SQL injection defence

- **Test classes:** `CredentialHygieneTest`, `SqlInjectionTest`, `InputBoundaryTest`.
- **Scenarios:**
  - Reflection sweep on `SQLServerConnection` — password-like fields are private + `transient`/unreadable; no public getter exposes them.
  - 6 induced failure paths (bad server, bad credentials, excessive `loginTimeout`, unknown keyword, invalid catalog, bad savepoint name) — full `SQLException` + `getCause()` chain scanned for the configured password (quoted, URL-encoded, key=value forms). Same scan against `java.util.logging` at `FINEST`.
  - `DriverPropertyInfo` — password/accessToken entry present but `value` field redacted/null.
  - `Connection.toString()` never contains password or the keyword name used for it.
  - Blank and missing `password` do not silently succeed against SQL-auth servers.
  - SQL-injection catalogue (`'`, `"`, `]`, CR/LF, `; DROP TABLE`, `' OR 1=1 --`, `xp_cmdshell`, `WAITFOR`) injected through: every `PreparedStatement` setter incl. `setObject` coercions, CallableStatement named/positional params, updatable cursors (`updateString` + `updateRow`), server-side cursors, batched inserts, `setCatalog`, `setSavepoint(name)`, `rollback(savepoint)`, `setCursorName`, all DatabaseMetaData pattern parameters, connection-string fragments. Proof: original test table + row count survive intact.
  - Buffer-boundary: insert at exactly `VARCHAR(8000)`/`NVARCHAR(4000)`/`VARBINARY(8000)` round-trips; oversize either truncates predictably or throws cleanly with the connection still usable.
  - VARCHAR OUT 8000-byte regression: requires `sendStringParametersAsUnicode=false` + 4-arg `registerOutParameter(idx, VARCHAR, 8000, 0)` — pinned.

### `LegacyFX_SSL` — TLS & server-identity validation

- **Test classes:** `TLSConnectionTest`, `EncryptModesTest`, `ServerSpoofingTest`.
- **Scenarios:**
  - Explicit TLS 1.2 succeeds; invalid protocol string rejected (no silent downgrade).
  - `encrypt=true|false|strict` honoured; `strict` enforces JVM trust store + CN match (hostname mismatch fatal).
  - `trustServerCertificate=true|false` round-trips through `Connection` properties; settings do not bleed across simultaneous connections.
  - `hostNameInCertificate` overrides hostname used for cert matching.
  - Same encryption properties via `DriverManager` and `SQLServerDataSource`.
  - Concurrent TLS-enabled connections from one JVM — no handshake races.
  - Server-spoofing defences: crafted redirect fragments blocked; SQL Browser instance-port spoofing cannot bypass TLS validation.

### `LegacyFX_AE` — Always Encrypted

- **Test classes:** `AEDataTypeEdgeCaseTest`, `CEKCacheTTLTest`, `CMKRotationTest`, `TrustedKeyPathTest`.
- **Scenarios:**
  - Edge values across encrypted types: `INT/BIGINT/SMALLINT/TINYINT` MIN/MAX, `DECIMAL(38,10)` max precision + zero + negative, `FLOAT` ±0.0, NULL for every type with `wasNull()`, `VARCHAR(8000)` / `NVARCHAR(4000)` / `VARBINARY(8000)` at column max, `DATE` `0001-01-01` and `9999-12-31`, `TIME(0..7)`, `BIT` true/false, empty string + empty `byte[]`.
  - CEK cache TTL: zero (disables), positive seconds/minutes/hours; runtime change takes effect on next op; multiple TTL changes do not leak old cache.
  - `setTrustedMasterKeyPaths`: single-server map, empty map (clears trust), overwrite (atomic), multi-server map.
  - `sys.column_master_keys` / `sys.column_encryption_keys` enumerable via Database APIs; each CEK row references a valid CMK.
  - Multiple CMK providers register side-by-side and are reported.

### `LegacyFX_Globalization` — Collations, locales, UTF-16

- **Test classes:** `CollationRoundTripTest`, `GlobalizedIdentifierTest`, `Utf16MatrixTest`, `CalendarOverloadTest`.
- **Scenarios:**
  - Collation round-trip per language/collation pair (single-byte, multibyte, `_SC` supplementary). Turkish CP1254 `ğ` via updatable ResultSet (write through `updateString`).
  - `NVARCHAR` at width 1, 4000, MAX with non-ASCII payloads.
  - Non-ASCII identifiers (CJK, accented Latin): create, SELECT/INSERT, `findColumn` across four casing variants.
  - `N'…'` literals with surrogate pairs round-trip.
  - `getDate(int, Calendar)` / `getTime` / `getTimestamp` across US, JP, AR locales (incl. non-Gregorian) — same coverage on CallableStatement OUT.
  - `getString` locale-independence — parameterised sweep across `Locale.getAvailableLocales()`.
  - UTF-16 matrix: 4 char types × 2 positions × 15 send methods × 18 receive methods, round-robin sampled.

### `LegacyFX_TVP` — Table-Valued Parameters

- **Test classes:** `TVPDataRecordTest`, `TVPDataTableTest`, `TVPBatchTest`, `TVPImmutabilityTest`, `TVPFuzzTest`, `TVPPrecisionScaleTest`, `TVPSchemaQualifiedTest`, `TVPTypeBoundaryTest`.
- **Scenarios:**
  - `ISQLServerDataRecord` streaming: empty, 1000 rows, all-null, mismatched column count, type mismatch mid-iteration, exception inside `next()` propagates, reuse after iterator consumed, schema-qualified TVP names.
  - `SQLServerDataTable`: null DataTable rejected, null TVP name rejected, under-populated rows rejected, null column names rejected, alternating null/non-null patterns, distinction between empty string and SQL NULL preserved.
  - Batches: `addBatch`/`executeBatch` for mixed-null, all-null, all-types, via stored proc, empty TVP, 100-batch scale, with transaction rollback, mixed TVP + non-TVP.
  - Immutability: duplicate column names rejected; prior-execution data not mutated by reuse; modify-between-executions takes effect on next execute only.
  - Fuzz: Unicode in TVP/param names, random JDBC type IDs, `addRow` wrong column count, out-of-range parameter index, extreme value sizes.
  - 33-column TVP exercising every precision/scale of `NUMERIC`, `DECIMAL`, `FLOAT`, `TIME(0/4/7)`, `DATETIME2(3/4/7)`, `DATETIMEOFFSET(0/4/7)`.
  - Schema-qualified TVPs via Prepared/Callable, via `setStructured` and `setObject`, incl. procedures returning structured results.

### `LegacyFX_XA` (slice) + `legacyFx_Xa` (umbrella) — Distributed transactions

- **Test classes:** `DTCTimeoutTest`, `XAResourceTest`, `XAStateWalkTest`.
- **Scenarios:**
  - `XADataSource.getXAConnection()` → usable `XAConnection`; `XAResource.start/end/prepare/commit/rollback/forget` follow spec.
  - 1-phase commit (`commit(xid, true)`) and 2-phase (`prepare` + `commit(xid, false)`); rollback before and after prepare.
  - `setTransactionTimeout(int)` — non-negative accepted; 0 = provider default; timeout actually fires and rolls back; `getTransactionTimeout()` round-trips.
  - `recover(TMSTARTRSCAN)` returns prepared xids; `forget(xid)` removes one.
  - `isSameRM` true for same DB, false otherwise; suspend/resume (`TMSUSPEND`/`TMRESUME`) keeps work in same xact; multiple concurrent xids on one RM.
  - Logical-close on XA pooled connection releases physical resource without affecting outstanding prepared xids.
  - Every `XAException.XA*` error code surfaced via deliberately illegal state transitions (e.g. `end` after `commit`).
  - All four isolation levels honoured inside XA transactions (verified via `DBCC USEROPTIONS`-style query).
  - Tightly-coupled sibling XA transactions commit together.
  - Randomized state walk `IDLE → STARTED → (SUSPENDED?) → ENDED → PREPARED → COMPLETED` with error paths and recorded seed.

### `LegacyFX_Routing` — Read-only routing & failover

- **Test classes:** `ReadonlyRoutingTest`.
- **Scenarios:**
  - `ApplicationIntent=ReadOnly` lands on secondary; `ReadWrite` always on primary; default is `ReadWrite`.
  - `loginTimeout` honoured during routing handshake (not just initial socket open).
  - `multiSubnetFailover=true` via AG listener; combined with `ReadOnly` routes to secondary.
  - Classic `failoverPartner` keyword still works for legacy mirroring.
  - Read-only routing under `encrypt=true` does not break TLS.

### `LegacyFX_Stress` — Cancel, timeout, concurrency

- **Test classes:** `StressTest`, `CancelTimeoutTest`, `PoolExhaustionTest`.
- **Scenarios:**
  - `Statement.cancel()` before `execute` is a clean no-op; cancel from another thread aborts blocked `executeQuery` and unblocks it; cancel on closed Statement does not throw; cancelled statement is re-executable.
  - `setQueryTimeout(n)` fires for long SELECT, server-cursor scan, `WAITFOR`, and mid-batch; `0` disables.
  - Repeated cancel-in-tight-loop does not leak server-side requests; "hammer cancel" leaves connection usable.
  - Many-thread `SELECT @@VERSION` open/exec/close completes within bound (no deadlock); ~50-cycle open/close churn leaks nothing.
  - Pool exhaustion is recoverable; pool identity isolates different `databaseName` into distinct logical pools.

### `LegacyFX_StateMachine` — Model-based random walks

- **Test classes:** `ResultSetRandomWalkTest`, `StatementRandomWalkTest`, `TransactionRandomWalkTest`, `XaRandomWalkTest`.
- **Scenarios:**
  - Each harness runs a weighted random sequence; after every action both driver state and an in-memory `DataCache` model are checked for divergence; invariants asserted at end; **seed recorded per run for reproducibility**.
  - ResultSet walk drives scrollable updatable cursor over 10-row table — main detector for cursor position/state bugs.
  - Statement walk weights ~40 action types (every cursor × statement kind, batch incl. PK violations, cancel from other thread, RAISERROR severities, every property setter at legal+invalid values, response buffering toggle, generated keys, FOR XML AUTO, `SELECT INTO` failure, full closed-state matrix, `sendStringParametersAsUnicode` × in/out-of-collation).
  - Transaction walk = e-commerce simulation: weighted inserts/updates/deletes/commits/rollbacks/savepoints/duplicate-PK/autoCommit toggles; cross-checks `commitCount`/`rollbackCount` invariants — catches autoCommit-stuck bugs.
  - XA walk drives every legal `XAResource` transition plus sampled illegal ones.

---

## 3. Known driver behaviours the suite pins

These are deliberate driver behaviours the tests lock in so an unintended change is caught immediately:

- `setReadOnly(true)` is a no-op; `isReadOnly()` always returns `false`.
- `TYPE_SCROLL_INSENSITIVE + CONCUR_UPDATABLE` throws at `createStatement` time, not at `executeQuery`.
- Default `sendStringParametersAsUnicode=true` caps VARCHAR OUT at 4000 chars — VARCHAR(8000) OUT tests override to `false`.
- `ResultSetMetaData.getCatalogName(i)` may be empty string on Azure SQL — tests assert non-null, not non-empty.
- Batches mixing DML with OUT params or with a SELECT-returning proc throw `BatchUpdateException`.
- `Connection.setCursorName(null)` does not throw.

---

## 4. Tag-layer issues (tracked for follow-up)

| # | Issue | Suggested fix |
|---|---|---|
| 1 | `Constants.legacyFxXa = "legacyFx_Xa"` and `Constants.legacyFxXA = "LegacyFX_XA"` differ only by Java identifier casing but mean different things (umbrella vs slice). | Rename to `legacyFxXaUmbrella` / `legacyFxXAParallel`, or drop the umbrella and use `legacyFx` + `LegacyFX_XA`. |
| 2 | Mixed value casing: `"legacyFx"` (camelCase) vs `"LegacyFX_<Area>"` (Pascal + uppercase `FX`). | Standardize on `LegacyFX` + `LegacyFX_<Area>` to match pipeline job names. |
| 3 | No `LegacyFX_Transactions` slice — local-txn coverage folded into `LegacyFX_StateMachine`. | Add `Constants.legacyFxTransactions = "LegacyFX_Transactions"`; tag `Transaction*Test`/`Savepoint*Test` + add `Test_LegacyFx_Transactions` pipeline job. |
| 4 | No `LegacyFX_Batching` slice — batching folded into `LegacyFX_Statement`. | Add `Constants.legacyFxBatching = "LegacyFX_Batching"`; tag `*BatchTest`. |
| 5 | `LegacyFX_DataTypes` constant defined but unused — data-type tests carry `LegacyFX_AE` or `LegacyFX_Statement`. | Either delete the constant or apply it to sparse-columns + legacy→MAX classes and add the job. |
| 6 | Umbrella exclusion list in `junit-test.yml` hard-codes the Maven group strings — no single source of truth. | Generate from `Constants.java` at build time, or commit a shared `legacyfx-tags.txt`. |

---

## 5. Adding a new test

1. Identify the slice in §2 that owns the behaviour. Re-use an existing test class in that area where possible.
2. Apply both tags on the class:
   ```java
   @Tag(Constants.legacyFx)
   @Tag(Constants.legacyFx<Area>)
   public class MyTest { ... }
   ```
3. Add one bullet under the relevant slice in §2 describing the *behaviour* asserted — not the method name. CI will pick the test up via the existing `Test_LegacyFx_<Area>` job.
