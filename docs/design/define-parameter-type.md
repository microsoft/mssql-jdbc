# `defineParameterType` — Parameter Length Hint for Variable-Length Types

## Table of Contents

1. [Issue](#1-issue)
2. [How Oracle Solves This](#2-how-oracle-solves-this)
3. [Why SQL Server / TDS Behaves Differently](#3-why-sql-server--tds-behaves-differently)
4. [Proposed API](#4-proposed-api)
5. [Affected Data Types](#5-affected-data-types)
6. [Types Explicitly Excluded](#6-types-explicitly-excluded)
7. [Implementation Details](#7-implementation-details)
8. [Interaction with Bulk Copy API](#8-interaction-with-bulk-copy-api)
9. [Test Scenarios](#9-test-scenarios)

---

## 1. Issue

**GitHub Issue:** [#2913](https://github.com/microsoft/mssql-jdbc/issues/2913) — String parameters always sent as `varchar(8000)` / `nvarchar(4000)`.

### Root cause

When a Java application calls `setString`, `setNString`, `setBytes` etc. on a `PreparedStatement`,
mssql-jdbc always declares the parameter in TDS using the maximum bounded width regardless of
the actual value size or any developer-provided hint:

| Setter | TDS TypeInfo sent today | Server memory grant (10k-row batch) |
|---|---|---|
| `setString("hello")` | `varchar(8000)` | ~76 MB |
| `setNString("hello")` | `nvarchar(4000)` | ~38 MB |
| `setBytes(new byte[20])` | `varbinary(8000)` | ~76 MB |

SQL Server uses the declared TypeInfo width to compute **memory grants** for query execution
plans. When a parameter is declared `varchar(8000)` but the column holds values of at most
50 characters, the server over-allocates ~160× more memory per row than needed. This
compounds across batch sizes and concurrent sessions.

### JDBC spec note

`PreparedStatement.setObject(int, Object, int, int)` has a `scaleOrLength` argument, but the
JDBC 4.3 specification explicitly states:

> *"For all other types \[than DECIMAL/NUMERIC and streams\], this value will be ignored."*

Therefore `scaleOrLength` cannot be used as the length hint for VARCHAR. A driver-extension
method is required — the same reason Oracle introduced `defineParameterType`.

---

## 2. How Oracle Solves This

Oracle's `ojdbc` exposes a proprietary extension on `OraclePreparedStatement`:

```java
void defineParameterType(int parameterIndex, int type, int maxSize)
```

### What it does internally

Reflection on `oracle.jdbc.driver.T4CPreparedStatement` (the real implementation behind
`OraclePreparedStatementWrapper`) reveals that **the only internal field changed** by
`defineParameterType` is `parameterMaxLength` — an `int[][]` indexed by parameter position:

```
Before:  parameterMaxLength = null
After defineParameterType(VARCHAR, 50):  parameterMaxLength[0][0] = 50
```

This value is stored on the prepared statement and **persists across all `addBatch` /
`setString` iterations**. It is passed to the server as a memory-allocation hint inside
the bind descriptor. Oracle never enforces it as a truncation boundary — the server
treats it purely as a memory hint.

### Oracle's batch memory savings (VARCHAR)

| Call | Server memory per bind slot | 10k-row batch |
|---|---|---|
| No `defineParameterType` | `minVcsBindSize = 32,766` bytes | ~320 MB |
| `defineParameterType(VARCHAR, 50)` | 50 bytes | ~488 KB |

### Oracle CLOB: a bigger win (not relevant to SQL Server)

For CLOB columns, `defineParameterType(VARCHAR, N)` does more than a memory hint:
it switches the entire wire protocol from "send a LOB locator" to "send inline bytes",
eliminating temporary LOB creation on the server entirely. SQL Server has no concept
of a LOB locator for input parameters, so this aspect has no equivalent.

### Numeric types

For `NUMERIC`, the precision hint does not change the OCI slot size (always `SQLT_NUM` =
22 bytes). Only changing the Java type to `INTEGER` or `DOUBLE` changes the slot. This
is fundamentally different from SQL Server's `DECIMALN`, where storage already shrinks
with precision (5–17 bytes), making this optimization less relevant for numeric types.

---

## 3. Why SQL Server / TDS Behaves Differently

### TDS wire format comparison

```
varchar(N)   : [0xA7 BIGVARCHR] [maxLen=N : 2B] [collation: 5B] [valueLen: 2B] [bytes]
               └─ server uses N for memory grant in query plan

varchar(max) : [0xA5 BIGVARCHAR] [maxLen=0xFFFF MAX marker] [collation: 5B]
               [PLP chunks: len+data, ...]  [0x00000000 terminator]
               └─ PLP streaming: no upfront fixed allocation
```

### SQL Server enforces the declared TypeInfo length

This is a key difference from Oracle: SQL Server treats the declared TypeInfo width as
the actual type constraint, not just a memory hint. However, **the driver does not
compute actual value lengths** — that would add per-row overhead and defeat the purpose
of the feature.

Instead, the hint is declared on the wire as-is:

- If a value fits within the hint, it is sent normally.
- If a value is larger than the hint, **SQL Server returns a truncation error** — the
  same contract as Oracle's `defineParameterType`. The user is responsible for providing
  an accurate hint.

This is intentional. The hint is called once before a batch loop; the caller is asserting
that their data will never exceed `maxLength`. If that assertion is wrong, the server
errors — which is the correct behaviour (fail-fast on bad data).

One protocol-level minimum is enforced by the driver: `varchar(0)` is an invalid TDS type
token, so a hint of `0` is promoted to `varchar(1)` via `Math.max(hint, 1)`. This is a
protocol guard, not a data-correctness guard.

---

## 4. Proposed API

Add a single new driver-extension method to `ISQLServerPreparedStatement` and
`SQLServerPreparedStatement`:

```java
/**
 * Specifies the SQL type and maximum character/byte length for a parameter.
 * When set, the driver uses this length as the upper bound of the TDS TypeInfo
 * declaration instead of the default maximum (e.g. varchar(50) instead of
 * varchar(8000)), allowing SQL Server to compute a tighter memory grant for
 * query execution plans.
 *
 * <p>The hint persists across all {@code setXxx} / {@code addBatch} calls on
 * this prepared statement. It is a hint only — the actual value length is always
 * respected to prevent SQL Server from truncating data.</p>
 *
 * <p>Supported target types: {@link java.sql.Types#VARCHAR},
 * {@link java.sql.Types#CHAR}, {@link java.sql.Types#NVARCHAR},
 * {@link java.sql.Types#NCHAR}, {@link java.sql.Types#VARBINARY},
 * {@link java.sql.Types#BINARY}.</p>
 *
 * @param parameterIndex 1-based parameter index
 * @param sqlType        {@code java.sql.Types} constant for the target SQL type
 * @param maxLength      maximum expected length in characters (VARCHAR/NVARCHAR/CHAR/NCHAR)
 *                       or bytes (VARBINARY/BINARY); must be &gt; 0
 * @throws SQLServerException if the parameter index is out of range, maxLength is
 *                            negative, or the sqlType is not a supported variable-length type
 */
public void defineParameterType(int parameterIndex, int sqlType, int maxLength)
        throws SQLServerException;
```

### Usage example

```java
SQLServerPreparedStatement ps = conn.unwrap(SQLServerPreparedStatement.class)
        .prepareStatement("INSERT INTO users (name, code, avatar) VALUES (?, ?, ?)");

// Hint once, before the batch loop
ps.defineParameterType(1, Types.VARCHAR,  100);  // name  VARCHAR(100)
ps.defineParameterType(2, Types.NVARCHAR,  10);  // code  NVARCHAR(10)
ps.defineParameterType(3, Types.VARBINARY, 64);  // avatar VARBINARY(64)

for (User u : users) {
    ps.setString(1, u.getName());
    ps.setNString(2, u.getCode());
    ps.setBytes(3, u.getAvatarBytes());
    ps.addBatch();
}
ps.executeBatch();
// Wire sends: varchar(100), nvarchar(10), varbinary(64)
// instead of: varchar(8000), nvarchar(4000), varbinary(8000)
```

---

## 5. Affected Data Types

Only **bounded variable-length types** where a numeric width appears in the TDS TypeInfo:

| JDBC Type | Default TypeInfo today | TypeInfo with hint | Notes |
|---|---|---|---|
| `Types.VARCHAR` | `varchar(8000)` | `varchar(N)` | Primary target |
| `Types.CHAR` | `varchar(8000)` | `varchar(N)` | Driver maps CHAR → VARCHAR on wire |
| `Types.NVARCHAR` | `nvarchar(4000)` | `nvarchar(N)` | Primary target |
| `Types.NCHAR` | `nvarchar(4000)` | `nvarchar(N)` | Driver maps NCHAR → NVARCHAR on wire |
| `Types.VARBINARY` | `varbinary(8000)` | `varbinary(N)` | Binary equivalent |
| `Types.BINARY` | `varbinary(8000)` | `varbinary(N)` | Driver maps BINARY → VARBINARY on wire |

Length boundary rules (matching existing AE logic):

```
VARCHAR / CHAR    : N ≤ 8000  → varchar(N)    N > 8000  → varchar(max)
NVARCHAR / NCHAR  : N ≤ 4000  → nvarchar(N)   N > 4000  → nvarchar(max)
VARBINARY / BINARY: N ≤ 8000  → varbinary(N)  N > 8000  → varbinary(max)
```

---

## 6. Types Explicitly Excluded

### CLOB / NCLOB / LONGVARCHAR / LONGNVARCHAR → `varchar(max)` / `nvarchar(max)` (unchanged)

These are inherently unbounded by definition (can hold up to 2 GB). A developer who
calls `setClob(reader)` is explicitly saying "I have data of unknown size."

Additionally, `varchar(max)` uses **PLP (Partially Long Pointer) streaming** in TDS —
the server allocates memory lazily as chunks arrive, not upfront. There is no fixed
memory grant problem to solve.

In Oracle, `defineParameterType(VARCHAR, N)` on a CLOB column works because it bypasses
the "temporary LOB locator" mechanism entirely. SQL Server has no temporary LOB locators
for input parameters — data is always sent inline — so there is nothing to bypass.

### BLOB / LONGVARBINARY → `varbinary(max)` (unchanged)

Same reasoning: unbounded, PLP streaming, no fixed allocation.

### DECIMAL / NUMERIC (no change, already handled)

mssql-jdbc has `calcBigDecimalPrecision=true` (connection property) which computes the
exact precision from the actual `BigDecimal` value — the equivalent of this feature for
numeric types, already shipped.

Furthermore, TDS `DECIMALN` storage already shrinks with precision (5–17 bytes), so
the over-declaration cost is much smaller than the VARCHAR case (8000 bytes/row).

### Fixed-width types (INT, BIGINT, FLOAT, BIT, DATE, DATETIME2, etc.)

These types have no width field in their TDS TypeInfo. No hint is applicable or needed.

---

## 7. Implementation Details

### 7.1 `Parameter.java` — new flag

A single new boolean field is added to `Parameter`:

```java
// Set to true when defineParameterType() has been called for this parameter.
// When true, valueLength holds the caller-supplied max-length hint and the type
// definition (e.g. varchar(N)) is built from that hint rather than the conservative
// driver default (e.g. varchar(8000) / nvarchar(4000) / varbinary(8000)).
// The hint is declared on the wire as-is; if a value larger than the hint is bound,
// SQL Server will return a truncation error.
private boolean defineParameterTypeCalled = false;
```

`defineParameterType` calls `param.setValueLength(maxLength)` (which stores the hint in
the existing `valueLength` field and sets `userProvidesPrecision = true`) and
`param.setDefineParameterTypeCalled(true)`. The `sqlType` argument is **validated but not
stored** — `setTypeDefinition()` already routes via `dtv.getJdbcType()`, which reflects
what `setString` / `setNString` / `setBytes` etc. actually placed on the parameter.

### 7.2 `setTypeDefinition()` in `Parameter.java` — `GetTypeDefinitionOp`

All three affected type families follow the same pattern — AE check first, then the hint
branch as an `else if`:

```java
// CHAR / VARCHAR
if (param.shouldHonorAEForParameter && (null != jdbcTypeSetByUser)
        && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
    // AE path — exact length from value; untouched
} else if (param.defineParameterTypeCalled) {
    // defineParameterType hint: declare the user-specified length directly.
    // If a value larger than maxLength is bound, SQL Server will return a truncation error.
    int hint = param.valueLength;
    if (hint >= DataTypes.SHORT_VARTYPE_MAX_BYTES) {
        param.typeDefinition = VARCHAR_MAX;
    } else {
        param.typeDefinition = SSType.VARCHAR.toString() + "(" + Math.max(hint, 1) + ")";
        // Math.max(hint, 1): varchar(0) is an invalid TDS token; minimum is varchar(1)
    }
} else {
    param.typeDefinition = VARCHAR_8K;  // default — unchanged
}
```

The same `else if` pattern applies to `NCHAR/NVARCHAR` (limit: `SHORT_VARTYPE_MAX_CHARS`
= 4000) and `BINARY/VARBINARY` (limit: `SHORT_VARTYPE_MAX_BYTES` = 8000).

### 7.3 `SQLServerPreparedStatement.java`

```java
@Override
public void defineParameterType(int parameterIndex, int sqlType, int maxLength)
        throws SQLServerException {
    checkClosed();
    if (maxLength < 0) {
        SQLServerException.makeFromDriverError(connection, this,
                SQLServerException.getErrString("R_invalidParameterLength"), null, false);
    }
    // Validate that sqlType is one of the supported bounded variable-length types.
    // The value is not stored — setTypeDefinition() routes via dtv.getJdbcType().
    switch (sqlType) {
        case Types.VARCHAR: case Types.CHAR:
        case Types.NVARCHAR: case Types.NCHAR:
        case Types.VARBINARY: case Types.BINARY:
            break;
        default:
            throw new SQLServerException(
                    new MessageFormat(SQLServerException.getErrString(
                            "R_unsupportedTypeForDefineParamType"))
                            .format(new Object[]{sqlType}), null, 0, null);
    }
    Parameter param = setterGetParam(parameterIndex);
    param.setDefineParameterTypeCalled(true);
    param.setValueLength(maxLength); // also sets userProvidesPrecision = true
}
```

### 7.4 Error messages — `SQLServerResource.java`

```java
{"R_invalidParameterLength",
    "The maxLength value {0} is not valid. maxLength must be >= 0."},
{"R_unsupportedTypeForDefineParamType",
    "The SQL type {0} is not supported by defineParameterType. "
    + "Supported types: VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY."},
```

### 7.5 `cloneForBatch()` in `Parameter.java`

The flag and hint must survive batch cloning so every row in the batch uses the same
type declaration:

```java
clonedParam.defineParameterTypeCalled = defineParameterTypeCalled;
clonedParam.valueLength              = valueLength;
clonedParam.userProvidesPrecision    = userProvidesPrecision;
```

### 7.6 Interaction with AE (Always Encrypted)

The `defineParameterTypeCalled` branch is placed in the `else if` of the AE check, so
it is mutually exclusive with the AE path:

```
if (AE active for param)  →  AE path (exact length from value; defines encrypted type)
else if (defineParameterTypeCalled)  →  hint path
else  →  conservative default (varchar(8000) etc.)
```

No AE behaviour changes. The hint is silently ignored when AE is active, which is correct:
AE requires exact type information that must be derived from the actual value.

---

## 8. Interaction with Bulk Copy API

### 8.1 Explicit `SQLServerBulkCopy` API — no interaction

`SQLServerBulkCopy` is a completely separate API from `PreparedStatement`.
It derives column type metadata from:
- `ResultSetMetaData` of a source `ResultSet`, or
- `SQLServerMetaData` provided by a custom `ISQLServerBulkRecord`

`defineParameterType` has no relationship to either of these.

### 8.2 `useBulkCopyForBatchInsert=true` — no interaction, already optimal

When this connection property is set, `executeBatch()` on a prepared INSERT internally
uses the Bulk Copy protocol. The column metadata for `SQLServerBulkBatchInsertRecord`
is built from the **server's destination table definition** — not from `Parameter` state:

```java
// SQLServerPreparedStatement.java (lines 2484–2488)
batchRecord.addColumnMetadata(
    i,
    c.getColumnName(),
    jdbctype,
    ti.getPrecision(),   // ← TypeInfo from the destination table on the server
    ti.getScale());
```

`defineParameterType` stores its hint in `Parameter.valueLength`, which is never read
by this code path. The Bulk Copy path already uses the actual column widths from the
server schema — which are already optimal.

### 8.3 Summary

| Execution path | Type width source | Effect of `defineParameterType` |
|---|---|---|
| Normal `execute()` / `executeBatch()` | `Parameter.typeDefinition` — `varchar(8000)` problem | **Fix applies here** |
| `useBulkCopyForBatchInsert=true` | Server's destination `TypeInfo.getPrecision()` | None — already uses actual column width |
| Explicit `SQLServerBulkCopy` | Source `ResultSetMetaData` or user `SQLServerMetaData` | None — separate API entirely |

---

## 9. Test Scenarios

The following scenarios must be covered. No test code is written yet — this table
defines what must be validated.

### 9.1 Basic correctness

| # | Scenario | Input | Expected TypeInfo on wire | Pass condition |
|---|---|---|---|---|
| T01 | VARCHAR hint, value shorter than hint | `defineParameterType(1, VARCHAR, 50)` + `setString("hi")` | `varchar(50)` | TypeInfo = `varchar(50)`, value not truncated |
| T02 | VARCHAR hint, value exactly at hint | `defineParameterType(1, VARCHAR, 5)` + `setString("hello")` | `varchar(5)` | TypeInfo = `varchar(5)`, value intact |
| T03 | VARCHAR hint, value longer than hint | `defineParameterType(1, VARCHAR, 3)` + `setString("Muskan")` | SQL Server truncation error | User is responsible for accurate hint; driver does not expand the declared type |
| T04 | NVARCHAR hint | `defineParameterType(1, NVARCHAR, 20)` + `setNString("テスト")` | `nvarchar(20)` | TypeInfo = `nvarchar(20)` |
| T05 | VARBINARY hint | `defineParameterType(1, VARBINARY, 64)` + `setBytes(new byte[10])` | `varbinary(64)` | TypeInfo = `varbinary(64)` |
| T06 | CHAR hint (maps to varchar) | `defineParameterType(1, CHAR, 10)` + `setString("x")` | `varchar(10)` | TypeInfo = `varchar(10)` |
| T07 | NCHAR hint (maps to nvarchar) | `defineParameterType(1, NCHAR, 10)` + `setNString("x")` | `nvarchar(10)` | TypeInfo = `nvarchar(10)` |
| T08 | BINARY hint (maps to varbinary) | `defineParameterType(1, BINARY, 10)` + `setBytes(new byte[5])` | `varbinary(10)` | TypeInfo = `varbinary(10)` |

### 9.2 Null and empty values

| # | Scenario | Input | Expected TypeInfo | Pass condition |
|---|---|---|---|---|
| T09 | NULL value with hint | `defineParameterType(1, VARCHAR, 50)` + `setNull(1, VARCHAR)` | `varchar(50)` | TypeInfo = `varchar(50)`, NULL sent |
| T10 | Empty string with hint | `defineParameterType(1, VARCHAR, 50)` + `setString("")` | `varchar(50)` | TypeInfo = `varchar(50)`, 0-length value |
| T11 | No hint, null value | No `defineParameterType` + `setNull(1, VARCHAR)` | `varchar(8000)` | Default behaviour unchanged |

### 9.3 Boundary values

| # | Scenario | Input | Expected TypeInfo | Pass condition |
|---|---|---|---|---|
| T12 | Hint exactly at VARCHAR max (8000) | `defineParameterType(1, VARCHAR, 8000)` | `varchar(8000)` | TypeInfo = `varchar(8000)` |
| T13 | Hint above VARCHAR max (8001) | `defineParameterType(1, VARCHAR, 8001)` | `varchar(max)` | Automatically promotes to `varchar(max)` |
| T14 | Hint exactly at NVARCHAR max (4000) | `defineParameterType(1, NVARCHAR, 4000)` | `nvarchar(4000)` | TypeInfo = `nvarchar(4000)` |
| T15 | Hint above NVARCHAR max (4001) | `defineParameterType(1, NVARCHAR, 4001)` | `nvarchar(max)` | Automatically promotes to `nvarchar(max)` |
| T16 | Hint = 1 | `defineParameterType(1, VARCHAR, 1)` + `setString("A")` | `varchar(1)` | TypeInfo = `varchar(1)` |
| T17 | Value exceeds 8000 chars (no hint) | `setString(9000-char string)` | `varchar(max)` | Existing behaviour — value length auto-promotes to MAX |
| T18 | Hint=3, value is 9000 chars | `defineParameterType(1, VARCHAR, 3)` + `setString(9000-char string)` | SQL Server truncation error | Driver declares `varchar(3)`; server errors on overflow — user must set an accurate hint |

### 9.4 Error cases

| # | Scenario | Expected exception | Pass condition |
|---|---|---|---|
| T19 | Negative maxLength | `defineParameterType(1, VARCHAR, -1)` | `SQLServerException` with `R_invalidParameterLength` | Exception thrown, message contains length |
| T20 | Unsupported sqlType | `defineParameterType(1, Types.INTEGER, 10)` | `SQLServerException` with `R_unsupportedTypeForDefineParamType` | Exception thrown |
| T21 | Unsupported sqlType CLOB | `defineParameterType(1, Types.CLOB, 100)` | `SQLServerException` | Exception thrown |
| T22 | Parameter index out of range | `defineParameterType(99, VARCHAR, 50)` on 1-param statement | `SQLServerException` | Exception thrown |
| T23 | Called after connection closed | Call `defineParameterType` after `connection.close()` | `SQLServerException` | Exception thrown |

### 9.5 Batch behaviour

| # | Scenario | Input | Expected behaviour | Pass condition |
|---|---|---|---|---|
| T24 | Hint persists across batch iterations | `defineParameterType` once + loop 1000× `setString` + `addBatch` | Same TypeInfo for all rows | TypeInfo = `varchar(N)` for all 1000 rows |
| T25 | Hint persists, values vary in length | Hint=50, values from 1–50 chars | `varchar(50)` throughout | No re-prepare triggered, TypeInfo stable |
| T26 | Hint too small for one row in batch | Hint=5, row 500 has 20-char value | SQL Server truncation error on that row | Driver declares `varchar(5)` throughout — user must set hint to the true maximum |
| T27 | No hint, batch baseline | No `defineParameterType` + batch | `varchar(8000)` | Default unchanged |

### 9.6 Interaction with existing features

| # | Scenario | Input | Expected behaviour | Pass condition |
|---|---|---|---|---|
| T28 | `sendStringParametersAsUnicode=true` | `defineParameterType(1, VARCHAR, 50)` + `setString("hi")` with SSPAУ=true | `nvarchar(50)` (type promoted to N-type) | Hint is preserved after Unicode promotion |
| T29 | AE enabled, encrypted column | `defineParameterType` on an AE-encrypted VARCHAR param | AE path is used, hint ignored | AE behaviour unchanged — no regression |
| T30 | AE enabled, non-encrypted column | `defineParameterType(1, VARCHAR, 50)` on a non-encrypted param in AE-enabled connection | `varchar(50)` | Hint applies for non-encrypted params |
| T31 | Re-prepare after `clearParameters()` | `defineParameterType`, execute, `clearParameters()`, execute again | Hint preserved after `clearParameters()` | Hint survives parameter value clear |
| T32 | Re-prepare after `close()` + new PS | `defineParameterType` on PS1, close PS1, create PS2, no `defineParameterType` | PS2 uses default `varchar(8000)` | State is not shared between statement objects |
| T33 | Multiple parameters, partial hint | `defineParameterType` on param 1 only; param 2 uses `setString` without hint | Param 1 = `varchar(N)`, Param 2 = `varchar(8000)` | Hints are per-parameter |

### 9.7 `useBulkCopyForBatchInsert` non-interaction

| # | Scenario | Expected behaviour | Pass condition |
|---|---|---|---|
| T34 | `useBulkCopyForBatchInsert=true` + `defineParameterType` called | Bulk Copy path uses server table TypeInfo, hint has no effect | No error, data inserted correctly, Bulk Copy path still used |
| T35 | `useBulkCopyForBatchInsert=true`, no `defineParameterType` | Existing Bulk Copy behaviour unchanged | No regression |
