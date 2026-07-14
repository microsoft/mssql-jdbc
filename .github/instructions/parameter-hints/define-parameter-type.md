# `defineParameterType()` — Primary Parameter Length Hint API

## Quick Reference

**Related:** See **[README.md](README.md)** for an overview of parameter length hint mechanisms, precedence rules, and when to use which API.

**Precedence:** `defineParameterType()` takes priority over `setObject(..., scaleOrLength)` when both are used on the same parameter. See [Relationship to setObject](#relationship-to-setobject).

## Table of Contents

1. [Relationship to setObject](#relationship-to-setobject)
2. [Background: The Issue](#background-the-issue)
3. [How Oracle Solves This](#how-oracle-solves-this)
4. [Why SQL Server / TDS Behaves Differently](#why-sql-server--tds-behaves-differently)
5. [API Specification](#api-specification)
6. [Affected Data Types](#affected-data-types)
7. [Types Explicitly Excluded](#types-explicitly-excluded)
8. [Implementation Details](#implementation-details)
9. [Interaction with Bulk Copy API](#interaction-with-bulk-copy-api)
10. [Test Coverage](#test-coverage)

---

## Relationship to setObject

mssql-jdbc provides two APIs for parameter length hints:

### `defineParameterType(int parameterIndex, int sqlType, int maxLength)`

- **Primary API** — Set once before a batch loop
- Hint persists across all `addBatch()` and `setString()` / `setNString()` / `setBytes()` calls
- More discoverable (explicit method name)
- Returns when using `defineParameterType()`, all parameter hints are not overridden

### `setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)`

- **Fallback API** — Applied per-call
- Hint is provided inline with the value
- Less discoverable (standard JDBC parameter)
- Must be supplied on every `setObject()` call

### Precedence Rule

**If both APIs are used on the same parameter, `defineParameterType()` takes priority:**

```java
ps.defineParameterType(1, Types.VARCHAR, 50);
ps.setObject(1, "value", Types.VARCHAR, 100);
ps.executeUpdate();
// Wire sends: varchar(50)  ← from defineParameterType, not setObject
```

This ensures `defineParameterType()` acts as an explicit type contract that cannot be accidentally overridden by inline `setObject()` hints.

For more details on when to use which API, see **[README.md](README.md#when-to-use-which)**.

---

## Background: The Issue

**GitHub Issue:** [#2913](https://github.com/microsoft/mssql-jdbc/issues/2913) — String parameters always sent as `varchar(8000)` / `nvarchar(4000)`.

### Root Cause

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

## How Oracle Solves This

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

## Why SQL Server / TDS Behaves Differently

### TDS wire format comparison

```
varchar(N)   : [0xA7 BIGVARCHR] [maxLen=N : 2B] [collation: 5B] [valueLen: 2B] [bytes]
               └─ server uses N for memory grant in query plan

varchar(max) : [0xA5 BIGVARCHAR] [maxLen=0xFFFF MAX marker] [collation: 5B]
               [PLP chunks: len+data, ...]  [0x00000000 terminator]
               └─ PLP streaming: no upfront fixed allocation
```

### SQL Server enforces the declared TypeInfo length

SQL Server treats the declared TypeInfo width as the actual type constraint, not just
a memory hint. In the current implementation, `defineParameterType` narrows the
parameter type definition used for preparation, but the value-writing path still sends
the full value length on the wire. SQL Server then applies the semantics of the
declared parameter type, which can include truncation or conversion when the value
exceeds the declared width.

In practice, this means:
- **String types** (VARCHAR, CHAR, NVARCHAR, NCHAR): SQL Server may truncate or otherwise
  convert the value according to the declared type and length
- **Binary types** (VARBINARY, BINARY): SQL Server may truncate the value according to
  the declared type and length

This is intentional. The hint is called once before a batch loop; the caller is asserting
that their data will fit within `maxLength` for the declared type. If a value is longer,
the outcome is determined by SQL Server based on that declared type definition rather
than by client-side pre-truncation. This differs from Oracle-style `defineParameterType`
semantics where the client can avoid sending excess data.

For supported bounded variable-length hint paths, non-positive hints (`<= 0`) are rejected
with `R_invalidParameterLength` during parameter type resolution.

### Interaction with `sendStringParametersAsUnicode`

The connection property `sendStringParametersAsUnicode` (SSPAU, default `true`) controls
whether `setString()` sends data as NVARCHAR or VARCHAR on the TDS wire. This property
is **not overridden** by `defineParameterType`:

| SSPAU | Setter | `defineParameterType` sqlType | Wire type |
|-------|--------|-------------------------------|-----------|
| `true` (default) | `setString("hi")` | `VARCHAR` or `CHAR` | `nvarchar(N)` |
| `true` (default) | `setNString("hi")` | `NVARCHAR` or `NCHAR` | `nvarchar(N)` |
| `false` | `setString("hi")` | `VARCHAR` or `CHAR` | `varchar(N)` |
| `false` | `setNString("hi")` | `NVARCHAR` or `NCHAR` | `nvarchar(N)` |

Key insight: when SSPAU=true, calling `defineParameterType(1, Types.VARCHAR, 50)` followed
by `setString("hi")` produces `nvarchar(50)` on the wire — not `varchar(50)`. The hint
length is preserved, but the type is promoted to the Unicode variant by SSPAU. The
`setNString()` method always uses NVARCHAR regardless of SSPAU.

The boundary thresholds also shift with SSPAU:
- SSPAU=true: VARCHAR/CHAR hints > 4000 → `nvarchar(max)` (hint passes through NVARCHAR path with 4000-char limit)
- SSPAU=false: VARCHAR/CHAR hints > 8000 → `varchar(max)`
- NVARCHAR/NCHAR hints > 4000 → `nvarchar(max)` (regardless of SSPAU)
- VARBINARY/BINARY hints > 8000 → `varbinary(max)` (SSPAU has no effect on binary)

---

## API Specification

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
* this prepared statement. If a value exceeds the declared length, execution
* fails with a validation error (no silent truncation).</p>
 *
 * <p>Supported target types: {@link java.sql.Types#VARCHAR},
 * {@link java.sql.Types#CHAR}, {@link java.sql.Types#NVARCHAR},
 * {@link java.sql.Types#NCHAR}, {@link java.sql.Types#VARBINARY},
 * {@link java.sql.Types#BINARY}.</p>
 *
 * @param parameterIndex 1-based parameter index
 * @param sqlType        {@code java.sql.Types} constant; used only for validation (must be
 *                       VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, or BINARY). The actual
 *                       wire type is determined by the setter method, not by this value.
 * @param maxLength      maximum expected length in characters (VARCHAR/NVARCHAR/CHAR/NCHAR)
 *                       or bytes (VARBINARY/BINARY); must be > 0 for the supported hint path
 * @throws SQLServerException if the parameter index is out of range, the sqlType is not a
 *                            supported variable-length type, or execution later resolves a
 *                            non-positive hint for a supported bounded variable-length parameter
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

## Affected Data Types

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

## Types Explicitly Excluded

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

## Implementation Details

### 8.1 `Parameter.java` — new flag

A single new boolean field is added to `Parameter`:

```java
// Set to true when defineParameterType() has been called for this parameter.
// When true, valueLength holds the caller-supplied max-length hint and the type
// definition (e.g. varchar(N)) is built from that hint rather than the conservative
// driver default (e.g. varchar(8000) / nvarchar(4000) / varbinary(8000)).
// Data exceeding the hint causes execution to fail with a validation error.
private boolean defineParameterTypeCalled = false;
```

`defineParameterType` calls `param.setValueLength(maxLength)` (which stores the hint in
the existing `valueLength` field and sets `userProvidesPrecision = true`) and
`param.setDefineParameterTypeCalled(true)`. The `sqlType` argument is **validated but not
stored** — `setTypeDefinition()` already routes via `dtv.getJdbcType()`, which reflects
what `setString` / `setNString` / `setBytes` etc. actually placed on the parameter.

### 8.2 `setTypeDefinition()` in `Parameter.java` — `GetTypeDefinitionOp`

All three affected type families follow the same pattern — AE check first, then the hint
branch as an `else if`:

```java
// CHAR / VARCHAR
if (param.shouldHonorAEForParameter && (null != jdbcTypeSetByUser)
        && !(null == param.getCryptoMetadata() && param.renewDefinition)) {
    // AE path — exact length from value; untouched
} else if (param.defineParameterTypeCalled) {
    // defineParameterType hint: declare the user-specified length directly.
    // Values exceeding maxLength are rejected before execution.
    int hint = param.valueLength;
    if (hint > DataTypes.SHORT_VARTYPE_MAX_BYTES) {
        param.typeDefinition = VARCHAR_MAX;
    } else {
        param.typeDefinition = SSType.VARCHAR.toString() + "(" + hint + ")";
    }
} else {
    param.typeDefinition = VARCHAR_8K;  // default — unchanged
}
```

The same `else if` pattern applies to `NCHAR/NVARCHAR` (limit: `SHORT_VARTYPE_MAX_CHARS`
= 4000) and `BINARY/VARBINARY` (limit: `SHORT_VARTYPE_MAX_BYTES` = 8000).

### 8.3 `SQLServerPreparedStatement.java`

```java
@Override
public void defineParameterType(int parameterIndex, int sqlType, int maxLength)
        throws SQLServerException {
    checkClosed();
    // Validate that sqlType is one of the supported character or binary types.
    // The sqlType value itself is not stored on the parameter — the wire type is
    // determined by what setString/setNString/setBytes sets on the DTV at execution time.
    switch (sqlType) {
        case Types.VARCHAR: case Types.CHAR:
        case Types.NVARCHAR: case Types.NCHAR:
        case Types.VARBINARY: case Types.BINARY:
            break;
        default:
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_unsupportedTypeForDefineParamType"));
            SQLServerException.makeFromDriverError(connection, this,
                    form.format(new Object[] {sqlType}), null, false);
    }
    Parameter param = setterGetParam(parameterIndex);
    param.setDefineParameterTypeCalled(true);
    param.setValueLength(maxLength); // also sets userProvidesPrecision = true
}
```

### 8.4 Error messages — `SQLServerResource.java`

```java
{"R_invalidParameterLength",
    "The maxLength value {0} is not valid. maxLength must be > 0."},
{"R_unsupportedTypeForDefineParamType",
    "The SQL type {0} is not supported by defineParameterType. "
    + "Supported types: VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY."},
```

### 8.5 `cloneForBatch()` in `Parameter.java`

The flag and hint must survive batch cloning so every row in the batch uses the same
type declaration:

```java
clonedParam.defineParameterTypeCalled = defineParameterTypeCalled;
clonedParam.valueLength              = valueLength;
clonedParam.userProvidesPrecision    = userProvidesPrecision;
```

### 8.6 Interaction with AE (Always Encrypted)

The `defineParameterTypeCalled` branch is placed in the `else if` of the AE check, so
it is mutually exclusive with the AE path:

```
if (AE active for param)  →  AE path (exact length from value; defines encrypted type)
else if (defineParameterTypeCalled)  →  hint path
else  →  conservative default (varchar(8000) etc.)

For supported bounded variable-length types, the hint is validated during parameter type
resolution in `Parameter.GetTypeDefinitionOp.getApplicationSpecifiedLengthHint(...)`. A
non-positive hint (`<= 0`) throws `R_invalidParameterLength` at execution/type-resolution
time rather than at the initial `defineParameterType()` call site.
```

No AE behaviour changes. The hint is silently ignored when AE is active, which is correct:
AE requires exact type information that must be derived from the actual value.

---

## Interaction with Bulk Copy API

### 9.1 Explicit `SQLServerBulkCopy` API — no interaction

`SQLServerBulkCopy` is a completely separate API from `PreparedStatement`.
It derives column type metadata from:
- `ResultSetMetaData` of a source `ResultSet`, or
- `SQLServerMetaData` provided by a custom `ISQLServerBulkRecord`

`defineParameterType` has no relationship to either of these.

### 9.2 `useBulkCopyForBatchInsert=true` — no interaction, already optimal

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

### 9.3 Summary

| Execution path | Type width source | Effect of `defineParameterType` |
|---|---|---|
| Normal `execute()` / `executeBatch()` | `Parameter.typeDefinition` — `varchar(8000)` problem | **Fix applies here** |
| `useBulkCopyForBatchInsert=true` | Server's destination `TypeInfo.getPrecision()` | None — already uses actual column width |
| Explicit `SQLServerBulkCopy` | Source `ResultSetMetaData` or user `SQLServerMetaData` | None — separate API entirely |

---

## Test Coverage

All tests are in `ParameterLengthHintTest.java` (formerly `DefineParameterTypeTest.java`) in `src/test/java/com/microsoft/sqlserver/jdbc/preparedStatement/`.
The test class creates a table with `VARCHAR(200)`, `NVARCHAR(200)`, and `VARBINARY(200)` columns,
then verifies the TDS type definition on the wire using reflection to invoke `Parameter.getTypeDefinition()`.

### Summary

- **String type definitions**: 48 parameterized cases (SSPAU=true and SSPAU=false)
- **Binary type definitions**: 12 parameterized cases
- **NULL/empty values**: 18 parameterized cases
- **Error handling + validation**: 22 cases
- **Batch execution**: 12 parameterized cases
- **Lifecycle/isolation**: 3 tests
- **Total**: ~115 test cases

See **[README.md](README.md)** for test reference links.

CallableStatement behavior is validated in **[CallableParameterLengthHintTest.java](../../../../src/test/java/com/microsoft/sqlserver/jdbc/callablestatement/CallableParameterLengthHintTest.java)**,
including named-parameter precedence and interaction with callable `setObject` overloads.
