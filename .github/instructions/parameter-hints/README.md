# Parameter Length Hints for Variable-Length Types

## Overview

This document describes the parameter length hint mechanisms available in mssql-jdbc for optimizing memory grants in SQL Server execution plans when working with variable-length types (VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY).

## The Problem

By default, when a Java application calls `setString()`, `setNString()`, or `setBytes()` on a `PreparedStatement`, mssql-jdbc declares the parameter in TDS using the maximum bounded width regardless of the actual value size:

| Setter | Default TDS TypeInfo | Server memory grant (10k-row batch) |
|---|---|---|
| `setString("hello")` | `varchar(8000)` or `nvarchar(4000)` | ~38–76 MB |
| `setNString("hello")` | `nvarchar(4000)` | ~38 MB |
| `setBytes(new byte[20])` | `varbinary(8000)` | ~76 MB |

SQL Server uses the declared TypeInfo width to compute **memory grants** for query execution plans. When a parameter is declared `varchar(8000)` but the actual data is only 50 characters, the server over-allocates memory — compounding across batch sizes and concurrent sessions.

## Solutions: Two APIs, One Precedence Rule

### 1. `defineParameterType()` — The Primary API

Establishes a max-length hint once, before a batch loop:

```java
ps.defineParameterType(1, Types.VARCHAR, 100);
for (User u : users) {
    ps.setString(1, u.getName());
    ps.addBatch();
}
ps.executeBatch();
// Wire sends: varchar(100) instead of varchar(8000)
```

**Advantages:**
- Hint persists across all `addBatch()` and `setString()` calls
- Set once per statement, simplifying batch workflows
- More explicit and discoverable (explicit method call)

**Reference:** See [define-parameter-type.md](define-parameter-type.md) for full details.

### 2. `setObject(..., scaleOrLength)` — The Fallback API

Provides a length hint on a per-call basis for each parameter:

```java
ps.setObject(1, "hello", Types.VARCHAR, 100);
ps.setObject(1, "world", Types.VARCHAR, 100);
ps.executeUpdate();
// Wire sends: varchar(100) instead of varchar(8000)
```

**Advantages:**
- Granular control (hint can vary per `setObject` call)
- Works with non-batch execution
- JDBC-standard parameter (though its behavior is driver-specific)

**Limitations:**
- Must be supplied on every `setObject()` call
- Less discoverable (buried in a standard JDBC parameter)

**Reference:** See [setObject-scaleOrLength.md](setObject-scaleOrLength.md) for full details.

### CallableStatement support

The same behavior is available on `SQLServerCallableStatement` for named parameters:

- `setObject(String parameterName, Object value, int sqlType, int scale)`
- `setObject(String parameterName, Object value, int sqlType, int scale, boolean forceEncrypt)`

For callable statements, length-hint semantics are intentionally applied only to the overloads where
`scale` is the only numeric hint argument. The overload with both precision and scale
(`setObject(String, Object, int, Integer, int)`) keeps its existing precision/scale semantics.

## Precedence Rule: defineParameterType Takes Priority

When **both APIs are used on the same parameter**, `defineParameterType()` takes precedence:

```java
ps.defineParameterType(1, Types.VARCHAR, 50);  // Primary hint
ps.setObject(1, "hello", Types.VARCHAR, 100);  // Fallback hint (ignored)
ps.executeUpdate();
// Wire sends: varchar(50)  (from defineParameterType, not setObject)
```

### Why This Precedence?

- **Explicit setup vs. inline hints**: `defineParameterType()` represents an explicit, upfront contract about the parameter's expected data range. It signals intent to all callers.
- **Batch consistency**: In batch workflows, `defineParameterType()` ensures all rows use the same type declaration. A `setObject()` hint that differs would break that contract.
- **Single point of configuration**: The precedence enforces a single, clear source of truth for the parameter's type definition, avoiding confusion when hints conflict.

## Supported Data Types

Both APIs support the same **bounded variable-length types**:

| JDBC Type | Notes |
|---|---|
| `Types.VARCHAR` | Maps to `varchar(N)` or `nvarchar(N)` on wire (depends on `sendStringParametersAsUnicode`) |
| `Types.CHAR` | Maps to `varchar(N)` or `nvarchar(N)` on wire |
| `Types.NVARCHAR` | Maps to `nvarchar(N)` on wire |
| `Types.NCHAR` | Maps to `nvarchar(N)` on wire |
| `Types.VARBINARY` | Maps to `varbinary(N)` on wire |
| `Types.BINARY` | Maps to `varbinary(N)` on wire |

### Explicitly Excluded Types

**CLOB, NCLOB, LONGVARCHAR, LONGNVARCHAR, BLOB, LONGVARBINARY** — These are inherently unbounded and use PLP (Partially Long Pointer) streaming in TDS, where memory is allocated lazily as chunks arrive. There is no fixed memory grant problem to solve, and length hints are not applicable.

**DECIMAL, NUMERIC** — Already handled separately via `calcBigDecimalPrecision=true` connection property, which computes exact precision from the actual `BigDecimal` value.

**Fixed-width types (INT, BIGINT, FLOAT, etc.)** — These types have no width field in their TDS TypeInfo.

## Length Boundary Rules

Both APIs enforce the same boundaries:

```
VARCHAR / CHAR    : N ≤ 8000  → varchar(N)    N > 8000  → varchar(max)
NVARCHAR / NCHAR  : N ≤ 4000  → nvarchar(N)   N > 4000  → nvarchar(max)
VARBINARY / BINARY: N ≤ 8000  → varbinary(N)  N > 8000  → varbinary(max)
```

When SSPAU (`sendStringParametersAsUnicode`) is `true` (default), VARCHAR/CHAR hints > 4000 characters promote to `nvarchar(max)` because they route through the NVARCHAR path.

## Error Handling

Both APIs validate constraints and reject violations:

- **Negative length**: Rejected with error `R_invalidParameterLength`
- **Unsupported JDBC type**: Rejected with error `R_unsupportedTypeForDefineParamType`
- **Value exceeds declared length**: Execution fails with error `R_defineParameterTypeValueLengthExceedsHint` (prevents silent data corruption)

## Batch Workflows

Both APIs work correctly in batch workflows:

```java
ps.defineParameterType(1, Types.VARCHAR, 100);
ps.defineParameterType(2, Types.NVARCHAR, 50);

for (User u : users) {
    ps.setString(1, u.getName());
    ps.setNString(2, u.getCode());
    ps.addBatch();
}

int[] counts = ps.executeBatch();
// All rows declared with the hints: varchar(100), nvarchar(50)
```

## When to Use Which?

| Scenario | Recommended |
|---|---|
| Batch insert / update of many rows with same type contract | `defineParameterType()` |
| Single execution with optional length hint | `setObject(..., scaleOrLength)` |
| Mixed single and batch operations on the same prepared statement | `defineParameterType()` (takes precedence) |
| Non-batch prepared statements | Either (but `setObject()` is simpler for single calls) |

## Documentation

- **[define-parameter-type.md](define-parameter-type.md)** — Detailed reference for `defineParameterType(int, int, int)` API
- **[setObject-scaleOrLength.md](setObject-scaleOrLength.md)** — Detailed reference for `setObject(..., scaleOrLength)` parameter behavior

## Test Coverage

See:

- **[ParameterLengthHintTest.java](../../../../src/test/java/com/microsoft/sqlserver/jdbc/preparedStatement/ParameterLengthHintTest.java)** for prepared-statement coverage (single execution, batch operations, precedence validation, and error handling)
- **[CallableParameterLengthHintTest.java](../../../../src/test/java/com/microsoft/sqlserver/jdbc/callablestatement/CallableParameterLengthHintTest.java)** for callable-statement coverage (named-parameter overloads, precedence, SSPAU true/false behavior, boundary/null/empty scenarios, and error handling)

## Related Methods

- `SQLServerPreparedStatement.defineParameterType(int parameterIndex, int sqlType, int maxLength)`
- `SQLServerPreparedStatement.setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)`
- `SQLServerCallableStatement.setObject(String parameterName, Object value, int sqlType, int scale)`
- `SQLServerCallableStatement.setObject(String parameterName, Object value, int sqlType, int scale, boolean forceEncrypt)`
