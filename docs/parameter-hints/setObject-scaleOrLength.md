# `setObject(..., scaleOrLength)` — Parameter Length Hint via Setter

## Overview

The `setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)` JDBC method accepts a `scaleOrLength` parameter. Although the JDBC 4.3 specification states this value is ignored for most types, mssql-jdbc extends this behavior for bounded variable-length types (VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY) to provide an optional length hint on a per-call basis.

**The hint is advisory, not a constraint.** It is never enforced. If the actual value is longer
than `scaleOrLength`, the driver widens the declared parameter length to the actual value length
rather than failing or truncating. A zero or negative `scaleOrLength` is treated the same as
omitting the hint. Applications that pass arbitrary or undersized `scaleOrLength` values for
string and binary types therefore behave exactly as they did before length hints were introduced —
the only cost of an inaccurate hint is a possible statement re-prepare.

This differs from `defineParameterType()`, which is an explicit declaration and **is** enforced as
a hard maximum.

## See Also

This document focuses specifically on the `setObject(..., scaleOrLength)` behavior. For a broader overview of parameter length hints and the precedence rule between this API and `defineParameterType()`, see **[README.md](README.md)**.

## API

```java
/**
 * Sets a parameter to the given object value, with optional length hint for
 * variable-length types (VARCHAR, CHAR, NVARCHAR, NCHAR, VARBINARY, BINARY).
 *
 * @param parameterIndex the first parameter is 1, the second is 2, ...
 * @param x the object containing the input parameter value
 * @param targetSqlType a java.sql.Types constant indicating the SQL type
 * @param scaleOrLength for most types this parameter is ignored; for supported
 *                      variable-length types (VARCHAR, CHAR, NVARCHAR, NCHAR,
 *                      VARBINARY, BINARY), this is treated as an advisory length
 *                      hint, in bytes for VARCHAR/CHAR/VARBINARY/BINARY and in
 *                      characters for NVARCHAR/NCHAR.
 *                      When set, mssql-jdbc declares the parameter in TDS using
 *                      this length instead of the default maximum. If a value
 *                      exceeds this length, the declared length is widened to the
 *                      actual value length; the value is never truncated and no
 *                      error is raised.
 * @throws SQLException if an error occurs
 */
public void setObject(int parameterIndex, Object x, int targetSqlType, int scaleOrLength)
        throws SQLException;
```

### CallableStatement named-parameter variants

For `SQLServerCallableStatement`, equivalent length-hint behavior is supported on:

- `setObject(String parameterName, Object value, int sqlType, int scale)`
- `setObject(String parameterName, Object value, int sqlType, int scale, boolean forceEncrypt)`

The overload with both precision and scale (`setObject(String, Object, int, Integer, int)`) keeps
its existing precision/scale semantics and does not use `scale` as a string/binary length hint.

## Usage Examples

### Single execution with length hint

```java
String name = "Engineering";
int maxNameLength = 100;

ps.setObject(1, name, Types.VARCHAR, maxNameLength);
ps.executeUpdate();
// Wire sends: varchar(100) instead of varchar(8000)
// Data: "Engineering" (fits within 100 chars)
```

### Multiple calls with same hint

```java
List<String> codes = Arrays.asList("A001", "B002", "C003");
int maxCodeLength = 10;

for (String code : codes) {
    ps.setObject(1, code, Types.NVARCHAR, maxCodeLength);
    ps.executeUpdate();
    // Each execution declares: nvarchar(10)
}
```

### Binary data with length hint

```java
byte[] avatar = readAvatarBytes();
int maxAvatarSize = 64;

ps.setObject(1, avatar, Types.VARBINARY, maxAvatarSize);
ps.executeUpdate();
// Wire sends: varbinary(64) instead of varbinary(8000)
```

## Supported Types

The same 6 bounded variable-length types are supported:

| JDBC Type | Default TypeInfo without hint | TypeInfo with hint | Notes |
|---|---|---|---|
| `Types.VARCHAR` | `varchar(8000)` or `nvarchar(4000)` | `varchar(N)` or `nvarchar(N)` | Affected by `sendStringParametersAsUnicode` |
| `Types.CHAR` | `varchar(8000)` or `nvarchar(4000)` | `varchar(N)` or `nvarchar(N)` | Maps to VARCHAR on wire |
| `Types.NVARCHAR` | `nvarchar(4000)` | `nvarchar(N)` | Always Unicode |
| `Types.NCHAR` | `nvarchar(4000)` | `nvarchar(N)` | Maps to NVARCHAR on wire |
| `Types.VARBINARY` | `varbinary(8000)` | `varbinary(N)` | Binary equivalent |
| `Types.BINARY` | `varbinary(8000)` | `varbinary(N)` | Maps to VARBINARY on wire |

For all other types the `scaleOrLength` parameter is not treated as a length hint. Note that it retains its
pre-existing JDBC meaning where applicable: it is still the scale for `DECIMAL`/`NUMERIC` and for the temporal
types (`TIMESTAMP`, `TIME`, `DATETIMEOFFSET`), and the stream length for `InputStream`/`Reader` values. This
applies equally to the ordinal `SQLServerPreparedStatement.setObject(int, ...)` overloads and the named-parameter
`SQLServerCallableStatement.setObject(String, ...)` overloads.

## Behavior and Semantics

### Hint Application

When `setObject()` is called with `scaleOrLength > 0` for a supported type:

1. The driver uses this value as an advisory length hint
2. The TDS TypeInfo for the parameter is declared using this hint instead of the default maximum
3. The actual value is sent to the server as-is (no client-side truncation)
4. If the actual value is longer than the hint, the declared length is widened to the actual value
   length before the TDS message is built, so the value always fits

### Widening When the Value Does Not Fit

An undersized hint is corrected automatically:

```java
ps.setObject(1, "this_is_a_very_long_value", Types.VARCHAR, 5);
ps.executeUpdate();
// Hint of 5 is too small for a 25-byte value.
// Wire sends: varchar(25) — the full value is stored, no error, no truncation.
```

The driver emits a `FINER` log record on the
`com.microsoft.sqlserver.jdbc.internals.Parameter` logger when it widens a hint, which is useful
for finding hints that are consistently too small.

Because widening changes the parameter's type definition, it may cause the statement to be
re-prepared on the server. To avoid that cost, size the hint for the largest value you expect.

For VARCHAR and CHAR the comparison is done on the **encoded byte length** using the database
collation's charset, not the Java character count, because `varchar(n)` counts bytes on the
server. NVARCHAR and NCHAR are compared in characters, and binary types in bytes.

### Per-Call Application

Unlike `defineParameterType()`, the `scaleOrLength` hint is applied only to the specific `setObject()` call. Each subsequent `setObject()` call requires its own hint if desired:

```java
ps.setObject(1, "short", Types.VARCHAR, 20);
ps.executeUpdate();
// Wire: varchar(20)

ps.setObject(1, "longer_value", Types.VARCHAR, 50);
ps.executeUpdate();
// Wire: varchar(50)  — hint changed per-call
```

### Interaction with sendStringParametersAsUnicode

When `sendStringParametersAsUnicode` is `true` (default), VARCHAR and CHAR hints are promoted to NVARCHAR on the wire:

```java
ps.setObject(1, "test", Types.VARCHAR, 100);
// With SSPAU=true (default): wire sends nvarchar(100)
// With SSPAU=false: wire sends varchar(100)
```

NVARCHAR and NCHAR hints always produce NVARCHAR on the wire, regardless of this setting.

### Length Boundary Rules

The same boundary thresholds apply as with `defineParameterType()`:

```
VARCHAR / CHAR    : N ≤ 8000  → varchar(N)    N > 8000  → varchar(max)
NVARCHAR / NCHAR  : N ≤ 4000  → nvarchar(N)   N > 4000  → nvarchar(max)
VARBINARY / BINARY: N ≤ 8000  → varbinary(N)  N > 8000  → varbinary(max)
```

With `sendStringParametersAsUnicode=true`:
- VARCHAR/CHAR hints > 4000 → `nvarchar(max)` (due to NVARCHAR path with 4000-char limit)

## Precedence: defineParameterType Takes Priority

When both APIs are used on the same parameter, **`defineParameterType()` takes precedence**:

```java
ps.defineParameterType(1, Types.VARCHAR, 50);  // Primary hint: VARCHAR(50)
ps.setObject(1, "hello", Types.VARCHAR, 100);  // Fallback hint: VARCHAR(100) — IGNORED
ps.executeUpdate();
// Wire sends: varchar(50)  (not varchar(100))
```

### Why This Precedence?

- `defineParameterType()` represents an **explicit upfront contract** about the parameter's type and size
- It persists across all calls (batch rows, multiple executions) and provides a clear, single source of truth
- `setObject(..., scaleOrLength)` is an **inline, per-call hint** that is less discoverable
- The precedence ensures batch consistency: all rows in a batch use the same type declaration

For more details, see **[README.md](README.md#precedence-rule-defineparametertype-takes-priority)**.

## Error Handling

### Non-Positive Length

Passing a non-positive `scaleOrLength` (`<= 0`) for a supported bounded variable-length type is
not an error. It is not a usable declared length, so the hint is ignored entirely and the driver
falls back to its default sizing (`varchar(8000)` / `nvarchar(4000)` / `varbinary(8000)`) — the
same behavior as calling `setObject()` without a length.

(Note: for `defineParameterType()`, non-positive values *are* rejected eagerly at call time with
`R_invalidParameterLength`.)

### Value Exceeds the Hint

This is not an error. The declared length is widened to the actual value length — see
[Widening When the Value Does Not Fit](#widening-when-the-value-does-not-fit).

### Invalid Type for Hint

If `targetSqlType` is not one of the 6 supported types, the `scaleOrLength` is silently ignored:

```java
ps.setObject(1, 42, Types.INTEGER, 100);
// scaleOrLength=100 is IGNORED (INTEGER doesn't support length hints)
// Wire sends: int (no length)
```

## Batch Behavior

When used in a batch, the hint is applied to each `setObject()` call independently:

```java
ps.setObject(1, "row1_data", Types.VARCHAR, 50);
ps.addBatch();

ps.setObject(1, "row2_data", Types.VARCHAR, 100);
ps.addBatch();

int[] counts = ps.executeBatch();
// First row: varchar(50)
// Second row: varchar(100)
```

**Note:** If `defineParameterType()` is called before the batch, it overrides the `setObject()` hints:

```java
ps.defineParameterType(1, Types.VARCHAR, 50);

ps.setObject(1, "row1", Types.VARCHAR, 100);  // hint=100 ignored
ps.addBatch();

ps.setObject(1, "row2", Types.VARCHAR, 100);  // hint=100 ignored
ps.addBatch();

int[] counts = ps.executeBatch();
// Both rows: varchar(50)  (from defineParameterType)
```

## Comparison with defineParameterType()

| Aspect | `setObject(..., scaleOrLength)` | `defineParameterType()` |
|---|---|---|
| **Scope** | Per-call | Persists across calls |
| **Enforcement** | Advisory — widened to fit if too small | Enforced — value longer than `maxLength` is an error |
| **Non-positive length** | Ignored, driver default sizing is used | Rejected with `R_invalidParameterLength` |
| **Batch behavior** | Hint varies per row | Hint is consistent across all rows |
| **Discoverability** | Less obvious (standard JDBC parameter) | More explicit (dedicated method) |
| **Best for** | Single executions, non-batch scenarios | Batch inserts, consistent type contracts |
| **Precedence** | Fallback (when defineParameterType not called) | Primary (takes priority if set) |

## Implementation Details

### Internal Processing

The `scaleOrLength` parameter is extracted by `SQLServerPreparedStatement.setObject()` and passed as an integer to the internal parameter setting logic. When the TDS message is prepared, the type definition is computed by checking:

1. **Is `defineParameterType()` called?** If yes, use that hint (takes precedence) and enforce it
    as a maximum
2. **Is `scaleOrLength` a positive value for a supported type?** If yes, use it as an advisory
    hint: honor it when the value fits, and widen it to the actual value length when it does not.
    Drop it when the actual length cannot be measured (for example a non-`String`/non-`byte[]`
    value). A non-positive `scaleOrLength` is ignored outright.
3. Otherwise, use the default TypeInfo (varchar(8000) / nvarchar(4000) / varbinary(8000))

### Interaction with Always Encrypted (AE)

When Always Encrypted is enabled on a parameter, the `scaleOrLength` hint is ignored because AE requires exact type information derived from the actual value to compute the correct encrypted representation.

### No Effect on useBulkCopyForBatchInsert

When `useBulkCopyForBatchInsert=true` is set on the connection, `executeBatch()` internally uses the Bulk Copy protocol, which derives column types from the server's destination table schema — not from the parameter's type definition. Therefore, `setObject(..., scaleOrLength)` has no effect in this mode.

## Test Coverage

See **[ParameterLengthHintTest.java](../../../../src/test/java/com/microsoft/sqlserver/jdbc/preparedStatement/ParameterLengthHintTest.java)** (formerly `DefineParameterTypeTest.java`) for prepared-statement coverage, including:

- **SetObjectLengthHintTests**: Verifies `setObject()` scaleOrLength honored for all 6 supported types
- **DefinePrecedesSetObjectTests**: Verifies `defineParameterType()` takes precedence over `setObject()` hints
- **ErrorHandlingTests**: Verifies `defineParameterType()` value-exceeds-hint errors, that
  undersized `setObject()` hints widen instead of failing, and that non-positive `setObject()`
  hints fall back to the driver's default sizing
- **BatchTests**: Verifies batch behavior with both APIs and precedence

See **[CallableParameterLengthHintTest.java](../../../../src/test/java/com/microsoft/sqlserver/jdbc/callablestatement/CallableParameterLengthHintTest.java)** for callable-statement coverage, including:

- Named-parameter `setObject` scale-only overload behavior across all 6 supported types
- Precedence (`defineParameterType` over callable `setObject` length hints) for both non-forceEncrypt and forceEncrypt overloads
- `sendStringParametersAsUnicode` true/false declaration behavior
- Boundary, NULL/empty, and value-exceeds-hint widening scenarios
- Guard test for callable precision+scale overload semantics

## Related Classes and Methods

- `SQLServerPreparedStatement.setObject(int, Object, int, int)`
- `ISQLServerPreparedStatement.defineParameterType(int, int, int)` — the primary API (takes precedence)
- `Parameter.getApplicationSpecifiedLengthHint()` — internal method that implements precedence logic
- `SQLServerCallableStatement.setObject(String, Object, int, int)`
- `SQLServerCallableStatement.setObject(String, Object, int, int, boolean)`
