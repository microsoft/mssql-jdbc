# Design: `useColumnTypeSizing` — column-aware sizing of variable-length parameters

Tracking: issue [#2913], relates to PR [#2960].

## Problem

For an unsized variable-length string/binary parameter (`setString`/`setNString`/`setBytes`), the driver
declares a fixed maximum bucket to the server regardless of the value or the target column:

| Runtime arm | Declared default |
|-------------|------------------|
| `case CHAR/VARCHAR` (non-AE) | `varchar(8000)` (`Parameter.GetTypeDefinitionOp`, varchar arm) |
| `case NCHAR/NVARCHAR` (non-AE) | `nvarchar(4000)` (nvarchar arm) |
| `case BINARY/VARBINARY` (non-AE) | `varbinary(8000)` (varbinary arm) |

This causes two independent production problems:

- **Symptom A — memory-grant over-allocation.** SQL Server sizes a query's memory grant from the *declared*
  width (~half per row). An 8000-wide declaration reserves memory as if every row were ~4 KB. Reproduced:
  a sorted `notes + @p` expression went from **1,108,256 KB** desired grant with `varchar(8000)` to
  **70,296 KB** with `varchar(8)` (~15.8×).
- **Symptom B — lost index seek / partition elimination.** Under the default `sendStringParametersAsUnicode=true`
  a parameter is declared `nvarchar`. Comparing `nvarchar` against a `varchar`/`char` column converts the
  *column* (type precedence), making the predicate non-SARGable. Reproduced on a `varchar(8)`-partitioned table:
  `nvarchar(4000)` → Clustered Index **Scan**, 13 partitions, 715 reads; `varchar(8)` → **Seek**, 1 partition,
  29 reads. **Tightening only the length does not fix this** (`nvarchar(8)` still scans) — the *type* must be
  `varchar`, which the application selects with `sendStringParametersAsUnicode=false`.

`varchar`/`char` columns suffer **both** symptoms; `nvarchar`/`nchar` columns suffer **only** Symptom A
(an `nvarchar` parameter already matches a unicode column, so the seek is not lost).

## Behavior

Opt-in boolean connection property `useColumnTypeSizing` (default `false`).

- **OFF**: behavior is byte-for-byte identical to today.
- **ON**: for an unsized, non-output `setString`/`setNString`/`setBytes` parameter whose target column the
  server resolves to a variable-length char/binary type `T(N)`, the driver declares
  `<runtime-arm-type>(N')` instead of the coarse default, where:
  - the **type keyword is the runtime arm's native type** (`varchar`/`nvarchar`/`varbinary`) — never
    overridden — so the TDS value encoding is unchanged and results cannot change;
  - **N' is the inferred column length**, clamped: if the bound value (measured in the declared unit — bytes
    for `varchar`/`varbinary`, UTF-16 units for `nvarchar`) is longer than the column, the declaration **snaps
    to `(max)`** so the operand is never truncated;
  - `Math.max(N,1)` is enforced (no `varchar(0)`); LOB/`(max)` columns yield the `(max)` variant.

Because the declaration is one of at most two value-independent strings (`T(N)` or `T(max)`), the prepared-
statement / plan-cache key `CityHash128Key(preparedSQL, preparedTypeDefinitions)` stays bounded — no
per-value fragmentation.

## Mechanism

1. **Inference (once per statement).** `SQLServerPreparedStatement.inferColumnTypeSizes(params)` runs before the
   parameter type definitions are built (the same pre-request window the Always Encrypted metadata fetch uses),
   guarded by: property on, not already attempted, not an internal query, not an AE connection, not FMTONLY,
   server ≥ 2012, and at least one eligible parameter. It calls `getParameterMetaData()`
   (→ `sp_describe_undeclared_parameters`) once, records the per-ordinal column length in
   `inferredColumnLengthByOrdinal`, and applies it to each `Parameter` via `setInferredColumnLength`. Any failure
   falls back to the legacy default and is never propagated. The same routine is invoked from the batch path.
2. **Recursion guard.** The inner `sp_describe_undeclared_parameters` statement is flagged
   `SQLServerStatement.isInternalQuery = true`, and the inference routine skips internal queries.
3. **Value capture.** The bound value reference is captured in `Parameter` at bind time (`setValue`, gated on the
   property) — not at build time — so the snap-to-max clamp can measure it reliably even after the metadata
   round-trip. Propagated in `cloneForBatch`.
4. **Consumption.** A new `else if (useInferredColumnSize())` rung in each of the three non-AE arms of
   `Parameter.GetTypeDefinitionOp.setTypeDefinition` emits the sized type via `buildColumnSizedTypeDefinition`.
   Precedence is **AE > (`#2960` explicit hint) > column inference > legacy default**. NOTE: PR #2960
   (`defineParameterType`) is **not** in this tree, so the explicit-hint rung does not exist here; the inference
   rung sits directly after the AE branch. If this work is rebased onto #2960, the inference rung MUST be inserted
   *after* the `defineParameterTypeCalled` rung so an explicit caller-supplied length always wins over inference.

## Safety / invariants

- **Backward compatible** — default off; runtime type unchanged ⇒ identical encoding.
- **No truncation** — clamp snaps to `(max)` when the value exceeds the column; ranges return the wide
  `varchar(8000)` from the server and are left wide.
- **Bounded plan cache** — declaration is value-independent (≤ 2 forms per parameter).
- **No unconditional hot-path round-trip** — describe runs at most once per statement object, independent of
  statement pooling; clean fallback on failure.
- **Always Encrypted preserved** — inference is not even attempted on AE connections; the AE arm remains first.
- **`sendStringParametersAsUnicode` honored** — the feature stays in whatever arm SSPAU selected.

## Tests

`com.microsoft.sqlserver.jdbc.preparedStatement.ColumnTypeSizingTest` covers: no-regression (off);
varchar/char/nchar/nvarchar/varbinary/LOB columns; SSPAU true vs false; the snap-to-max clamp and exact-length
boundary; range predicates (no truncation); non-string-column fallback; explicit-column INSERT; batch INSERT
persistence; declaration stability across values; and NULL values.
