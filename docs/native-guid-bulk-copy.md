# Native GUID Bulk Copy: PR #3041

This document explains the implementation in
[PR #3041](https://github.com/microsoft/mssql-jdbc/pull/3041), as of commit
`d29209bc`.

The code change modifies one production file, `SQLServerBulkCopy.java`, and adds
six test/helper files. It sends eligible GUID values using SQL Server's native
16-byte GUID format instead of sending character text for SQL Server to parse.

This describes the final, simplified implementation. The earlier source-type
storage and metadata-cache-reset approach was removed.

## 1. Before and after

For a GUID such as:

```text
00112233-4455-6677-8899-aabbccddeeff
```

| Stage | Before | Native path in this PR |
| --- | --- | --- |
| Bulk SQL declaration | `[id] CHAR(36)` | `[id] UNIQUEIDENTIFIER` |
| TDS column type | Character | GUID |
| Value payload | 36-character representation | 16-byte GUID representation |
| Text parsing | SQL Server | Driver, if the supplied value is text |
| Destination storage | `uniqueidentifier` | Same `uniqueidentifier` |

GUID inserts already worked. The PR changes how eligible values reach the
destination.

A canonical GUID has 32 hexadecimal digits and four hyphens, producing a
36-character textual representation. Its binary representation is 16 bytes.

For table-to-table copies, the driver still reads the source GUID through the
existing string getter. This PR does not implement a direct
source-binary-to-destination-binary pipeline.

## 2. Native-path selection: `isNativeGuid()`

The new boolean helper determines whether a source column should use native GUID
encoding for a particular destination column.

It rejects native encoding when:

- The destination is not `uniqueidentifier`.
- The destination has encryption metadata.
- `allowEncryptedValueModifications` is enabled.

It then checks the source:

| Source | Eligibility check |
| --- | --- |
| `SQLServerResultSet` | Inspect the current column's internal metadata. Require the actual SQL Server type to be GUID and the source to be unencrypted. |
| Other sources | Use the existing declared JDBC metadata. Require `microsoft.sql.Types.GUID`. |

This distinction matters because SQL Server's public ResultSet metadata exposes
GUID as JDBC CHAR. The internal check recognizes a genuine GUID column without
changing that public behavior.

The helper does not inspect a string's contents to infer its type. A VARCHAR
column containing GUID-looking text remains a VARCHAR source.

### What the simplified implementation does not change

- No added source `ssType` population or copying.
- No new `srcColumnMetadata = null` resets.
- No replacement of existing `bulkJdbcType` or `srcJdbcType` assignments.
- No `getSourceJdbcType()` helper.
- No changes to public ResultSet metadata or general source metadata caching.

Eligibility is evaluated per destination. The same GUID source column can
therefore map to both a GUID destination and a character destination.

## 3. Three places use the same eligibility decision

### Bulk SQL declaration: `getDestTypeFromSrcType()`

A narrow branch returns `UNIQUEIDENTIFIER` when `isNativeGuid()` is true.
Otherwise, the existing declaration logic continues.

Before returning, it verifies that declared precision is within 1 through 8000.
This preserves the previous `CHAR(n)` metadata restriction, including for
null-only records.

That range is a legacy compatibility restriction, not the size of a native GUID.

### TDS column metadata: `writeColumnMetaDataColumnData()`

After the existing encryption and binary handling, the native branch writes:

```text
GUID type token
Maximum value length: 16
```

Otherwise, it calls the existing `writeTypeInfo()`.

`writeTypeInfo()` itself retains its original implementation, including the
existing Always Encrypted base-type handling.

### Row payload: `writeColumn()`

The method determines native eligibility before reading source data, then
performs the existing read, validation, and encryption processing.

It selects the writer as follows:

```text
Native GUID selected -> writeGuidToTdsWriter()
Otherwise            -> existing writeColumnToTdsWriter()
```

`writeColumnToTdsWriter()` itself retains its original implementation.

Checking eligibility before reading is important:
`SQLServerResultSet.getColumn()` can close an active stream. Inspecting metadata
after opening a character stream could break streamed VARCHAR input.

The declaration, column metadata, and row payload must agree. Declaring character
data but sending GUID bytes would create a TDS protocol mismatch.

## 4. Value encoding: `writeGuidToTdsWriter()`

| Java value | Action |
| --- | --- |
| `null` | Write one zero byte: the native GUID null marker. |
| `UUID` | Check logical textual length 36 against precision and use the UUID directly. |
| Other non-null value, normally `String` | Obtain its text and call `parseGuid()`. |

A non-null GUID is written as:

```text
Length byte: 16
Payload:     16 GUID bytes
```

The payload comes from the existing `Util.asGuidByteArray()` helper, which handles
SQL Server's GUID byte ordering.

A Java UUID object does not need to be converted to a string and parsed again.

Invalid textual input is wrapped in the driver's `SQLServerException`, using
`R_errorConvertingValue` and retaining the parsing exception as its cause.

## 5. Parsing and shared precision validation

`parseGuid()` preserves the tested SQL Server text-conversion rules rather than
relying solely on Java's parser.

It:

1. Checks the original input length against declared precision.
2. Recognizes an optional opening brace and requires the matching closing brace immediately after the GUID.
3. Requires hyphens at the canonical positions.
4. Requires ASCII hexadecimal characters everywhere else.
5. Extracts the canonical 36-character portion.
6. Calls `UUID.fromString()`.

This allows valid braces and suffixes while rejecting malformed GUIDs that Java
might otherwise normalize.

### `UUID.fromString()` versus our parser

Let `G` mean the valid GUID `6f9619ff-8b86-d011-b42d-00c04fc964ff`.
The Java behavior below refers to JDK 21; parser leniency can vary across JDK
versions.

| Validation / input | `UUID.fromString()` | Our `parseGuid()` | Why the difference matters |
| --- | --- | --- | --- |
| Canonical GUID: `G` | Accepts | Accepts when precision >= 36 | Standard supported input |
| Uppercase or lowercase hex | Accepts both | Accepts both | No difference |
| Short groups: `1-1-1-1-1` | Accepts and zero-pads groups | Rejects | Enforces the required **8-4-4-4-12** layout |
| Exactly 36 characters, wrong groups: `06f9619ff-8b8-d011-b42d-00c04fc964ff` | Accepts despite **9-3-4-4-12** layout | Rejects | **Length checking alone is insufficient** |
| Exactly 36 characters, leading `+`: `+f9619ff-8b86-d011-b42d-00c04fc964ff` | Accepts | Rejects | Requires hexadecimal characters, not signed numeric components |
| Non-ASCII digits | Can accept some through numeric parsing | Rejects | Explicitly permits only `0-9`, `a-f`, and `A-F` |
| Invalid hex, such as `g` inside the GUID | Rejects | Rejects | Both reject invalid hexadecimal content |
| Braced GUID: `{G}` | Rejects | Accepts when precision >= 38 | Preserves supported legacy SQL Server conversion behavior |
| Trailing text: `Gxyz` | Rejects | Accepts when precision >= 39; ignores `xyz` | Preserves legacy conversion of a complete GUID followed by a suffix |
| Braced GUID with suffix: `{G}xyz` | Rejects | Accepts when precision >= 41 | Requires the closing brace immediately after the GUID, then ignores the suffix |
| Leading whitespace: `" " + G` | Rejects | Rejects | Our parser does not trim leading whitespace |
| Canonical GUID with source precision 35 | Accepts; has no precision parameter | Rejects | Preserves the former character-payload length restriction |
| `{G}` with source precision 36 | Rejects because of braces | Rejects because all 38 characters exceed precision | Precision applies to the **original input**, not just the extracted GUID |
| UUID version / variant restrictions | Does not enforce a particular version or variant | Does not add such restrictions | Neither is a UUID-generation-policy validator |
| Conversion result | Returns a `UUID` | Returns a `UUID` by calling `UUID.fromString()` on the validated 36-character portion | We reuse Java's conversion rather than implement binary parsing |

### Why validation moves into the driver

The malformed GUID cases were already failing before this PR, usually when SQL
Server converted the text to `uniqueidentifier`. But valid braced GUIDs and GUIDs
with suffixes could succeed. Our parser is intended to preserve that distinction.

**Before this PR, the affected flow was:**

```text
Source value -> CHAR text sent by bulk copy -> SQL Server validates/converts -> uniqueidentifier
```

**With the native path:**

```text
Source value -> driver validates/parses -> 16-byte GUID sent -> uniqueidentifier
```

Because SQL Server now receives binary GUID data, it no longer sees the original
text and cannot reject malformed text that Java silently accepted or normalized.
Preserving accepted and rejected inputs does not establish identical error or
transaction behavior; see the unresolved transaction compatibility issue in
section 9.

### Shared precision validation

Checking only `length == 36` before calling `UUID.fromString()` is insufficient.
For example, Java can accept the 36-character input
`06f9619ff-8b8-d011-b42d-00c04fc964ff`, even though its groups have lengths
9-3-4-4-12 rather than 8-4-4-4-12.

`validateGuidLength()` shares length validation between UUID and textual inputs:

- UUID: logical textual length is 36.
- String: use the original complete input length, before removing braces or
  ignoring a suffix.

Three named constants replace parsing magic numbers:

| Constant | Value |
| --- | --- |
| `GUID_TEXT_LENGTH` | 36 |
| `BRACED_GUID_TEXT_LENGTH` | 38 |
| `BRACED_GUID_CLOSING_BRACE_INDEX` | 37 |

## 6. Behavior by operation

| Operation | Final PR flow |
| --- | --- |
| Custom record declaring GUID -> plaintext GUID | Native. |
| CSV declaring GUID -> plaintext GUID | Native. |
| Plaintext SQL Server GUID ResultSet -> plaintext GUID | Native. |
| CHAR/VARCHAR, including streamed VARCHAR(MAX), -> GUID | Existing character/server-conversion path. |
| GUID -> character destination | Existing character path. |
| Encrypted source or destination | Existing encrypted/conversion handling. |
| Eligible bulk-copy prepared-statement INSERT batch -> GUID | Native through the batch adapter. |
| Ordinary prepared-statement execution without bulk-copy batching | Unchanged. |

For `useBulkCopyForBatchInsert=true`, the adapter already derives source metadata
from the destination. Consequently, even `setString()` input targeting
`uniqueidentifier` can reach the native writer.

No production changes were needed in `SQLServerPreparedStatement`.

There are no new connection properties, public APIs, or getter/setter/updater
conversion mappings.

## 7. Test and helper files

| File under `src/test/java/com/microsoft/sqlserver/jdbc/` | Purpose |
| --- | --- |
| `BulkCopyGuidParserTest.java` | Accepted/rejected text formats, precision, ASCII hex, and GUID bit patterns. |
| `BulkCopyGuidMetadataTest.java` | Native eligibility and exact GUID payload/null-marker encoding. |
| `bulkCopy/BulkCopyGuidTest.java` | Custom records, CSV, table-to-table conversions, mappings, cursors, streaming, source reuse, and server conversion checks. |
| `preparedStatement/BulkCopyGuidBatchInsertTest.java` | Both batch APIs, binding methods, nulls, mixed columns, options, statement reuse, and invalid inputs. |
| `AlwaysEncrypted/BulkCopyGuidAETest.java` | Existing encrypted-destination behavior and encrypted-source-to-plaintext-GUID consistency. |
| `BulkCopyCommandCapture.java` | Test-only capture of matching `INSERT BULK` declarations. |

The command-capture helper scopes itself to a uniquely named destination table.
It removes its handler afterward and restores the previous logger level. This
provides declaration assertions without requiring Extended Events permissions.

Integration coverage distinguishes "the value copied correctly" from "native
encoding was selected." It includes declaration assertions and server-side
conversion checks with character sources as positive controls.

The public `writeToServer(ResultSet)` Javadoc also explains native selection and
retained fallbacks.

## 8. Validation

The latest targeted suite passed 443 cases per profile under `jre11` and `jre8`,
both running on JDK 21, with zero failures, errors, or skips.

| Test class | Executed cases per profile |
| --- | ---: |
| `BulkCopyGuidParserTest` | 33 |
| `BulkCopyGuidMetadataTest` | 9 |
| `BulkCopyGuidTest` | 371 |
| `BulkCopyGuidBatchInsertTest` | 26 |
| Existing `BulkCopyAllTypesTest` | 4 |
| **Total** | **443** |

Commands:

```text
mvn -q -Pjre11 -Dtest=BulkCopyGuidParserTest,BulkCopyGuidMetadataTest,BulkCopyGuidTest,BulkCopyGuidBatchInsertTest,BulkCopyAllTypesTest test
mvn -q -Pjre8 -Dtest=BulkCopyGuidParserTest,BulkCopyGuidMetadataTest,BulkCopyGuidTest,BulkCopyGuidBatchInsertTest,BulkCopyAllTypesTest test
```

These results do not claim execution on an actual JDK 8 runtime or a full
repository-suite pass.

## 9. Remaining limitations and backward compatibility

### Transaction compatibility remains unresolved

Client-side GUID rejection can change the outcome of a caller-owned transaction
compared with the old server-side conversion error. The
[blocking review comment](https://github.com/microsoft/mssql-jdbc/pull/3041#discussion_r4095650956)
reports that earlier work can become committable after catching the new
client-side failure when the old server-side failure rolled it back.

This decision was explicitly deferred. The PR does not add an opt-in or force a
rollback of caller-owned transactions. Successful-value and parsing tests do not
establish equivalent transaction failure semantics.

### Always Encrypted execution is not verified locally

The AE test setup was blocked by the missing
`target/test-classes/JavaKeyStore.txt` fixture. The tests are present, but no local
AE execution pass is claimed.

### Performance improvement is not quantified

Native encoding and removal of the targeted server conversion are checked, but
no baseline/head throughput or CPU benchmark was run.

### Existing large-batch mapping limitation remains

The pre-existing reordered-column limitation in `executeLargeBatch()` is not
fixed here. Reordered INSERT columns are covered for `executeBatch()`;
`executeLargeBatch()` coverage uses destination-order columns.

## Summary

The final implementation adds a narrow native-GUID path around the existing bulk
copy machinery, using a shared eligibility check and SQL-compatible parsing.
It does not rewrite source metadata caching, conversion mappings, or existing
type/value writers. Full failure-semantics compatibility and merge readiness
remain subject to the outstanding transaction decision and validation limits.
