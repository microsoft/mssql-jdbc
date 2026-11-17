# Direct prepared-statement batch update counts

`prepareMethod=none` combines eligible `PreparedStatement` batch entries into one
language-batch request with literal parameter substitution. The direct path of
`scopeTempTablesToConnection` uses the same implementation. This fix preserves
that execution model: it does not add a round trip per entry or move all batches
to `sp_executesql`.

## Count and failure behavior

Each result read from the response is assigned to the next batch entry:

- an update count is reported as returned, including `0` and multi-row counts;
- a successful result without a count, such as under `SET NOCOUNT ON`, is
  `Statement.SUCCESS_NO_INFO` (`-2`);
- a recoverable error marks that entry `Statement.EXECUTE_FAILED` (`-3`), and
  `executeBatch()` / `executeLargeBatch()` throw `BatchUpdateException` with the
  counts;
- entries without a result, for example after an error that aborts the rest of
  the batch, stay `Statement.EXECUTE_FAILED`.

Session failures and transaction rollbacks are thrown immediately instead of
being reported as entry failures.

## Limitations

**Counts are reliable only when each entry produces exactly one result**, such as
a single `INSERT`, `UPDATE`, `DELETE` or `MERGE` without an `OUTPUT` result set.
The server's responses identify SQL statements, not JDBC parameter sets, so:

- compound or conditional entries (`INSERT ...; UPDATE ...`, `IF ... UPDATE ...`)
  produce a variable number of results, and results from one entry can fill the
  slots of later entries;
- only as many results are read as there are entries. A result set, or an error
  raised after an entry's count, can go unreported;
- an error that aborts the batch, such as a conversion failure, `THROW` or
  `XACT_ABORT`, also stops all later entries, which would still run under
  `prepexec`;
- T-SQL that must be unique or alone in a batch, such as `DECLARE` of the same
  variable, `GOTO` labels, or `CREATE VIEW`, fails when entries are combined.

Use `prepexec` when entries contain more than one statement.

## Routing and scope

Under `scopeTempTablesToConnection`, the detector recognizes local
`CREATE TABLE #...` and `SELECT ... INTO #...` operations. Ordinary DML merely
referencing a pre-existing temporary table does **not** select the direct path.
Local temporary tables actually created by a direct batch retain connection
lifetime.

Existing procedure-call and unsupported-literal-parameter routing remains
unchanged. Parameters such as streams can reject literal construction before
submission and select typed RPC execution; temporary tables created inside such
an RPC have RPC scope. This pre-send fallback is not a retry of submitted SQL.

## Alternatives

This is proposal 1 from [the analysis in #3101](https://github.com/microsoft/mssql-jdbc/pull/3101).
Proposal 2, using grouped unprepared RPCs for all otherwise-direct batch entries,
is an **alternative**, not a dependency. It supplies RPC entry boundaries but
broadens RPC-scoped temporary-table behavior.
