# Direct prepared-statement batch update counts

`prepareMethod=none` combines eligible `PreparedStatement` batch entries into one
language-batch request with literal parameter substitution. The direct path of
`scopeTempTablesToConnection` uses the same implementation. This fix preserves
that execution model: it does not add a round trip per entry or move all batches
to `sp_executesql`.

## Count and failure behavior

For entries producing one result each, `executeBatch()` and `executeLargeBatch()`
report actual server counts, including zero and multi-row counts. A successful
result with no count is `Statement.SUCCESS_NO_INFO` (`-2`), not `1` or `-1`.
Large-batch counts use the driver's `long` count without narrowing through
`getUpdateCount()`. Failed or unexecuted positions remain
`Statement.EXECUTE_FAILED` (`-3`) where the result sequence identifies them.

The driver processes the entire response, not just as many results as there are
array slots. Recoverable database errors and unexpected result sets fail the
batch through `BatchUpdateException`, including errors after all slots have
been filled. Its cause retains the original SQL exception and additional
observed errors in that exception's `getNextException()` chain. The chain is
finalized and detached from the statement before it is returned, so inspecting
a saved exception cannot consume results from a later execution. Session failures,
transaction rollback, cancellation and timeout are not converted into successful
batch results or ordinary recoverable batch failures.

## Important attribution limitation

**This is not general per-entry count or error attribution, nor parity with
`prepexec`.** Language-batch completion tokens identify SQL results, not JDBC
parameter sets. Compound SQL, conditional branches, loops and triggers can
produce different numbers of counts per entry. The returned array contains the
first observed result positions, up to the number of submitted entries; later
results are consumed for completion and errors, not aggregated into entry totals.

For example, one submitted entry containing an update of three rows followed by
an update of one row returns `[3]`, not an invented total `[4]`. With several
entries, results from the first entry can occupy slots an application might
otherwise interpret as later entries. Neither static SQL-statement counting nor
draining the response recovers the missing boundaries.

Similarly, `INSERT ...; RAISERROR(...)` can produce a valid count before the
error. A single-entry batch can throw `BatchUpdateException` with `[1]`. The
driver does not overwrite that count with `EXECUTE_FAILED` and pretend to know
which entry owns an unlabelled late error. **Always handle the exception even
when every returned count is non-negative.** Confirm persisted state or explicitly
roll back according to the application's transaction policy.

A combined language batch can abort subsequent entries differently from grouped
RPCs. Already submitted SQL is never replayed to recover counts. Use `prepexec`
when reliable per-entry boundaries are required, subject to that mode's scope
semantics, or execute entries individually if separate round trips are acceptable.

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
