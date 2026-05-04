---
name: mssql-jdbc-code-review
description: AI-assisted code review for a pull request in mssql-jdbc.
argument-hint: <PR number or branch name>
agent: agent
---

Review the pull request "${input:pr}" in `microsoft/mssql-jdbc`.

Follow this structured review process:

## 1. Understand the Change

- Fetch the PR details: title, description, linked issue(s), and diff.
- Read the PR description to understand the intent and scope of the change.
- Check which files are modified and categorize them:
  - **Driver source** (`src/main/java/com/microsoft/sqlserver/jdbc/`) — the main review focus
  - **Tests** (`src/test/java/`) — verify coverage
  - **Build config** (`pom.xml`, `build.gradle`) — dependency or plugin changes
  - **Documentation** (`README.md`, `CHANGELOG.md`, `doc/`) — doc updates
  - **Resources** (`src/main/resources/`, `SQLServerResource.java`) — error messages or config

## 2. Architectural Compliance

Verify the change follows project architecture rules (see `.github/instructions/architecture.instructions.md`):

- [ ] Source code follows the package structure under `com.microsoft.sqlserver.jdbc`
- [ ] TDS protocol changes are in `IOBuffer.java`, `tdsparser.java`, or `Stream*.java` files
- [ ] Connection property changes update `SQLServerConnection.java`, `SQLServerDataSource.java`, and `SQLServerDriver.java`
- [ ] New error messages are added to `SQLServerResource.java` using the `R_` prefix convention
- [ ] The change compiles for all JRE profiles: `jre8`, `jre11`, `jre17`, `jre21`, `jre25`, `jre26`
- [ ] No platform-specific assumptions — driver must work on Windows, Linux, and macOS

## 3. Code Quality Review

Check against `Coding_Guidelines.md` and `coding-best-practices.md`:

### Style

- Eclipse formatter `mssql-jdbc_formatter.xml` applied to changed code
- Javadoc comments on public methods (not required for simple getters/setters)
- `null == variable` pattern for null checks (project convention)
- `camelCase` for local variables and methods, `PascalCase` for class names
- Constants are `UPPER_SNAKE_CASE`

### Best Practices

- Try-with-resources for `Connection`, `Statement`, `ResultSet`, streams, and sockets
- No unnecessary object creation in hot paths (tight loops, TDS parsing)
- Specific exception types caught (`SQLTimeoutException`, `IOException`) — not bare `Exception`
- Exceptions wrapped via `SQLServerException.makeFromDriverError(...)` with localized error keys
- Logger level checks before constructing log messages: `if (logger.isLoggable(Level.FINER))`
- No sensitive data (passwords, tokens, connection strings) in log messages or stack traces
- Shared mutable state is properly synchronized or `volatile`

## 4. Security Review

- [ ] No credentials, passwords, or secrets in code or comments
- [ ] No sensitive data logged via `java.util.logging` or exposed in exceptions
- [ ] SQL injection prevention via parameterized queries, even in metadata calls
- [ ] TLS/certificate settings handled safely (no blind `trustServerCertificate=true` in production code)
- [ ] Inputs validated to prevent injection in SQL or shell commands
- [ ] No unbounded data streams sent to server prior to authentication (e.g., in feature extensions)
- [ ] Sensitive `char[]` arrays (passwords, tokens) cleared after use; use `SecureStringUtil` for encrypted storage
- [ ] Always Encrypted key material handled via `SQLServerColumnEncryptionKeyStoreProvider` — not inline

## 5. Test Coverage Review

- [ ] Bug fixes have a test that would have caught the bug
- [ ] New features have unit tests and integration tests as appropriate
- [ ] Tests are in the correct package under `src/test/java/com/microsoft/sqlserver/jdbc/`
  - `unit/` — isolated tests, no SQL Server needed
  - Feature-specific packages (`connection/`, `datatypes/`, `bulkCopy/`, etc.) — integration tests
- [ ] Tests do NOT use hardcoded connection strings (use `AbstractTest` base class and test config)
- [ ] Tests that require SQL Server are tagged with appropriate JUnit groups
- [ ] Tests excluded from default runs use correct group annotations (`xSQLv12`, `xSQLv15`, `reqExternalSetup`, `fedAuth`, `kerberos`, etc.)
- [ ] No flaky patterns (timing-dependent, order-dependent, resource-dependent)
- [ ] Consider state machine tests for complex state interactions (see `.github/instructions/state-machine-testing.instructions.md`)

## 6. API Design Review (if public API changed)

- [ ] Follows JDBC specification (4.2/4.3) for standard interfaces
- [ ] No breaking changes to existing public APIs
- [ ] New public methods have Javadoc comments
- [ ] New connection string properties are documented and have defaults preserving backward compatibility
- [ ] New `ISQLServer*` interfaces follow existing naming patterns
- [ ] Deprecated APIs have `@Deprecated` annotation with `@since` and migration guidance

## 7. Backward Compatibility

- [ ] Existing behavior is preserved for current consumers
- [ ] Default values for new connection properties maintain old behavior
- [ ] Driver works with SQL Server 2012+ and Azure SQL Database
- [ ] Changes compile across all JRE profiles (`jre8` through `jre26`)
- [ ] If behavior changes, it's documented and opt-in (e.g., via connection property or feature flag)
- [ ] No JAR or package changes that break existing references

## 8. Performance Impact

- [ ] No new allocations on hot paths (TDS parsing, result set iteration)
- [ ] Buffers reused via preallocated arrays where possible
- [ ] No Java Stream API operations (`.stream().filter().map()`) on hot paths — use manual loops
- [ ] Caches or pools are bounded (not growing unbounded)
- [ ] Lazy initialization for expensive objects used conditionally
- [ ] For major features or large PRs, internal performance benchmarks should be run

## 9. Documentation and Changelog

- [ ] Javadoc comments on new/changed public APIs
- [ ] PR description references the issue (`Fixes #...`)
- [ ] PR description includes a self-contained summary (see `review-process.md`)
- [ ] CHANGELOG.md updated if user-visible behavior changes

## 10. Summary

Provide a review summary with:
- **Verdict**: Approve / Request Changes / Comment
- **Strengths**: What the PR does well
- **Issues Found**: Categorized as Critical / Major / Minor / Nit
- **Suggestions**: Optional improvements or alternative approaches
- **Questions**: Any clarifications needed from the author
- **Questions**: Any clarifications needed from the author
