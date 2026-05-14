---
name: mssql-jdbc-fix-bug
description: Guided workflow for fixing a bug in mssql-jdbc.
argument-hint: <issue number or description>
agent: agent
---

Fix the bug described in "${input:issue}".

Follow this workflow step-by-step:

## 1. Understand the Bug

- If a GitHub issue number is provided, fetch the issue details from `microsoft/mssql-jdbc`.
- Identify the repro steps, expected behavior, and actual behavior.
- Determine which JRE profiles are affected (`jre8`, `jre11`, `jre17`, `jre21`, `jre25`, `jre26`).
- Determine which platforms are affected (Windows, Linux, macOS).
- If the issue lacks sufficient detail, note what information is missing.

## 2. Locate the Relevant Code

- All source code lives in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- Search for related classes, methods, or keywords using the architecture guide:
  - **Connection issues**: `SQLServerConnection.java`, `SQLServerDataSource.java`
  - **Statement issues**: `SQLServerStatement.java`, `SQLServerPreparedStatement.java`, `SQLServerCallableStatement.java`
  - **ResultSet issues**: `SQLServerResultSet.java`, `Column.java`, `ScrollWindow.java`
  - **TDS protocol issues**: `IOBuffer.java`, `tdsparser.java`, `Stream*.java`
  - **Data type issues**: `DataTypes.java`, `DDC.java`, `dtv.java`
  - **Authentication issues**: `KerbAuthentication.java`, `NTLMAuthentication.java`, `SQLServerMSAL4JUtils.java`
  - **Bulk copy issues**: `SQLServerBulkCopy.java`, `SQLServerBulkCopyOptions.java`
  - **Always Encrypted issues**: `SQLServerColumnEncryption*.java`, `SQLServerAeadAes256CbcHmac256*.java`
  - **Retry/Resilience issues**: `ConfigurableRetryLogic.java`, `IdleConnectionResiliency.java`
- Check error resource keys in `SQLServerResource.java` for related error messages.

## 3. Write a Failing Test

- Create a test that reproduces the bug BEFORE implementing the fix.
- Choose the correct test package under `src/test/java/com/microsoft/sqlserver/jdbc/`:
  - `unit/` — use only when it matches existing test patterns in that area; do not assume tests there are database-free, as many extend `AbstractTest` and require SQL Server
  - Feature-specific packages (`connection/`, `datatypes/`, `bulkCopy/`, etc.) — typically for integration tests and other area-specific coverage
- Follow existing test patterns:
  - Extend `AbstractTest` for tests needing a SQL Server connection.
  - Use JUnit 5 (`@Test`, `@Tag`, `@BeforeAll`, `@AfterAll`).
  - Tag tests requiring external resources with appropriate group annotations (`xSQLv12`, `xSQLv15`, `reqExternalSetup`, `fedAuth`, etc.).
- Do NOT hardcode connection strings — use `AbstractTest` utilities and test config.

## 4. Implement the Fix

- Make the minimal change needed to fix the bug.
- Ensure the fix compiles for ALL JRE profiles: `jre8`, `jre11`, `jre17`, `jre21`, `jre25`, `jre26`.
- Follow code patterns in `.github/instructions/patterns.instructions.md`:
  - Wrap exceptions via `SQLServerException.makeFromDriverError(...)`.
  - Add error messages to `SQLServerResource.java` with `R_` prefix if new messages are needed.
  - Guard log statements with `if (logger.isLoggable(Level.FINER))`.
  - Never log sensitive data (passwords, tokens, connection strings with credentials).
- Do NOT introduce breaking changes to public APIs.
- Use try-with-resources for any new JDBC resource handling.

## 5. Validate

- Verify the failing test now passes with the fix applied.
- Check that existing tests in the affected area still pass:
  ```bash
  mvn test -Dtest=<TestClass> -pl . -Pjre11
  ```
- Check that the full build compiles cleanly:
  ```bash
  mvn clean compile -Pjre11
  ```
- Review that no new compiler warnings or errors are introduced.

## 6. Document

- Add Javadoc comments if the fix changes behavior of documented APIs.
- If the fix addresses a user-visible issue, note it for CHANGELOG.md.

## 7. Prepare for PR

- Summarize the root cause and the fix.
- Reference the issue with `Fixes #<issue_number>`.
- Provide a checklist:
  - [ ] Tests added or updated
  - [ ] Compiles on all JRE profiles (`jre8` through `jre26`)
  - [ ] No sensitive data logged
  - [ ] No breaking changes introduced
  - [ ] Existing tests still pass
  - [ ] Follows coding guidelines (`Coding_Guidelines.md`)
