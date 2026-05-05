# Copilot Instructions — mssql-jdbc

## 📚 Project Overview

This project is the Microsoft JDBC Driver for SQL Server, enabling Java applications to interact with SQL Server and Azure SQL databases. It implements the JDBC 4.2/4.3 specification, communicating over the TDS (Tabular Data Stream) protocol. It supports features like connection pooling, Always Encrypted, Azure AD authentication, bulk copy, configurable retry logic, and idle connection resiliency.

The project builds from a single Maven project (`pom.xml`) with multiple JRE profiles (`jre8`, `jre11`, `jre17`, `jre21`, `jre25`, `jre26`). Each profile compiles against a different Java source level and produces a profile-specific JAR.

The project includes:

- **Driver source**: All source code in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- **Error resources**: Localized error messages in `SQLServerResource.java`.
- **Tests**: Located in `src/test/java/com/microsoft/sqlserver/jdbc/`.
  - **Unit Tests**: In `src/test/java/com/microsoft/sqlserver/jdbc/unit/` — isolated tests, no SQL Server needed.
  - **Integration Tests**: In feature-specific packages (`connection/`, `datatypes/`, `bulkCopy/`, `AlwaysEncrypted/`, etc.) — require a SQL Server instance.
  - **BVT Tests**: In `src/test/java/com/microsoft/sqlserver/jdbc/bvt/` — build verification / smoke tests.
  - **State Machine Tests**: In `src/test/java/com/microsoft/sqlserver/jdbc/statemachinetest/` — model-based testing for complex state interactions.

## 🔧 Working with Issues

- If the issue is a bug, reproduce it and identify the root cause in source code.
- If the issue is a feature request, review the proposal and assess its feasibility.
- If the issue is a task, follow the instructions provided in the issue description.
- Cross-reference issue descriptions with code in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- If public APIs are changed, update Javadoc comments on all affected public members.
- Add or update tests in `src/test/java/` to validate the fix.

### 🧪 Writing Tests

- For every bug fix, ensure there are unit tests and integration tests that cover the scenario.
- For new features, write tests that validate the functionality.
- **Write a failing test before implementing the fix** (test-driven approach).
- Use the existing test framework: extend `AbstractTest` for tests needing SQL Server, use JUnit 5 annotations (`@Test`, `@Tag`, `@BeforeAll`, `@AfterAll`).
- Follow the naming conventions and structure of existing tests.
- Ensure tests are comprehensive and cover edge cases.
- Do NOT hardcode connection strings — use `AbstractTest` utilities and test config.
- Tag tests requiring external resources with appropriate group annotations (`xSQLv12`, `xSQLv15`, `reqExternalSetup`, `fedAuth`, `kerberos`, etc.).
- Consider state machine tests for complex stateful features (see `.github/instructions/state-machine-testing.instructions.md`).

### ⚙️ Automating Workflows

- Auto-label PRs based on folder paths (e.g., changes in `src/main/java/` → `area-driver`, changes in `src/test/java/` → `area-testing`).
- Suggest CHANGELOG entries for fixes in `CHANGELOG.md`.
- Tag reviewers based on area of change.

## 🧠 Contextual Awareness

- All source code is in `src/main/java/com/microsoft/sqlserver/jdbc/`. Follow the package structure described in `.github/instructions/architecture.instructions.md`.
- The driver must work cross-platform: Windows, Linux, and macOS. Do not make platform-specific assumptions.
- Code must compile across all JRE profiles (`jre8` through `jre26`). Avoid using APIs unavailable in older JDK versions without profile guards.
- Respect API compatibility rules — do not introduce breaking changes without proper justification and documentation.
- Follow exception handling patterns: `SQLServerException.makeFromDriverError(...)` with error keys from `SQLServerResource.java` (see `.github/instructions/patterns.instructions.md`).
- Guard log statements: `if (logger.isLoggable(Level.FINER))` — never log sensitive data (passwords, tokens, connection strings with credentials).

## Constraints

- Do not change repository ownership or review-routing conventions without team discussion.
- Do not close issues without a fix or without providing a clear reason.
- All changed code must be formatted with Eclipse formatter `mssql-jdbc_formatter.xml`.

## 📝 Notes

- Follow `Coding_Guidelines.md` for code style and `coding-best-practices.md` for engineering practices.
- Follow `review-process.md` for PR review guidelines.
- Regularly review and update documentation to ensure it reflects the current state of the project.
