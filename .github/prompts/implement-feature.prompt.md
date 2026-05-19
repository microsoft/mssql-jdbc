---
name: mssql-jdbc-implement-feature
description: Guided workflow for implementing a new feature in mssql-jdbc.
argument-hint: <feature name or issue number>
agent: agent
---

Implement the feature described in "${input:feature}".

Follow this workflow step-by-step:

## 1. Understand the Feature

- If a GitHub issue number is provided, fetch the full issue details from `microsoft/mssql-jdbc`.
- Identify the feature scope, requirements, and acceptance criteria.
- Determine which JRE profiles must support the feature (`jre8`, `jre11`, `jre17`, `jre21`, `jre25`, `jre26`).
- Check for related issues or prior discussions.
- If the feature involves a new connection string property, new data type, TDS protocol change, or authentication method, note the additional areas impacted.

## 2. Plan the Implementation

Before writing code, produce a brief implementation plan covering:

- **Files to modify or create** — all in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- **Public API surface changes** — any new classes, methods, properties, enums, or connection string keywords.
- **JRE profile considerations** — will the feature need Maven profile-based source inclusion or runtime JDK version checks?
- **Dependencies** — any new Maven dependencies needed in `pom.xml`?
- **Test strategy** — which test packages will cover this feature?
- **Documentation plan** — Javadoc, samples, CHANGELOG entries.

## 3. Implement the Feature

### Source Code

- Add or modify files in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- Follow the package structure in `.github/instructions/architecture.instructions.md`.
- Follow code patterns in `.github/instructions/patterns.instructions.md`:
  - Exceptions: `SQLServerException.makeFromDriverError(...)` with error keys from `SQLServerResource.java`.
  - Logging: Guard with `if (logger.isLoggable(Level.FINER))` — never log sensitive data.
  - Resources: Try-with-resources for all JDBC objects and I/O streams.
  - Null checks: Use `null == variable` pattern (project convention).
- Ensure the code compiles for ALL JRE profiles: `jre8` through `jre26`.
- Apply Eclipse formatter `mssql-jdbc_formatter.xml` to changed code.

### Connection String Properties (if applicable)

1. Add the property constant to `SQLServerDriverStringProperty` or `SQLServerDriverBooleanProperty` or `SQLServerDriverIntProperty`.
2. Add getter/setter to `SQLServerDataSource.java`.
3. Add parsing in `SQLServerConnection.java` connection property handling.
4. Default to a backward-compatible value.
5. Document the property in Javadoc.

### TDS Protocol Changes (if applicable)

1. Reference the MS-TDS specification for the protocol extension.
2. Add new token/flag constants to the relevant TDS classes.
3. Implement parsing/writing in `IOBuffer.java`, `tdsparser.java`, and `Stream*.java` files.
4. Test against multiple SQL Server versions (2012+, Azure SQL).

### Authentication Changes (if applicable)

1. Update `SqlAuthentication` enum if adding a new authentication method.
2. Implement token acquisition in the appropriate auth class (`SQLServerMSAL4JUtils.java`, `KerbAuthentication.java`, etc.).
3. Ensure federated auth flow works with `FedAuthTokenCommand` if applicable.
4. Test with Azure AD, Kerberos, NTLM as appropriate.

## 4. Write Tests

- **Unit tests** in `src/test/java/com/microsoft/sqlserver/jdbc/unit/` for isolated logic.
- **Integration tests** in the appropriate feature package (`connection/`, `datatypes/`, `bulkCopy/`, etc.).
- Extend `AbstractTest` for tests needing a SQL Server connection.
- Use JUnit 5 annotations (`@Test`, `@Tag`, `@BeforeAll`, `@AfterAll`).
- Cover:
  - Happy path and edge cases
  - Cross-platform behavior (Windows, Linux, macOS)
  - Error handling and invalid inputs
  - Backward compatibility (existing behavior unchanged)
- Tag tests requiring external resources with correct group annotations.
- Do NOT hardcode connection strings — use test config utilities.
- Consider state machine tests for complex stateful features (see `.github/instructions/state-machine-testing.instructions.md`).

## 5. Add Documentation

- Add Javadoc comments on all new public APIs.
- Update `CHANGELOG.md` with the new feature.
- If adding a new connection property, document it in the appropriate instructions file.

## 6. Validate

- Compile for all profiles:
  ```bash
  mvn clean compile -Pjre8
  mvn clean compile -Pjre11
  mvn clean compile -Pjre17
  mvn clean compile -Pjre21
  mvn clean compile -Pjre25
  mvn clean compile -Pjre26
  ```
- Run relevant tests:
  ```bash
  mvn test -Dtest=<TestClass> -pl . -Pjre11
  ```
- Run full test suite before PR:
  ```bash
  mvn clean verify -Pjre11
  ```

## 7. Prepare for PR

- Summarize the feature and link to the issue.
- Provide a checklist:
  - [ ] Compiles on all JRE profiles (`jre8` through `jre26`)
  - [ ] Tests added (unit and integration as appropriate)
  - [ ] Javadoc on all new public APIs
  - [ ] No breaking changes to existing APIs
  - [ ] No sensitive data logged
  - [ ] Backward-compatible defaults for new properties
  - [ ] Follows coding guidelines (`Coding_Guidelines.md`)
  - [ ] Eclipse formatter applied to changed code
  - [ ] CHANGELOG.md updated
