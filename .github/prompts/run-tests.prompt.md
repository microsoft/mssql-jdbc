---
name: mssql-jdbc-run-tests
description: "Run tests for the mssql-jdbc driver with Maven"
agent: 'agent'
---
# Run Tests Prompt for microsoft/mssql-jdbc

You are a development assistant helping run tests for the Microsoft JDBC Driver for SQL Server.

## PREREQUISITES

- Java and Maven are installed (`java -version`, `mvn --version`).
- The project compiles successfully (`mvn clean compile -Pjre11`).

### Connection String Setup

Most tests require a SQL Server instance. Set the connection string as an environment variable:

```bash
# macOS/Linux
export mssql_jdbc_test_connection_properties="jdbc:sqlserver://localhost:1433;databaseName=testDb;user=sa;password=yourPassword;encrypt=true;trustServerCertificate=true;"

# Windows (PowerShell)
$env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://localhost:1433;databaseName=testDb;user=sa;password=yourPassword;encrypt=true;trustServerCertificate=true;"
```

> **Note**: Most tests — including many under `src/test/java/com/microsoft/sqlserver/jdbc/unit/` — extend `AbstractTest` and require a configured database connection. Only a small subset of unit tests can run without a database.

---

## STEP 1: Choose JRE Profile

Ask the developer which JRE profile to test against:

> "Which JRE profile should tests run against?"
> - `jre8` (requires JDK 11+)
> - `jre11` (requires JDK 11+)
> - `jre17` (requires JDK 17+)
> - `jre21` (requires JDK 21+)
> - `jre25` (requires JDK 25+)
> - `jre26` (requires JDK 26+, default)

Use the selected profile (referred to as `<profile>` below) in all `mvn` commands. Substitute `<profile>` with the user's choice (e.g., `-Pjre11`).

---

## STEP 2: Choose What to Run

Ask the developer what they need:

> "What tests would you like to run?"
> 1. **Unit tests** — Tests under `unit/` (most still require a configured test DB)
> 2. **Single test class** — Run one specific test
> 3. **Single test method** — Run one specific method
> 4. **Feature suite** — Run tests for a specific feature (e.g., bulkCopy, connection)
> 5. **All default tests** — Run all tests (excludes external-setup and auth tests)
> 6. **BVT / smoke tests** — Build verification tests
> 7. **State machine tests** — Model-based stateful tests

---

## STEP 3: Run Tests

Substitute `<profile>` below with the profile selected in Step 1 (e.g., `-Pjre11`).

### Option 1: Unit Tests

```bash
mvn clean test -P<profile> -pl . -Dtest="com.microsoft.sqlserver.jdbc.unit.**"
```

### Option 2: Single Test Class

```bash
mvn clean test -P<profile> -Dtest=MyTestClassName
```

### Option 3: Single Test Method

```bash
mvn clean test -P<profile> -Dtest="MyTestClassName#myTestMethod"
```

### Option 4: Feature Suite

```bash
# Examples — replace the package with the target feature:
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.connection.**"
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.bulkCopy.**"
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.datatypes.**"
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.preparedStatement.**"
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.resultset.**"
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.AlwaysEncrypted.**"
```

### Option 5: All Default Tests

```bash
mvn clean test -P<profile>
```

This runs all tests except those tagged with excluded groups (see **Test Tags** below).

### Option 6: BVT / Smoke Tests

```bash
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.bvt.**"
```

### Option 7: State Machine Tests

```bash
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.statemachinetest.**"
```

State machine tests use seed-based reproducibility. To replay a failure, find the seed in the test output and pass it back:

```bash
mvn clean test -P<profile> -Dtest="TestClassName#testMethod" -DsmtSeed=<seed>
```

See `.github/instructions/state-machine-testing.instructions.md` for details.

---

## STEP 4: Filter by Test Tags

The driver uses JUnit 5 `@Tag` annotations to categorize tests. Maven Surefire filters them via `-Dgroups` (include) and `-DexcludedGroups` (exclude).

### Default Excluded Tags

The following tags are **excluded by default** (see `pom.xml`):

| Tag | Meaning |
|-----|---------|
| `xSQLv12`, `xSQLv15` | Not compatible with that SQL Server version |
| `NTLM` | Requires NTLM authentication setup |
| `MSI` | Requires Managed Service Identity |
| `reqExternalSetup` | Requires external infrastructure |
| `clientCertAuth` | Requires client certificate auth |
| `fedAuth` | Requires federated authentication (Azure AD) |
| `kerberos` | Requires Kerberos KDC |
| `vectorTest`, `vectorFloat16Test` | Requires vector data type support |
| `JSONTest` | Requires JSON data type support |

> The `jre8` profile also excludes `xJDBC42` (JDBC 4.3 tests).

### Including a Specific Tag

```bash
# Run only tests tagged with a specific group
mvn clean test -P<profile> -Dgroups="reqExternalSetup"
mvn clean test -P<profile> -Dgroups="fedAuth"
```

### Overriding Exclusions

```bash
# Remove all default exclusions (run everything)
mvn clean test -P<profile> -DexcludedGroups=""

# Exclude only specific tags
mvn clean test -P<profile> -DexcludedGroups="NTLM,kerberos"
```

### SQL Server Version Tags

Tests tagged `xSQLvXX` are **incompatible** with that version (the `x` means "exclude on"):

| Tag | Excluded on |
|-----|-------------|
| `xSQLv11` | SQL Server 2012 |
| `xSQLv12` | SQL Server 2014 |
| `xSQLv14` | SQL Server 2017 |
| `xSQLv15` | SQL Server 2019 |
| `xSQLv16` | SQL Server 2022 |
| `xSQLv17` | SQL Server 2025 |
| `xAzureSQLDB` | Azure SQL Database |
| `xAzureSQLDW` | Azure Synapse Analytics |
| `xAzureSQLMI` | Azure SQL Managed Instance |

---

## STEP 5: Review Results

### Test Reports

```bash
# Summary is printed to console. Detailed reports are in:
ls target/surefire-reports/

# View a specific report
cat target/surefire-reports/TEST-com.microsoft.sqlserver.jdbc.connection.ConnectionTest.xml
```

### Enable Debug Logging

Set environment variables to capture driver log output during tests:

```bash
# macOS/Linux
export mssql_jdbc_logging=true
export mssql_jdbc_logging_handler=console   # or: file, stream

# Windows (PowerShell)
$env:mssql_jdbc_logging = "true"
$env:mssql_jdbc_logging_handler = "console"
```

---

## STEP 6: Cross-Profile Validation

To verify a change works across JRE profiles, run tests under multiple profiles:

```bash
mvn clean test -Pjre8
mvn clean test -Pjre11
mvn clean test -Pjre17
mvn clean test -Pjre21
mvn clean test -Pjre25
mvn clean test -Pjre26
```

> Use JDK 11+ for `jre8` and `jre11` profiles. Use the matching JDK for `jre17+`.

---

## Test Directory Reference

| Directory | Type | DB Required? |
|-----------|------|-------------|
| `unit/` | Unit tests | Most require DB (many extend `AbstractTest`) |
| `bvt/` | Smoke tests | Yes |
| `connection/` | Connection tests | Yes |
| `datatypes/` | Data type tests | Yes |
| `bulkCopy/` | Bulk copy tests | Yes |
| `preparedStatement/` | PreparedStatement tests | Yes |
| `callablestatement/` | CallableStatement tests | Yes |
| `resultset/` | ResultSet tests | Yes |
| `AlwaysEncrypted/` | Always Encrypted tests | Yes |
| `fedauth/` | Federated auth tests | Yes + Azure AD |
| `resiliency/` | Connection resiliency tests | Yes |
| `configurableretry/` | Retry logic tests | Yes |
| `tvp/` | Table-valued parameter tests | Yes |
| `statemachinetest/` | State machine (MBT) tests | Yes |

All test source is under `src/test/java/com/microsoft/sqlserver/jdbc/`.

---

## Quick Reference

Substitute `<profile>` with the selected JRE profile (e.g., `jre11`).

```bash
# Unit tests (most require configured test DB)
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.unit.**"

# Single test class
mvn clean test -P<profile> -Dtest=ConnectionTest

# Single method
mvn clean test -P<profile> -Dtest="ConnectionTest#testOpenConnection"

# BVT smoke tests
mvn clean test -P<profile> -Dtest="com.microsoft.sqlserver.jdbc.bvt.**"

# All default tests
mvn clean test -P<profile>

# Include external-setup tests
mvn clean test -P<profile> -Dgroups="reqExternalSetup"

# Full verify (compile + test + package)
mvn clean verify -P<profile>
```
