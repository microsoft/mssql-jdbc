---
name: mssql-jdbc-setup-dev
description: "Set up the development environment for mssql-jdbc"
agent: 'agent'
---
# Dev Setup Prompt for microsoft/mssql-jdbc

You are a development assistant helping set up the environment for contributing to the Microsoft JDBC Driver for SQL Server.

## TASK

Walk the developer through setting up their local development environment. Follow each step in order, verifying success before moving on.

---

## STEP 1: Install JDK

The project requires **JDK 11 or newer** for all builds (including the `jre8` profile, which cross-compiles).

Check if a JDK is already installed:

```bash
java -version
javac -version
```

If not installed, install a JDK (any distribution — Microsoft OpenJDK, Adoptium, Oracle):

```bash
# Example: Microsoft OpenJDK 21 (Windows — winget)
winget install Microsoft.OpenJDK.21

# Example: Adoptium (macOS — Homebrew)
brew install --cask temurin@21

# Example: Adoptium (Linux — apt)
sudo apt install temurin-21-jdk
```

Verify `JAVA_HOME` is set:

```bash
# macOS/Linux
echo $JAVA_HOME

# Windows (PowerShell)
echo $env:JAVA_HOME
```

---

## STEP 2: Install Maven

The project requires **Maven 3.5.0 or newer**.

Check if Maven is already installed:

```bash
mvn --version
```

If not installed:

```bash
# Windows (winget)
winget install Apache.Maven

# macOS (Homebrew)
brew install maven

# Linux (apt)
sudo apt install maven
```

Verify Maven can find the JDK:

```bash
mvn --version
# Should show the correct Java version and JAVA_HOME
```

---

## STEP 3: Clone the Repository

```bash
git clone https://github.com/microsoft/mssql-jdbc.git
cd mssql-jdbc
```

---

## STEP 4: Verify the Build

Run a quick compile to confirm everything works:

```bash
mvn clean compile -Pjre11
```

If this succeeds, the environment is correctly set up.

---

## STEP 5: Configure Test Database (Optional)

Integration tests require a SQL Server instance. Set the connection string as an environment variable:

```bash
# macOS/Linux
export mssql_jdbc_test_connection_properties="jdbc:sqlserver://localhost:1433;databaseName=testDb;user=sa;password=yourPassword;encrypt=true;trustServerCertificate=true;"

# Windows (PowerShell)
$env:mssql_jdbc_test_connection_properties = "jdbc:sqlserver://localhost:1433;databaseName=testDb;user=sa;password=yourPassword;encrypt=true;trustServerCertificate=true;"
```

> **Tip**: For a quick local SQL Server, use Docker:
> ```bash
> docker run -e "ACCEPT_EULA=Y" -e "SA_PASSWORD=YourStrong!Passw0rd" -p 1433:1433 -d mcr.microsoft.com/mssql/server:2022-latest
> ```

Unit tests in `src/test/java/com/microsoft/sqlserver/jdbc/unit/` do **not** require a database.

---

## STEP 6: IDE Setup (Optional)

### Code Formatter

All changed code must be formatted with the project's Eclipse formatter. The formatter config is in the repository root:

- **Eclipse**: Import `mssql-jdbc_formatter.xml` via *Preferences > Java > Code Style > Formatter > Import*.
- **IntelliJ IDEA**: Install the *Eclipse Code Formatter* plugin, then point it to `mssql-jdbc_formatter.xml`.
- **VS Code**: Use the *Language Support for Java* extension. Add to `.vscode/settings.json`:
  ```json
  {
    "java.format.settings.url": "mssql-jdbc_formatter.xml"
  }
  ```

### Checkstyle

The project includes `mssql-jdbc-checkstyle.xml` for static analysis. Configure your IDE's Checkstyle plugin to use it.

---

## STEP 7: Verify Full Setup

Run this checklist:

```bash
# 1. Java
java -version          # Should be 11+

# 2. Maven
mvn --version          # Should be 3.5.0+

# 3. Compile
mvn clean compile -Pjre11

# 4. Unit tests (no DB needed)
mvn clean test -Pjre11 -Dtest="com.microsoft.sqlserver.jdbc.unit.**"

# 5. (Optional) Integration tests (requires DB)
mvn clean test -Pjre11
```

---

## Key Files Reference

| File | Purpose |
|------|---------|
| `pom.xml` | Maven build config with JRE profiles |
| `mssql-jdbc_formatter.xml` | Eclipse code formatter config |
| `mssql-jdbc-checkstyle.xml` | Checkstyle rules |
| `Coding_Guidelines.md` | Java coding standards |
| `coding-best-practices.md` | Engineering best practices |
| `CONTRIBUTING.md` | Contribution guidelines |

---

## Next Steps

Once your environment is ready:

1. **Build the driver** -> Use `#build`
2. **Run tests** -> Use `#run-tests`
3. **Fix a bug** -> Use `#fix-bug`
4. **Implement a feature** -> Use `#implement-feature`
