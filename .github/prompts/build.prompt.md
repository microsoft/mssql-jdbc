---
description: "Build the mssql-jdbc driver with Maven"
name: "mssql-jdbc-build"
agent: 'agent'
---
# Build Prompt for microsoft/mssql-jdbc

You are a development assistant helping build the Microsoft JDBC Driver for SQL Server.

## PREREQUISITES

> This prompt assumes your development environment is already set up.
> If it is not, install and configure the required Java and Maven tools before continuing.

**Quick sanity check:**
```bash
java -version
mvn --version
```

---

## TASK

Help the developer build the mssql-jdbc driver after making code changes. Follow this process sequentially.

---

## STEP 0: Understand the Build

### JRE Profiles

The driver supports multiple JRE profiles. Each profile compiles against a different Java source level and produces a profile-specific JAR.

| Profile | Source Level | Default? | Output JAR |
|---------|-------------|----------|------------|
| `jre8` | Java 8 | No | `mssql-jdbc-{version}.jre8.jar` |
| `jre11` | Java 11 | No | `mssql-jdbc-{version}.jre11.jar` |
| `jre17` | Java 17 | No | `mssql-jdbc-{version}.jre17.jar` |
| `jre21` | Java 21 | No | `mssql-jdbc-{version}.jre21.jar` |
| `jre25` | Java 25 | No | `mssql-jdbc-{version}.jre25.jar` |
| `jre26` | Java 26 | Yes | `mssql-jdbc-{version}.jre26.jar` |

### When to Rebuild

- After modifying any `.java` source files in `src/main/`
- After changing `pom.xml` dependencies or plugin configuration
- After pulling changes from remote
- After switching branches

---

## STEP 1: Choose JRE Profile

Ask the developer which JRE profile to build against:

> "Which JRE profile should be used for this build?"
> - `jre8` (requires JDK 11+, cross-compiles to Java 8 bytecode)
> - `jre11` (requires JDK 11+)
> - `jre17` (requires JDK 17+)
> - `jre21` (requires JDK 21+)
> - `jre25` (requires JDK 25+)
> - `jre26` (requires JDK 26+, default)

Use the selected profile (referred to as `<profile>` below) in all `mvn` commands.

---

## STEP 2: Choose Build Type

Ask the developer what they need:

> "What would you like to build?"
> 1. **Quick compile** - Compile only (fastest, no JAR)
> 2. **Package JAR** - Compile + create JAR (skip tests)
> 3. **Full build** - Compile + test + package
> 4. **Install locally** - Build + install to local Maven repo
> 5. **Run tests only** - Run tests without packaging (requires DB connection)

---

## STEP 3: Build the Driver

Substitute `<profile>` with the profile selected in Step 1 (e.g., `-Pjre11`).

### Option 1: Quick Compile

```bash
mvn clean compile -P<profile>
```

### Option 2: Package JAR (Skip Tests)

```bash
mvn clean package -DskipTests -P<profile>
```

### Option 3: Full Build with Tests

```bash
mvn clean verify -P<profile>
```

> Requires SQL Server connection string. See `#run-tests` for details.

### Option 4: Install to Local Maven Repository

```bash
mvn clean install -DskipTests -P<profile>
```

This installs the JAR to `~/.m2/repository/com/microsoft/sqlserver/mssql-jdbc/` for use as a dependency in other projects.

### Option 5: Run Tests Only

```bash
mvn clean test -P<profile>
```

> Requires the `mssql_jdbc_test_connection_properties` environment variable set to a valid connection string. See `#run-tests` for details.

---

## STEP 4: Verify the Build

### 3.1 Check Output

```bash
# List built artifacts
ls target/*.jar

# Check JAR contents
jar tf target/mssql-jdbc-*.jar | head -20
```

### 3.2 Expected Output Location

Build artifacts are in the `target/` directory:

| Artifact | Description |
|----------|-------------|
| `target/mssql-jdbc-{version}.{profile}.jar` | Driver JAR |
| `target/classes/` | Compiled classes |
| `target/test-classes/` | Compiled test classes |
| `target/surefire-reports/` | Test reports (if tests ran) |

---

## STEP 5: Generate Javadoc (Optional)

```bash
mvn javadoc:javadoc -P<profile>
```

Output is in `target/apidocs/`.

---

## STEP 6: Clean Build (If Needed)

If you need a completely fresh build:

```bash
# Clean all build artifacts
mvn clean

# Then rebuild
mvn clean compile -P<profile>
```

---

## Troubleshooting

### "source/target release X not supported by this compiler"

**Cause:** JDK version does not match the selected Maven profile.

**Fix:**
```bash
# Check your JDK version
java -version

# Use JDK 11 or newer for ALL builds, including the jre8 profile.
# The jre8 profile cross-compiles to Java 8 bytecode using JDK 11+.
# Do NOT install or switch to JDK 8.

# Minimum JDK required -> Maven profile (target artifact):
# JDK 11+  -> -Pjre8   (cross-compiles to Java 8 bytecode)
# JDK 11+  -> -Pjre11
# JDK 17+  -> -Pjre17
# JDK 21+  -> -Pjre21
# JDK 25+  -> -Pjre25
# JDK 26+  -> -Pjre26  (default - no flag needed)
```

### Dependency resolution fails

**Cause:** Cannot reach the Maven artifact feed.

**Fix:**
```bash
# Force update of dependencies
mvn clean compile -U

# If behind a proxy, configure Maven proxy in ~/.m2/settings.xml
```

### "Cannot find symbol" or compilation errors

**Cause:** Usually a code issue or missing dependency.

**Fix:**
```bash
# Clean and rebuild
mvn clean compile -P<profile>

# Check for dependency issues
mvn dependency:tree -P<profile>
```

### Build succeeds but JAR is missing

**Cause:** Used `compile` goal instead of `package`.

**Fix:**
```bash
# Use package goal to create JAR
mvn clean package -DskipTests -P<profile>
```

### Out of memory during build

**Fix:**
```bash
# Increase Maven heap size
# Windows (PowerShell)
$env:MAVEN_OPTS = "-Xmx1024m"

# macOS/Linux
export MAVEN_OPTS="-Xmx1024m"

mvn clean compile -P<profile>
```

---

## Quick Reference

Substitute `<profile>` with the selected JRE profile (e.g., `jre11`).

### One-Liner Build Commands

```bash
# Fast compile check
mvn clean compile -P<profile>

# Package JAR
mvn clean package -DskipTests -P<profile>

# Install locally
mvn clean install -DskipTests -P<profile>

# Full build with tests (requires DB connection).
# Set the connection string as an environment variable beforehand:
#   export mssql_jdbc_test_connection_properties="jdbc:sqlserver://localhost:1433;..."
# Or pass it inline:
mssql_jdbc_test_connection_properties="jdbc:sqlserver://localhost:1433;..." mvn clean verify -P<profile>
```

---

## After Building

Once the build succeeds:

1. **Run tests** -> Use `#run-tests`
2. **Create a PR** with your changes -> Use `#create-pr`
