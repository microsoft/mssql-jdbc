---
description: "Build the mssql-jdbc driver with Maven"
name: "mssql-jdbc-build"
agent: 'agent'
---
# Build Prompt for microsoft/mssql-jdbc

You are a development assistant helping build the Microsoft JDBC Driver for SQL Server.

## PREREQUISITES

> This prompt assumes your development environment is already set up.
> If you haven't set up your environment yet, use `#setup-dev-env` first.

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

## STEP 1: Choose Build Type

Ask the developer what they need:

> "What would you like to build?"
> 1. **Quick compile** - Compile only (fastest, no JAR)
> 2. **Package JAR** - Compile + create JAR (skip tests)
> 3. **Full build** - Compile + test + package
> 4. **Install locally** - Build + install to local Maven repo
> 5. **Specific profile** - Build for a specific JRE version

---

## STEP 2: Build the Driver

### Option A: Quick Compile (Default Profile)

```bash
mvn clean compile
```

### Option B: Quick Compile with Specific Profile

```bash
# Choose one:
mvn clean compile -Pjre8
mvn clean compile -Pjre11
mvn clean compile -Pjre17
mvn clean compile -Pjre21
mvn clean compile -Pjre25
mvn clean compile -Pjre26
```

### Option C: Package JAR (Skip Tests)

```bash
mvn clean package -DskipTests
# Or with specific profile:
mvn clean package -DskipTests -Pjre11
```

### Option D: Full Build with Tests

```bash
mvn clean verify -Pjre11
```

> Requires SQL Server connection string. See `#run-tests` for details.

### Option E: Install to Local Maven Repository

```bash
mvn clean install -DskipTests -Pjre11
```

This installs the JAR to `~/.m2/repository/com/microsoft/sqlserver/mssql-jdbc/` for use as a dependency in other projects.

---

## STEP 3: Verify the Build

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

## STEP 4: Generate Javadoc (Optional)

```bash
mvn javadoc:javadoc -Pjre11
```

Output is in `target/apidocs/`.

---

## STEP 5: Clean Build (If Needed)

If you need a completely fresh build:

```bash
# Clean all build artifacts
mvn clean

# Then rebuild
mvn clean compile -Pjre11
```

---

## Troubleshooting

### "source/target release X not supported by this compiler"

**Cause:** JDK version does not match the selected Maven profile.

**Fix:**
```bash
# Check your JDK version
java -version

# Use a profile matching your JDK:
# JDK 8  -> -Pjre8
# JDK 11 -> -Pjre11
# JDK 17 -> -Pjre17
# JDK 21 -> -Pjre21
# JDK 25 -> -Pjre25
# JDK 26 -> -Pjre26 (default - no flag needed)
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
mvn clean compile -Pjre11

# Check for dependency issues
mvn dependency:tree -Pjre11
```

### Build succeeds but JAR is missing

**Cause:** Used `compile` goal instead of `package`.

**Fix:**
```bash
# Use package goal to create JAR
mvn clean package -DskipTests -Pjre11
```

### Out of memory during build

**Fix:**
```bash
# Increase Maven heap size
# Windows (PowerShell)
$env:MAVEN_OPTS = "-Xmx1024m"

# macOS/Linux
export MAVEN_OPTS="-Xmx1024m"

mvn clean compile -Pjre11
```

---

## Quick Reference

### One-Liner Build Commands

```bash
# Fast compile check (default profile)
mvn clean compile

# Package JAR for JRE 11
mvn clean package -DskipTests -Pjre11

# Install locally for JRE 11
mvn clean install -DskipTests -Pjre11

# Full build with tests (requires DB connection)
mvn clean verify -Pjre11 -DmssqlJDBC_URL="jdbc:sqlserver://localhost:1433;..."
```

---

## After Building

Once the build succeeds:

1. **Run tests** -> Use `#run-tests`
2. **Create a PR** with your changes -> Use `#create-pr`
