---
name: mssql-jdbc-perf-optimization
description: Investigate and implement performance improvements in mssql-jdbc.
argument-hint: <area or issue to optimize, e.g. "IOBuffer allocation reduction" or issue number>
agent: agent
---

Investigate and optimize performance for: "${input:area}".

Performance is critical for a database driver — every allocation, copy, and synchronization on the hot path affects all consumers. Follow this structured approach:

## 1. Identify the Hotspot

- If a GitHub issue is provided, fetch the issue details and any profiling data.
- Determine the code area to optimize in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- Identify whether this is:
  - **Allocation reduction** — reducing GC pressure on hot paths
  - **CPU optimization** — reducing computation or branching
  - **I/O optimization** — reducing network round-trips or buffer copies
  - **Lock contention** — reducing synchronization overhead
  - **Caching** — reusing computed results or objects

## 2. Analyze Current Implementation

- Read the relevant source files to understand the current code path.
- Look for common performance anti-patterns:

  | Anti-pattern | Better Alternative |
  |---|---|
  | `new byte[]` on hot paths | Reuse preallocated buffers or use pooling |
  | Excessive `String` concatenation | Use `StringBuilder` or `String.format` |
  | Repeated object creation in loops | Cache as `static final` or instance field |
  | Stream API (`.stream().filter().map()`) on hot paths | Manual `for` loops |
  | Autoboxing of primitives | Use primitive types directly |
  | Unnecessary `synchronized` blocks | Use more granular locking or `volatile` |
  | Creating `Logger` messages unconditionally | Guard with `if (logger.isLoggable(Level.FINER))` |
  | Allocating config/settings objects per call | Cache as `static final` instances |
  | Reflection-based operations on hot paths | Direct method calls with cached `MethodHandle` |

## 3. Plan the Optimization

Before making changes, document:

- **What** is being optimized (specific class/method)
- **Why** it matters (frequency of execution, measured or estimated impact)
- **How** it will be optimized (which pattern applied)
- **Risk** assessment (could this change behavior? thread safety implications?)

### Common Optimization Patterns in This Codebase

| Pattern | When to Use | Example Area |
|---------|-------------|--------------|
| Buffer reuse | Temporary byte/char arrays | TDS packet buffers in `IOBuffer.java` |
| Lazy initialization | Expensive objects used conditionally | SSL stream setup, enclave providers |
| Object caching | Immutable config or frequently created objects | `ParameterMetaDataCache`, encryption key cache |
| Reduced copying | Avoiding array copies in data paths | Result set column data, bulk copy |
| Lock narrowing | Synchronized blocks wider than necessary | Connection pool, statement cache |
| Batch I/O | Multiple small writes/reads | TDS packet assembly in `IOBuffer.java` |
| Avoid autoboxing | Primitive wrappers in hot loops | Data type conversion in `DDC.java`, `dtv.java` |

## 4. Implement the Optimization

- Make changes in `src/main/java/com/microsoft/sqlserver/jdbc/`.
- Ensure the optimization compiles for ALL JRE profiles (`jre8` through `jre26`).
- Use conditional JDK version checks if newer APIs are needed (e.g., APIs only available in JDK 11+).
- Preserve exact behavioral semantics — the optimization must be invisible to callers.
- Pay special attention to thread safety when caching or sharing instances.
- Follow code patterns in `.github/instructions/patterns.instructions.md`.
- Apply Eclipse formatter `mssql-jdbc_formatter.xml` to changed code.

## 5. Validate

- Verify all existing tests pass — performance changes must not alter behavior:
  ```bash
  mvn clean verify -Pjre11
  ```
- Check for thread safety if shared state is introduced.
- If feasible, create a micro-benchmark using JMH or timed test loops to demonstrate the improvement.
- Review for regressions across JRE profiles.
- For major optimizations, run the internal performance benchmarks to confirm improvement (see `coding-best-practices.md`).

## 6. Write Tests (if needed)

- If the optimization changes internal structure, add unit tests to verify the new code path.
- For cache/pool implementations, add tests for concurrent access.
- For buffer reuse, verify buffers are properly reset between uses (no data leaks).

## 7. Prepare for PR

- Summarize the optimization with before/after analysis (allocations, throughput, or timing if available).
- Provide a checklist:
  - [ ] No behavior changes (optimization is transparent)
  - [ ] Thread-safe if shared state introduced
  - [ ] Compiles for all JRE profiles (`jre8` through `jre26`)
  - [ ] All existing tests pass
  - [ ] No sensitive data exposed through caching
  - [ ] Follows coding guidelines (`Coding_Guidelines.md`)
  - [ ] Performance benchmarks run (for major changes)
