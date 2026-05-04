# AI Agent Guidelines for Microsoft JDBC Driver for SQL Server

This document provides guidance for AI coding agents working with the mssql-jdbc repository.

## Quick Start

### Essential Context Files

Before making changes, agents should be aware of:

| File | Purpose |
|------|---------|
| [README.md](README.md) | Project overview |
| [CONTRIBUTING.md](CONTRIBUTING.md) | Contribution guidelines |
| [Coding_Guidelines.md](Coding_Guidelines.md) | Java coding standards |
| [coding-best-practices.md](coding-best-practices.md) | Engineering best practices |
| [review-process.md](review-process.md) | PR review requirements |
| [.github/copilot-instructions.md](.github/copilot-instructions.md) | Copilot-specific instructions |

### Detailed Technical Instructions

The `.github/instructions/` directory contains comprehensive guides:

| Guide | Coverage |
|-------|----------|
| [architecture.instructions.md](.github/instructions/architecture.instructions.md) | Project structure, package layout, layer architecture |
| [patterns.instructions.md](.github/instructions/patterns.instructions.md) | Exception handling, logging, resource management, testing patterns |
| [glossary.instructions.md](.github/instructions/glossary.instructions.md) | Terms, acronyms, authentication modes, data types |
| [performance-metrics.instructions.md](.github/instructions/performance-metrics.instructions.md) | Performance instrumentation, connection and statement metrics |
| [state-machine-testing.instructions.md](.github/instructions/state-machine-testing.instructions.md) | Model-based testing framework, seed-based reproducibility |

## Workflow Prompts

This repository provides reusable prompts in `.github/prompts/` for common maintainer workflows. Use these to guide agents through multi-step operations.

| Prompt | Purpose |
|--------|---------|
| [setup-dev-env.prompt.md](.github/prompts/setup-dev-env.prompt.md) | Set up JDK, Maven, and development environment |
| [build.prompt.md](.github/prompts/build.prompt.md) | Build the driver with Maven across JRE profiles |
| [run-tests.prompt.md](.github/prompts/run-tests.prompt.md) | Run unit, integration, or BVT tests |
| [fix-bug.prompt.md](.github/prompts/fix-bug.prompt.md) | Diagnose and fix a bug with tests and documentation |
| [implement-feature.prompt.md](.github/prompts/implement-feature.prompt.md) | Plan and implement a new feature end-to-end |
| [code-review.prompt.md](.github/prompts/code-review.prompt.md) | AI-assisted code review for a pull request |
| [perf-optimization.prompt.md](.github/prompts/perf-optimization.prompt.md) | Investigate and implement performance improvements |
| [create-pr.prompt.md](.github/prompts/create-pr.prompt.md) | Create well-structured pull requests |

## Core Principles

1. **Cross-Platform Compatibility**: Code must work on Windows, Linux, and macOS
2. **Multi-Profile Compilation**: Code must compile across all JRE profiles (`jre8` through `jre26`)
3. **Backward Compatibility**: No breaking changes without proper deprecation and documentation
4. **Test-First Development**: All changes require tests — write failing tests before implementing fixes
5. **Security by Default**: Secure defaults, no credential logging, parameterized queries
6. **Protocol Compliance**: Follow MS-TDS specifications
7. **Performance Awareness**: Avoid allocations on hot paths, reuse buffers, guard log messages
8. **JDBC Specification Compliance**: Follow JDBC 4.2/4.3 specification for standard interfaces

## Common Tasks

### Bug Fix Workflow

1. Understand the issue from the bug report
2. Locate relevant code in `src/main/java/com/microsoft/sqlserver/jdbc/`
3. Write a failing test that reproduces the issue
4. Implement the fix using patterns from `.github/instructions/patterns.instructions.md`
5. Ensure all tests pass across JRE profiles
6. Update Javadoc if behavior changes

### Feature Implementation

1. Review the feature specification or issue
2. Plan the implementation (see `implement-feature` prompt)
3. Implement with tests (unit + integration)
4. Document new public APIs with Javadoc
5. Update CHANGELOG.md

### Adding Connection String Properties

1. Add the property constant to `SQLServerDriverStringProperty`, `SQLServerDriverBooleanProperty`, or `SQLServerDriverIntProperty`
2. Add getter/setter to `SQLServerDataSource.java`
3. Add parsing in `SQLServerConnection.java`
4. Default to a backward-compatible value
5. Add tests for the new property
6. Document in Javadoc

### TDS Protocol Changes

1. Reference the MS-TDS specification for the protocol extension
2. Add new token/flag constants to the relevant TDS classes
3. Implement parsing/writing in `IOBuffer.java`, `tdsparser.java`, and `Stream*.java` files
4. Test against multiple SQL Server versions (2012+, Azure SQL)
5. Consider backward compatibility

### Authentication Changes

1. Update `SqlAuthentication` enum if adding a new authentication method
2. Implement token acquisition in the appropriate auth class (`SQLServerMSAL4JUtils.java`, `KerbAuthentication.java`, etc.)
3. Ensure federated auth flow works if applicable
4. Test with the relevant authentication infrastructure

### Performance Optimization

1. Profile the issue using benchmarks or tracing
2. Identify allocation hotspots (see `perf-optimization` prompt)
3. Apply patterns: buffer reuse, lazy initialization, object caching, lock narrowing
4. Verify no regressions with existing tests
5. Run internal performance benchmarks for major changes

## External Resources

### Key Documentation Links

- [Microsoft JDBC Driver on Microsoft Learn](https://learn.microsoft.com/sql/connect/jdbc/microsoft-jdbc-driver-for-sql-server)
- [MS-TDS Protocol Specification](https://learn.microsoft.com/openspecs/windows_protocols/ms-tds)
- [SQL Server Documentation](https://learn.microsoft.com/sql/sql-server/)
- [JDBC API Specification](https://docs.oracle.com/javase/8/docs/technotes/guides/jdbc/)

## Repository Policies

See the root directory for:

- [Coding_Guidelines.md](Coding_Guidelines.md) — Java coding standards and formatting rules
- [coding-best-practices.md](coding-best-practices.md) — Engineering best practices
- [review-process.md](review-process.md) — PR review requirements and responsibilities

## Getting Help

- Check existing tests for usage patterns
- Reference similar implementations in the codebase
- Consult the Microsoft Docs for API behavior specifications
- For protocol questions, refer to MS-TDS open specifications
- For JDBC specification questions, refer to the Oracle JDBC documentation

---

*This document is automatically loaded as context for AI agents working in this repository.*
