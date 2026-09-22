---
name: code-graph-context
description: "Analyze mssql-jdbc by combining the workspace code graph with every repository Markdown file. Use when asked to understand architecture, trace behavior, investigate a bug, plan a change, review code, or implement work that must follow all project documentation."
---

# Code Graph Context

Use the workspace code graph as the source of truth for current code structure and behavior. Use every Markdown file in the repository as supporting project, domain, process, and policy context.

## Context Collection

1. Find every tracked or visible Markdown file with `rg --files -g '*.md'`, or the equivalent workspace file-search tool when `rg` is unavailable.
2. Exclude this `SKILL.md` from recursive context loading.
3. Read the root guidance first:
   - [`AGENTS.md`](../../../AGENTS.md)
   - [`README.md`](../../../README.md)
   - [`CONTRIBUTING.md`](../../../CONTRIBUTING.md)
   - [`Coding_Guidelines.md`](../../../Coding_Guidelines.md)
   - [`coding-best-practices.md`](../../../coding-best-practices.md)
   - [`review-process.md`](../../../review-process.md)
   - [`SECURITY.md`](../../../SECURITY.md)
   - [`CHANGELOG.md`](../../../CHANGELOG.md)
   - [`CODE_OF_CONDUCT.md`](../../../CODE_OF_CONDUCT.md)
4. Read all Markdown under `.github/`, including Copilot instructions, scoped instructions, prompts, issue templates, pull request templates, and modernization artifacts.
5. Read all Markdown under `docs/` and `src/`, including nested documentation and sample guidance.
6. If additional Markdown files exist outside those locations, read them too. Do not rely on a hard-coded inventory as the repository can evolve.
7. For a large document set, inspect headings and relevant sections first, then fully read the files that govern or explain the requested area. Do not omit a file solely because it appears unrelated before its title or headings are checked.

## Code Graph Workflow

1. Translate the request into concrete symbols, packages, tests, configuration, and observable behavior.
2. Query the workspace semantic index or code graph for the most relevant symbols and implementations.
3. Follow definitions, references, implementations, callers, callees, inheritance, and test usages until the behavior-controlling path is identified.
4. Treat build files and source code as authoritative for the current implementation. Treat documentation as authoritative for intent, constraints, terminology, and workflow unless the code demonstrates that it is stale.
5. Cross-check graph findings against neighboring tests and the Markdown context before drawing conclusions or editing.
6. State discrepancies between code and documentation explicitly. Do not silently choose one when the conflict affects the requested outcome.
7. Keep exploration scoped to the request. Expand one dependency or call-site hop at a time when evidence is incomplete.

## Instruction Precedence

Apply context in this order when guidance conflicts:

1. User request and active session instructions
2. `AGENTS.md` files nearest the file being changed
3. `.github/copilot-instructions.md` and matching `.github/instructions/*.instructions.md`
4. Security, contribution, coding, testing, and review documentation
5. Feature documentation, samples, prompts, templates, and historical notes

Call out unresolved conflicts before making a consequential change.

## mssql-jdbc Requirements

- Preserve JDBC 4.2/4.3 behavior and public API compatibility unless the request explicitly changes them.
- Keep code compatible with the supported JRE profiles and with Windows, Linux, and macOS.
- Trace TDS changes through packet I/O, token parsing, stream handlers, and affected tests.
- Use localized `SQLServerResource` keys and established `SQLServerException` patterns.
- Guard detailed logging and never expose credentials, tokens, or sensitive connection data.
- Write or update focused JUnit 5 tests; distinguish isolated tests from tests requiring SQL Server configuration.
- Format changed Java code with `mssql-jdbc_formatter.xml` and validate the narrowest relevant Maven profile first.

## Expected Output

For analysis, plans, reviews, or implementation reports, provide:

1. The controlling code path and important symbol relationships discovered from the graph
2. The Markdown guidance that materially constrained the result
3. Any code/documentation discrepancies or assumptions
4. The focused validation performed, or the reason validation could not run

Use workspace-relative file links when citing evidence.