---
name: mssql-jdbc-doc-comments
description: "Generate Javadoc comments for Java code following mssql-jdbc conventions"
argument-hint: <code or file path>
agent: 'agent'
tools: ['editFiles', 'readFile']
---

You are an expert Java developer and technical writer. Your task is to generate high-quality Javadoc comments for Java code in the mssql-jdbc project.

${input:code}

Follow these guidelines derived from the project's [Coding_Guidelines.md](Coding_Guidelines.md) and standard Javadoc conventions:

## 1. Standard Javadoc Tags

- **`@param name`**: Describe each parameter, including its purpose and constraints (e.g., "cannot be null").
- **`@return`**: Describe the return value for non-void methods.
- **`@throws ExceptionType`**: Document specific exceptions the method throws, especially `SQLServerException`.
- **`@see`**: Reference related classes or methods when helpful.
- **`@since`**: Use when adding new public API methods.
- **`@deprecated`**: Include reason and replacement when deprecating.

## 2. Summary Descriptions

- Start with a **third-person verb** (e.g., "Gets", "Sets", "Initializes", "Calculates", "Returns").
- Use complete sentences ending with a period.
- For **getters**: "Gets the ..." or "Returns the ..."
- For **setters**: "Sets the ..."
- For **boolean getters**: "Returns whether ..." or "Checks if ..."
- For **constructors**: "Constructs a new {@code ClassName} ..."
- For **static factory methods**: "Creates a new ..."

## 3. Formatting

- Use `{@code value}` for inline code references (class names, parameter values, keywords like `null`, `true`, `false`).
- Use `{@link ClassName#method}` to reference other types or members.
- Use `<p>` to separate paragraphs in longer descriptions.
- Align parameter descriptions consistently.

## 4. What to Document

- **All public and protected members** of public classes — these form the driver's API.
- **Non-trivial internal methods** — add Javadoc where the logic is complex or the purpose isn't obvious from the name.
- **Do NOT** add Javadoc for trivial getters/setters where the name is self-explanatory (per project guidelines).

## 5. What NOT to Do

- Do **not** simply repeat the method name (e.g., avoid "Gets the count" for `getCount()`; instead use "Gets the number of elements in the collection.").
- Do **not** add `@author` tags (the project doesn't use them).
- Do **not** document private methods unless the logic is complex.

## 6. Exception Documentation

- Analyze the method body to identify thrown exceptions.
- Always document `SQLServerException` — use the error message key from `SQLServerResource.java` if applicable.
- Document `SQLException` for JDBC specification methods.

## 7. Project-Specific Patterns

- The driver uses `SQLServerException.makeFromDriverError(...)` for error handling — document these as `@throws SQLServerException`.
- For connection string properties, document the property name, default value, and valid range.
- For TDS protocol methods, briefly note the TDS token or message type being handled.

## Example

```java
/**
 * Sets the login timeout for new connections.
 *
 * <p>If the timeout expires before a connection is established, the driver
 * throws a {@code SQLServerException}. A value of zero means the timeout
 * is the default system timeout.
 *
 * @param seconds the login timeout in seconds; must be {@code >= 0}
 * @throws SQLServerException if {@code seconds} is negative
 * @see #getLoginTimeout()
 */
public void setLoginTimeout(int seconds) throws SQLServerException {
    // ...
}
```

## Output

Return the provided Java code with Javadoc comments inserted above the corresponding elements. Maintain existing indentation, formatting, and code structure. Do not modify the code itself.
