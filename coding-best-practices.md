# Coding Best Practices

This document describes some typical programming pitfalls and best practices
related to Java and JDBC. It will grow and change as we encounter new situations
and the codebase evolves.

## Correctness & Business Logic

- Validate if the code adheres to what it is supposed to do. Compare the
  implementation with the specification and ask questions, to get clarity.

## Memory / Resource Management

- Use try-with-resources for managing ResultSet, Statement, Connection, streams, sockets, file handles.  
- Avoid unnecessary object creation in hot paths (tight loops, parsing routines). 
- Reuse preallocated buffers if available (e.g., for TDS packets).
- Can memory usage be optimized? Is pooling or reuse possible?

## Error Handling & Reliability  

- Catch specific exception types (SQLTimeoutException, IOException, etc.).  
- Wrap exceptions using SQLServerException.makeFromDriverError(...) or similar APIs. 
- Ensure logs are useful and include information such as connection ID or any state transitions. 
- Avoid catching the base Exception unless absolutely required.

## Async, Concurrency & Thread Safety (if applicable)

- Synchronize access to shared mutable state.
- For background threads, handle interruption and termination gracefully.
- Avoid deadlocks by keeping lock hierarchy consistent.
- Are all shared variables properly synchronized or volatile ?
- Are ExecutorServices or thread pools properly shut down?

## Backward Compatibility

- Verify unit tests and integration tests haven’t regressed (especially server compatibility tests).
- Preserve public interfaces and behaviors unless part of a breaking release plan.
- Annotate deprecated methods if replacing functionality.
- Are any existing APIs modified or removed? If yes, is this justified and documented?
- Could this change affect driver users on older SQL Server versions or JDBC clients?
- Are test expectations changed in a way that signals a behavior shift?

## Security Considerations

- Never log passwords, secrets, or connection strings with credentials.
- Validate inputs to avoid SQL injection, even on metadata calls.
- Are there any user inputs going into SQL or shell commands directly?
- Are secrets being logged or exposed in stack traces? 
- Are TLS/certificate settings handled safely and explicitly?
- Are we sending unbounded data streams to server prior to authentication e.g. in feature extensions?

## Performance & Scalability  

- Avoid blocking operations on performance-critical paths.
- Profile memory allocations or TDS I/O if large buffers are introduced.
- Use lazy loading for large metadata or result sets.
- Will this impact startup, connection, or execution latency? 
- Could this increase memory usage, thread contention, or GC pressure?
- Are any caches or pools growing unbounded?
- For major features or large PRs, always run the internal performance benchmarks or performance 
  testsuite to determine if the new changes are causing any performance degradation.

## Observability (Logging / Tracing / Metrics)  

- Use existing logging framework (java.util.logging) for debug/trace with appropriate logging level.
- Include connection/session ID when logging.
- Ensure log messages are actionable and contextual.
- Is the new code adequately traceable via logs?
- Are any logs too verbose or leaking user data?

## Unit Tests / Integration

- Have you added unit tests and integration tests?
- Unit tests should not depend on external resources.  Code under unit test
  should permit mocked resources to be provided/injected.
- Avoid tests that rely on timing.


## Configuration & Feature Flags  

- Is the change considered a breaking change? If breaking change is not
  avoidable, has a Configuration/Feature flag been introduced for customers to
  revert to old behavior?

## Code Coverage expectations

- Does the new code have sufficient test coverage? 
- Are the core paths exercised (including error conditions and retries)? 
- If the code is untestable, is it marked as such and is there a reason (e.g., hard dependency on native SQL Server behavior)?

## Pipeline runs

- Is the CI passing? If not, then have you looked at the failures, and found the
  cause for failures?