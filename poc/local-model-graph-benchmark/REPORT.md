# Local Model Graph Benchmark Report

## Setup

- Repository revision: `b09d40a6077ba9727849248b1e502ac8dca51947` (`v13.4.0`)
- Local model: `Qwen/Qwen2.5-Coder-3B-Instruct`
- Runtime: Transformers 4.49.0, PyTorch 2.6.0 CPU, deterministic greedy decoding
- Hardware: AMD Ryzen 7 PRO 7840U, 23.7 GB RAM, no CUDA
- Conditions: identical source context; graph condition appends matching nodes and edges
- Rubric: five criteria per case, scored `0`, `0.5`, or `1`

## Results: GitHub Issue #2999

A blind follow-up used the `v13.4.0` source and matching graph with issue #2999. The issue body described persistent Managed Identity failures but did not disclose the credential-cache fix. Both conditions used the same deterministic model and a 500-token output limit.

| Metric | Source | Source + graph |
|---|---:|---:|
| Context characters | 9,242 | 13,910 |
| Input tokens | 4,406 | 5,471 |
| Output tokens | 232 | 251 |
| Generation time | 228.4 s | 264.1 s |
| Rubric score | 0.5 / 5 | 1.0 / 5 |

Neither condition discovered that the static `CREDENTIAL_CACHE` retained and reused the failed credential instance. The graph answer named more of the stack-trace call chain, but incorrectly treated `FluxTimeout` as shared state and proposed timeout or retry changes. The accepted fix conditionally evicts the failed credential instance so another thread's healthy replacement is preserved.

## Why It Did Not Perform Better

1. **The relevant source was not retrieved.** Lexical retrieval matched broad issue and stack-trace terms, filling the context with nearby but non-controlling snippets. It did not include the static cache declaration and complete credential lookup/failure path needed to infer persistent reuse.
2. **The graph was too coarse.** It represented types, imports, and inheritance, but not method calls, field reads/writes, lock usage, or exception paths. The root cause depended on exactly those missing relationships.
3. **The graph mostly repeated issue-visible symbols.** Names such as `SQLServerSecurityUtility` and `SQLServerConnection` were already present in the stack trace, so rediscovering them added little diagnostic information.
4. **More context added noise and cost.** The graph condition used 1,065 more input tokens and took 35.7 seconds longer, without adding the controlling cache behavior. For a 3B model, irrelevant relationships can compete with the small amount of useful evidence.
5. **The model guessed after evidence ran out.** Without the cache implementation, it proposed generic timeout, retry, and non-blocking changes rather than reporting that the evidence was insufficient.

## Conclusion

In this blind run, type-level graph context did not materially improve root-cause analysis. The primary limitation was not simply model size: the evidence pipeline omitted the code that controlled the behavior.

The next experiment should add method-level source retrieval, call edges, field-access relationships, and exception-flow context while keeping the same issue, model, and rubric.

## Raw Runs

Raw JSON is intentionally ignored under `results/` because it is generated and may contain machine-specific metadata.

- GitHub issue #2999: `local-20260922-011345.json`