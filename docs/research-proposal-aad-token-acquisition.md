# Research Proposal: Optimizing AAD/MSAL Token Acquisition in the MSSQL JDBC Driver

**Status:** Draft — research proposal, intern scoping document
**Owner:** Driver team
**Audience:** Engineering management, intern candidate, driver maintainers, cross-driver (ODBC / .NET) leads
**Related PR:** [microsoft/mssql-jdbc#2562 — *Introduced timeouts for MSAL calls*](https://github.com/microsoft/mssql-jdbc/pull/2562)
**Related discussion threads:**
- [PR #2562 discussion r1906542454](https://github.com/microsoft/mssql-jdbc/pull/2562#discussion_r1906542454)
- [PR #2562 discussion r1906543165](https://github.com/microsoft/mssql-jdbc/pull/2562#discussion_r1906543165)
- [HikariCP #2274 — single connection-adder thread interaction with token waits](https://github.com/brettwooldridge/HikariCP/issues/2274)

---

## 1. Executive Summary

PR [#2562](https://github.com/microsoft/mssql-jdbc/pull/2562) shipped a defensive fix in `12.10.0` to stop connection threads from hanging indefinitely on `CompletableFuture.get()` calls into MSAL4J. That fix introduced two coupled mechanisms in [SQLServerMSAL4JUtils.java](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerMSAL4JUtils.java):

1. A **per-call MSAL future timeout** (`TOKEN_WAIT_DURATION_MS = 20s`, capped by remaining login timeout).
2. A **single process-wide `Semaphore(1)`** with a best-effort `tryAcquire(5s)` whose stated purpose is to serialize the *first* token acquisition so subsequent callers benefit from MSAL's cache instead of all stampeding AAD.

The semaphore design is intentionally coarse — one permit, shared by **every** authentication mode and **every** user in **every** connection — chosen because the team didn't have empirical data to justify a finer-grained scheme. Reviewers explicitly flagged this in the PR discussion as a follow-up worth investigating:

> *"We should explore whether a per-auth-mode or per-user semaphore would yield better throughput without stressing AAD; today's single global gate may serialize unrelated workloads."*
> — paraphrased from PR #2562 review thread

This document scopes a **research project for an intern (estimated 10–12 weeks)** to:

1. Empirically characterize the cost and contention behavior of the current single-semaphore model.
2. Prototype and benchmark **3–5 alternative serialization / coalescing strategies** (per-user, per-auth-mode, per-tenant, request-coalescing/single-flight, async pre-warm).
3. Recommend a data-backed algorithmic enhancement, with reproducible benchmarks and a draft PR for the JDBC driver.
4. Produce a **cross-driver analysis** documenting how Microsoft.Data.SqlClient (.NET) and the MS ODBC Driver for SQL Server solve the same problem, so the JDBC team can converge on the strongest cross-stack approach.

**Expected deliverable:** A technical report + reference implementation + benchmark harness checked into the repo, plus a recommendation memo that the maintainer team can convert into a follow-up PR.

---

## 2. Background

### 2.1 The Walmart-reported symptom (origin of PR #2562)

Walmart reported a production incident where JDBC connection threads in a HikariCP pool occasionally hung indefinitely while acquiring AAD access tokens. Root-cause inspection showed `CompletableFuture<IAuthenticationResult>.get()` calls into MSAL4J blocking with no upper bound when the auth endpoint or its TLS path was degraded. Because HikariCP serializes connection creation through a single "house-keeping" thread (see [HikariCP #2274](https://github.com/brettwooldridge/HikariCP/issues/2274)), a single stuck token call could drain the pool to zero and surface as a full application outage.

### 2.2 What PR #2562 actually changed

The PR added timeouts at two layers:

| Layer | Constant | Value | Behavior |
|---|---|---|---|
| MSAL future wait | `TOKEN_WAIT_DURATION_MS` | 20,000 ms | Cap on `future.get(timeout, ms)`; further capped by `loginTimeout - elapsed`. |
| Serialization gate | `TOKEN_SEM_WAIT_DURATION_MS` | 5,000 ms | `sem.tryAcquire(...)`; if it fails, **the call proceeds anyway** (best-effort serialization). |

Relevant snippet from [SQLServerMSAL4JUtils.java](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerMSAL4JUtils.java#L83):

```java
private static final Semaphore sem = new Semaphore(1);

static SqlAuthenticationToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, String user,
        String password, String authenticationString, int millisecondsRemaining) throws SQLServerException {
    ...
    boolean isSemAcquired = false;
    try {
        // Try to acquire; if we can't within 5s, fall through and try to get the token anyway.
        isSemAcquired = sem.tryAcquire(
            Math.min(millisecondsRemaining, TOKEN_SEM_WAIT_DURATION_MS),
            TimeUnit.MILLISECONDS);
        ...
        final IAuthenticationResult authenticationResult = future.get(
            Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS),
            TimeUnit.MILLISECONDS);
        ...
    } finally {
        if (isSemAcquired) sem.release();
        executorService.shutdown();
    }
}
```

The same pattern is repeated for the four other acquisition methods:

- `getSqlFedAuthToken` — `ActiveDirectoryPassword`
- `getSqlFedAuthTokenPrincipal` — `ActiveDirectoryServicePrincipal` (client secret)
- `getSqlFedAuthTokenPrincipalCertificate` — `ActiveDirectoryServicePrincipalCertificate`
- `getSqlFedAuthTokenIntegrated` — `ActiveDirectoryIntegrated`
- `getSqlFedAuthTokenInteractive` — `ActiveDirectoryInteractive`

All five share the **same one-permit semaphore**.

### 2.3 The token cache that the semaphore is trying to protect

`SQLServerMSAL4JUtils` maintains a static `TokenCacheMap` whose key is a SHA-hash of `{stsurl, user, secret}`, mapping to a `PersistentTokenCacheAccessAspect`. The aspect plugs into MSAL4J's per-`ClientApplication` cache. When the cache is warm, MSAL serves a token from memory (microseconds). When cold, MSAL performs an HTTPS round-trip to the AAD `/token` endpoint (typically 80–400 ms, sometimes seconds under stress).

Two important implementation details:

1. **A fresh `PublicClientApplication` / `ConfidentialClientApplication` is built on every call.** MSAL's cache is keyed *within* the application instance; the JDBC driver re-attaches it via `setTokenCacheAccessAspect(...)`. This means cache *lookup* is essentially `O(map.get + MSAL hydrate)` rather than a free in-memory hit.
2. **Cache key includes the password / secret.** A credential rotation (password change) correctly invalidates the cached aspect, but it also means there is no sharing across users — each `(user, secret)` pair gets its own cache.

### 2.4 Why a single global semaphore is suboptimal

A single `Semaphore(1)` means **completely unrelated** token acquisitions serialize against each other:

| Caller A | Caller B | Should serialize? | Today's behavior |
|---|---|---|---|
| user `alice@tenant-X` (ActiveDirectoryPassword) | user `alice@tenant-X` (same) | **Yes** — one warms the cache for the other | Serializes (good) |
| user `alice@tenant-X` | user `bob@tenant-X` | Maybe — different cache keys, same tenant | Serializes (questionable) |
| user `alice@tenant-X` | SPN `svc-prod@tenant-Y` | **No** — totally unrelated | Serializes (bad) |
| Interactive popup for user `carol` | Background service-principal refresh | **No** — interactive can take 30s+ of human time | Serializes (very bad) |

The interactive case is the worst: if a developer leaves a browser-based AAD consent screen open, every other thread in the JVM that needs a token waits 5 seconds and then stampedes anyway — undermining both responsiveness and the anti-stampede intent.

### 2.5 What we'd ideally have

The token-acquisition layer has three (sometimes competing) goals:

1. **Latency** — return a valid token to the connection thread as fast as possible.
2. **Cache-friendliness** — coalesce concurrent requests for the same `(authority, credential, scope)` so the first one populates the cache and the rest are served from memory.
3. **AAD politeness** — never burst more than necessary against AAD's `/token` endpoint (which has documented per-tenant throttling limits and can degrade under sustained load).

The current design optimizes (3) at the cost of (1) and (2) in a coarse way. A finer-grained design can plausibly improve all three — but this is *not* obvious without measurement, which is the heart of this research project.

---

## 3. Problem Statement

> **How should the JDBC driver gate concurrent AAD token acquisitions so that, in the presence of MSAL's local cache, it (a) minimizes connection establishment latency under realistic concurrency, (b) preserves cache-stampede protection on cold caches, and (c) stays within AAD's per-tenant throttling envelope — without penalizing unrelated authentications?**

Concretely, four interrelated sub-questions:

- **Q1 (Granularity).** What is the right key for the serialization gate? Options include: global, per-auth-mode, per-authority/tenant, per-credential (`{authority, user, secret-hash}`), per-scope.
- **Q2 (Mechanism).** Is a `Semaphore(1)` the right primitive? Or should we use **single-flight / request coalescing** (one in-flight `CompletableFuture` shared by waiters), an **async background refresh** thread, or a **token-bucket rate limiter** per key?
- **Q3 (Lifecycle).** Should token acquisition be on the critical path of `Connection.open()` at all? Could a background warmer (kicked off at driver init, on first connection, or proactively refreshing before expiry) move latency off the user's request?
- **Q4 (Timeout values).** Are the two magic numbers shipped in PR #2562 — `TOKEN_WAIT_DURATION_MS = 20s` (MSAL future cap) and `TOKEN_SEM_WAIT_DURATION_MS = 5s` (semaphore wait) — actually the right values? They were chosen by intuition under time pressure, not by measurement. The wrong values produce two distinct failure modes: **too short** ⇒ spurious timeouts on a healthy-but-slow AAD, with no chance for cache warming to amortize; **too long** ⇒ the hang the PR was trying to fix is barely mitigated, and a stuck leader still blocks waiters far longer than necessary. The right values are also almost certainly *not* the same across auth modes (interactive vs. service-principal), tenants, or network environments.

---

## 4. Investigative Areas

The intern will explore the following four pillars. Each yields a concrete artifact.

### 4.1 Pillar A — Instrument and characterize the existing system

**Goal:** Build a reproducible benchmark and measure the actual cost of the current semaphore on representative workloads.

Steps:

1. Add **fine-grained logging** (or JFR / OpenTelemetry spans behind a flag) around: (i) `sem.tryAcquire` wait time, (ii) `sem` held duration, (iii) MSAL `future.get` duration, (iv) cache hit/miss, (v) outcome (success / `TimeoutException` / `SQLServerException`).
2. Build a **JMH or custom multi-threaded harness** that simulates connection burst patterns:
   - Burst-N: N threads simultaneously open connections (N ∈ {1, 8, 32, 128, 512}).
   - Mixed: subset uses `ActiveDirectoryPassword`, subset uses `ActiveDirectoryServicePrincipal`, subset uses `ActiveDirectoryIntegrated`.
   - Multi-tenant: same JVM connects to databases in 2–4 different AAD tenants.
   - Pool churn: HikariCP / DBCP with `maxLifetime` set low enough to force re-auth at known cadence.
3. Use a **fault-injection harness** (e.g., Toxiproxy in front of `login.microsoftonline.com`, or a stub MSAL using `IHttpClient`) to model AAD latency profiles: 50ms/p50, 500ms/p99, 30s tail, full hang. This lets the intern run experiments **without** actually hammering AAD.
4. Produce histograms / CDFs for end-to-end `Connection.open()` time, broken down by authentication mode and concurrency level.

**Deliverable A.1:** A reproducible benchmark harness checked into `src/test/java/.../perf/` plus a written characterization report ("baseline.md").

### 4.2 Pillar B — Design and prototype alternative gating strategies

The intern will implement at least three of the following as feature-flagged variants of `SQLServerMSAL4JUtils`:

| Strategy | Sketch | Hypothesis |
|---|---|---|
| **S0: Current** | `Semaphore(1)` global, `tryAcquire(5s)` then proceed anyway. | Baseline. |
| **S1: Per-cache-key semaphore** | `ConcurrentHashMap<String /* hashedSecret */, Semaphore>`, one permit each. | Eliminates cross-credential blocking; preserves stampede protection. |
| **S2: Per-authority semaphore** | Keyed on `fedAuthInfo.stsurl` (≈ tenant). | Coarser than S1 but matches AAD per-tenant throttling. |
| **S3: Single-flight / request coalescing** | `ConcurrentHashMap<String, CompletableFuture<SqlAuthenticationToken>>`. First caller installs the future; concurrent callers `join` the same future. No semaphore needed. | Strictly dominates S1 in theory because waiters get the *result*, not just permission to retry. |
| **S4: Async pre-warm + background refresh** | Schedule token refresh ≈ 80% into the token lifetime on a daemon `ScheduledExecutorService`. New connections almost always hit the warm cache. | Moves latency off the connect path entirely. |
| **S5: Token-bucket rate limiter per authority** | Replaces serialization with explicit AAD-politeness budget (e.g., 10 req/s per tenant). | Decouples concurrency from politeness. |

Cross-cutting concerns the intern must address for every prototype:

- **Correctness under credential rotation** (existing `getHashedSecret` semantics).
- **Cleanup of stale map entries** to prevent unbounded growth (LRU? size cap? TTL?).
- **Cancellation / timeout propagation** so a slow leader doesn't pin waiters past their own login timeout.
- **Behavior on `MsalInteractionRequiredException`** (interactive flow needs special handling — cannot be coalesced across users).
- **Thread-safety review** with `jcstress` or careful reasoning; concurrency bugs here will be production-impacting.

#### 4.2.1 Timeout tuning — a parallel experimental track

The gating strategy and the timeout values are **two orthogonal knobs**. Even if we keep S0 (the current semaphore), the constants `TOKEN_WAIT_DURATION_MS` and `TOKEN_SEM_WAIT_DURATION_MS` deserve their own empirical justification. The intern must run a dedicated sweep:

| Knob | Today | Sweep range | What we're trying to learn |
|---|---|---|---|
| `TOKEN_WAIT_DURATION_MS` (MSAL `future.get` cap) | 20,000 ms | {1s, 2s, 5s, 10s, 20s, 30s, 60s, `loginTimeout`} | At what value does the spurious-timeout rate on a healthy-but-slow AAD become unacceptable? Conversely, how short can it go before we mask the original hang bug? Plot p50/p95/p99 success latency and timeout rate vs. cap. |
| `TOKEN_SEM_WAIT_DURATION_MS` (semaphore wait) | 5,000 ms | {0 (skip-if-busy), 100ms, 500ms, 1s, 2s, 5s, 10s, ∞} | When the leader is slow, how long should waiters wait before barging? Trades p99 latency against AAD request volume / stampede severity. |
| Interaction with `loginTimeout` | `Math.min(remaining, CONST)` | unchanged + alternative: `min(remaining * fraction, CONST)` | Does taking a *fraction* (e.g., 50%) of remaining login time produce better outcomes than `min`, by leaving budget for retry? |
| Per-auth-mode override | one constant for all | distinct values for `Password`, `ServicePrincipal`, `ServicePrincipalCertificate`, `Integrated`, `Interactive` | Interactive should plausibly have a much longer cap (human in the loop, 60–120s) while SPN with cert should be much shorter (no human, no network round-trip variance). |

Experimental protocol for the timeout sweep:

1. Hold the gating strategy constant (start with S0, then re-run for the winning strategy from §4.2).
2. Use the fault-injection layer from Pillar A to drive AAD latency through a deterministic schedule: clean (p50=80ms), degraded (p50=500ms, p99=3s), bad (p50=2s, p99=15s), broken (full hang on 10% of calls).
3. For each `(TOKEN_WAIT_DURATION_MS, TOKEN_SEM_WAIT_DURATION_MS)` cell on the grid, measure:
   - End-to-end `Connection.open()` p50 / p95 / p99 / max.
   - Timeout-induced failure rate.
   - Number of AAD `/token` requests issued per N connections (stampede metric).
   - Cache-hit ratio.
4. Build a **Pareto frontier** (latency vs. failure-rate vs. AAD request rate) and pick the values that dominate the current `(20s, 5s)` point, or document that they're already near-optimal.
5. **Decide whether timeouts should be user-tunable.** Today they are `final static`. Options: (a) keep them hardcoded but pick better defaults; (b) expose them as system properties (`mssql.jdbc.token.maxWaitMs`, etc.) — no public API surface change; (c) expose them as connection-string properties — public surface change, needs approval.

**Deliverable B.3:** A timeout-sweep report (`timeout-tuning.md`) with the Pareto frontier, recommended new defaults, and a recommendation on tunability.

**Deliverable B.1:** Reference implementations of S1/S3/S4 (minimum) as parallel classes (`SQLServerMSAL4JUtilsV2`, etc.) gated behind a system property or a connection string property.

**Deliverable B.2:** Comparative benchmark results — same harness from Pillar A, run against every variant.

### 4.3 Pillar C — Cross-driver analysis

The same problem exists in every SQL Server client that supports Entra ID auth. We want to understand how the sibling drivers solved it before recommending a JDBC change.

The intern will produce a written comparison covering at least:

| Aspect | mssql-jdbc (Java) | Microsoft.Data.SqlClient (.NET) | MS ODBC Driver for SQL Server | Notes |
|---|---|---|---|---|
| MSAL library used | MSAL4J | MSAL.NET | MSAL C++ | |
| Token cache scope | per-`PublicClientApplication`, persisted via `ITokenCacheAccessAspect` | per `PublicClientApplication`; in-memory + optional [`MSAL.NET extensions`](https://github.com/AzureAD/microsoft-authentication-extensions-for-dotnet) | per-process cache | |
| Concurrent acquisition gate | `Semaphore(1)` global (PR #2562) | `ActiveDirectoryAuthenticationProvider` and friends — what does `SqlAuthenticationProvider` do under concurrent requests? | What does the ODBC driver do? | **To research.** |
| Timeout behavior | 20s hard cap + 5s semaphore wait, capped by login timeout | `ConnectionTimeout`-driven | Login Timeout-driven | |
| Background refresh | None | MSAL.NET `WithBackgroundRefresh()` available in some versions | Unclear | |
| Pluggable provider | `SQLServerAccessTokenCallback` (Java) | `SqlAuthenticationProvider` (extensible) | DRIVER token provider callback | |

Sources to consult:

- [microsoft/SqlClient repo](https://github.com/dotnet/SqlClient) — search for `ActiveDirectoryAuthenticationProvider`, `s_pendingAcquireTokens`, semaphore/lock patterns.
- [Microsoft Authentication Library docs](https://learn.microsoft.com/azure/active-directory/develop/msal-acquire-cache-tokens).
- AAD's published throttling guidance: ["Identity service limits"](https://learn.microsoft.com/entra/identity-platform/throttling).
- ODBC driver source is not public; the intern should request a code-walkthrough with the ODBC team or scrape behavior from instrumentation / docs.

**Deliverable C.1:** A side-by-side comparison document (`cross-driver-aad-token-acquisition.md`) with an explicit "what JDBC should adopt / avoid" section.

### 4.4 Pillar D — Recommendation and reference PR

Based on the data from Pillars A–C, the intern proposes one (or a small composition) of the strategies as the recommended path forward, and produces a draft PR that:

1. Replaces the global semaphore with the chosen mechanism.
2. Preserves all current public behavior (no API or connection-string surface changes unless explicitly proposed and approved).
3. Comes with new tests covering the new concurrency contract.
4. Includes the benchmark harness so future regressions are catchable in CI (optional but encouraged).

**Deliverable D.1:** A draft PR (or branch + design doc) the maintainer team can finalize. The recommendation must be backed by data from Pillar A/B and informed by Pillar C.

---

## 5. Hypotheses (to validate or refute)

Stating these explicitly so the research has falsifiable claims, not just exploration:

- **H1.** Under multi-tenant or mixed-auth-mode workloads, the global semaphore measurably increases p95 `Connection.open()` latency vs. a per-credential gate, *with no improvement* in AAD request volume.
- **H2.** Single-flight coalescing (S3) is strictly better than any semaphore variant on cold caches because waiters reuse the in-flight result instead of retrying.
- **H3.** An async background refresh (S4) removes token acquisition from the critical path of *steady-state* connection opens (i.e., after first warm-up), reducing p99 by ≥ the median MSAL round-trip on cache-miss scenarios.
- **H4.** Removing the semaphore entirely (and relying on MSAL4J's own internal locking) does *not* meaningfully increase AAD request volume in practice, because MSAL4J already deduplicates concurrent acquisitions to the same authority+scope+account.
  - This is a critical hypothesis: if true, the simplest correct fix is to **delete the semaphore**. We need empirical evidence either way.
- **H5.** The interactive auth flow (`getSqlFedAuthTokenInteractive`) must be excluded from any global gating, because human-in-the-loop response time dominates and unfairly penalizes unrelated callers.
- **H6.** `TOKEN_WAIT_DURATION_MS = 20s` is too long for non-interactive auth modes under a degraded-but-not-broken AAD: a smaller cap (5–10s) combined with retry/fallback would yield better p99 connection latency without measurably increasing the spurious-failure rate.
- **H7.** `TOKEN_SEM_WAIT_DURATION_MS = 5s` provides little real stampede protection because waiters proceed anyway on timeout: either a much smaller value (≤ 500ms, fail-fast and proceed) or full coalescing (S3) dominates the current 5s point on every workload.

---

## 6. Success Criteria

The project succeeds if the intern delivers:

1. **A reproducible benchmark** that a future engineer can re-run to evaluate any future change to this code path.
2. **Quantitative answers** to each of H1–H7 (accept/reject + supporting graphs).
3. **A recommended algorithm *and a recommended set of timeout defaults*** with a justification rooted in (1) and (2), not in opinion.
4. **A draft PR** implementing the recommendation, with tests, that compiles and passes existing CI.
5. **A cross-driver analysis** that gives the JDBC team confidence the chosen design (and chosen timeout values) is consistent (or intentionally divergent) with .NET and ODBC.

Soft success criteria:
- Findings shareable as a blog post / internal tech talk.
- Recommendations applicable to the .NET / ODBC drivers (the cross-driver memo could trigger follow-up work in those repos).

---

## 7. Estimated Effort and Timeline

**Total effort: 10–12 weeks for a strong intern with prior Java + concurrency experience.** Add 2 weeks of slack if the intern is new to MSAL / AAD.

| Week | Phase | Activity |
|---|---|---|
| 1 | Onboarding | Read PR #2562 + all linked threads. Read MSAL4J docs. Build the driver locally. Run existing AAD tests. |
| 2 | Onboarding | Read `SQLServerMSAL4JUtils`, `PersistentTokenCacheAccessAspect`, `SQLServerConnection.getFedAuthToken`. Document current flow with sequence diagrams. |
| 3–4 | Pillar A | Build the benchmark harness + fault injection layer. Instrument current code. Produce baseline.md. |
| 5–6 | Pillar B | Implement S1 and S3. Run benchmarks. |
| 7 | Pillar B | Implement S4 (background refresh) — harder, requires lifecycle management. **Also run the timeout-tuning sweep (§4.2.1) against S0 and the leading variant.** |
| 8 | Pillar C | Cross-driver analysis (read SqlClient code, talk to ODBC team). **Document what timeout values .NET / ODBC use and why.** |
| 9 | Pillar B | Optional: S2 / S5 if time permits. Otherwise, deepen S3/S4 analysis. |
| 10 | Pillar D | Pick winner. Write recommendation memo. Draft PR. |
| 11 | Pillar D | Reviews, address feedback, polish benchmarks. |
| 12 | Wrap-up | Final report, demo to driver team, knowledge transfer. |

Risks that could extend the timeline:

- **Access to a controllable AAD test tenant** with enough header-room to run burst experiments — needs to be lined up before Week 3. Otherwise fault injection only, which limits realism.
- **ODBC code-walkthrough scheduling** — line up early; their team's availability is the long pole.
- **MSAL4J internal behavior surprises** — e.g., it may already deduplicate concurrent calls, which would change the conclusion of H4 in interesting ways.

---

## 8. Required Background and Skills (Intern Profile)

**Must-have:**
- Strong Java, including `java.util.concurrent` (`CompletableFuture`, `Semaphore`, `ReentrantLock`, `ConcurrentHashMap`).
- Familiarity with JVM performance measurement (JMH, async-profiler, JFR).
- Comfort reading non-trivial production code without hand-holding.

**Nice-to-have:**
- Prior exposure to OAuth 2.0 / OpenID Connect / Entra ID.
- C# reading proficiency (for the SqlClient cross-driver analysis).
- Network fault injection (Toxiproxy, tc/netem) experience.

**Mentorship:**
- A driver maintainer as primary mentor (code review + design check-ins).
- An identity / MSAL expert as secondary mentor for AAD throttling / cache semantics questions.

---

## 9. Out of Scope

To keep the project tractable, the following are explicitly **out of scope** (but should be flagged in the final report as follow-ups if encountered):

- Changing the public driver API or connection-string surface (e.g., adding a new `tokenCacheMode=` property) — *unless* a small, additive change is unavoidable and approved by the maintainer team.
- Replacing MSAL4J with another library.
- Solving HikariCP's single connection-adder thread bottleneck ([brettwooldridge/HikariCP#2274](https://github.com/brettwooldridge/HikariCP/issues/2274)) — that's the *pool's* problem; we just have to make sure our gating doesn't make it worse.
- Server-side AAD throttling behavior characterization (we'll respect documented limits, not reverse-engineer them).
- Token cache **persistence to disk** improvements (separate problem, lives in `PersistentTokenCacheAccessAspect`).

---

## 10. Risks and Mitigations

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Cannot get realistic AAD load without affecting production tenants | High | High | Build robust fault-injection layer in Week 3; keep AAD-against-real-tenant runs to small, scheduled bursts. |
| Intern lacks AAD/MSAL domain context | Medium | Medium | Pair with identity SME in week 1; provide reading list. |
| Findings are inconclusive (no variant clearly wins) | Medium | Medium | Even a "the global semaphore is harmless; H4 confirmed" is a publishable result. Document the conditions under which each variant wins. |
| Concurrency bugs in prototypes ship by accident | Low | High | All prototypes behind a feature flag (off by default). No merge without `jcstress`-style review. |
| Cross-driver analysis blocked on ODBC team availability | Medium | Low | Start that thread in Week 1. If blocked, deliver SqlClient comparison only and call it out. |
| Scope creep into broader connection-pool / `Connection.open()` performance work | Medium | Medium | Hold the line — the deliverable is *token acquisition*, not connection establishment. |

---

## 11. Open Questions for the Reviewing Team

Before kickoff, the driver team should answer:

1. **Is there an AAD test tenant we can use** for benchmark runs (controlled throttling, controlled credentials, low blast radius)?
2. **Who owns the cross-driver memo audience?** Is there appetite for the .NET / ODBC teams to act on the findings, or is this purely informational for JDBC?
3. **Is a connection-string property** (e.g., `tokenAcquisitionStrategy=coalesce|background|legacy`) acceptable as an escape hatch in the recommended PR, or should the new behavior be unconditional?
4. **What is the latency / throughput SLA** we should hold the new design to? (Suggest: no regression for single-thread case; ≥ 2× improvement on the 32-thread mixed-auth case as a target, but settle this with the team.)
5. **Telemetry / logging posture:** How much new logging or metrics emission is acceptable in the token acquisition path? The instrumentation needed for the benchmark in Pillar A should be feature-flagged; the team should decide which (if any) of it stays on by default in shipped builds.

---

## 12. Reference Code Locations

For the intern's quick orientation:

- [`SQLServerMSAL4JUtils.java`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerMSAL4JUtils.java) — the heart of this proposal. All five `getSqlFedAuthToken*` methods + `TokenCacheMap` + the `Semaphore`.
- [`SQLServerConnection.java`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java#L6843) — `getFedAuthToken(...)`, the caller and retry loop. Owns the login-timeout clock.
- [`PersistentTokenCacheAccessAspect.java`](../src/main/java/com/microsoft/sqlserver/jdbc/PersistentTokenCacheAccessAspect.java) — the MSAL cache aspect (in-memory; despite the name, no disk persistence by default).
- [`SQLServerSecurityUtility.java`](../src/main/java/com/microsoft/sqlserver/jdbc/SQLServerSecurityUtility.java) — `getHashedSecret(...)` for cache keys.
- [`KeyVaultTokenCredential.java`](../src/main/java/com/microsoft/sqlserver/jdbc/KeyVaultTokenCredential.java) — separate code path for AKV; out of scope but worth a glance for consistency.

---

## 13. Appendix A — Reading List

1. PR #2562 — full thread, all 13 commits.
2. [MSAL4J — token cache documentation](https://learn.microsoft.com/entra/msal/java/token-cache-serialization).
3. [Entra throttling and service limits](https://learn.microsoft.com/entra/identity-platform/throttling).
4. *Java Concurrency in Practice*, Goetz et al. — Chapter 5 (Building Blocks) and Chapter 14 (Building Custom Synchronizers) — particularly relevant for the single-flight prototype.
5. Caffeine's [`AsyncLoadingCache`](https://github.com/ben-manes/caffeine/wiki/Population#asynchronous-population) — a reference single-flight implementation.
6. [Guava's `Striped`](https://guava.dev/releases/snapshot/api/docs/com/google/common/util/concurrent/Striped.html) — a reference for per-key lock striping if S1 is chosen.

---

## 14. Appendix B — Glossary

- **AAD / Entra ID** — Azure Active Directory / Microsoft Entra ID. The identity provider issuing access tokens.
- **MSAL / MSAL4J** — Microsoft Authentication Library / its Java implementation.
- **STS** — Security Token Service. In this code, the `stsurl` is the authority URL.
- **Authority** — A tenant-scoped endpoint, e.g., `https://login.microsoftonline.com/<tenant-id>`.
- **Scope / SPN** — The resource we're requesting a token for; for SQL it's typically `https://database.windows.net/.default`.
- **Single-flight / request coalescing** — A pattern where N concurrent requesters for the same value share a single in-flight computation rather than each launching their own.
- **Stampede** — When many threads simultaneously miss a cache and overwhelm the backing store.
- **`PublicClientApplication` / `ConfidentialClientApplication`** — MSAL4J client objects representing, respectively, a public app (interactive / device-code / username-password) and a confidential app (service principal with a secret or cert).

---

*End of proposal.*
