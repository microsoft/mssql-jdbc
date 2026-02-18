# State Machine Testing Framework

> **Audience:** PMs, Tech Leads, and Driver Team Engineers (Java, C#, C++, Python, Rust)  
> **Status:** Active — first implemented in MSSQL-JDBC; designed for adoption across all driver teams  

---

## 1. Background — Why This Framework Exists

### The Problem with Traditional Tests

Traditional JDBC driver tests look like this:

```
Test 1:  open → insert → commit → close          ✅ passes
Test 2:  open → insert → rollback → close         ✅ passes
Test 3:  open → insert → commit → insert → close  ✅ passes
```

Each test follows a **fixed, hand-written path**. Engineers only test the paths they can think of.  
But real applications do unexpected things:

```
open → insert → rollback → insert → commit → rollback → insert → commit → close
```

Nobody writes a test for that sequence — yet it's a perfectly valid usage pattern, and it may crash the driver.

### The Existing FX Framework (KoKoMo MBT)

MSSQL-JDBC already has the **FX Framework** — a Model-Based Testing (MBT) system built on top of the open-source **KoKoMo** library. FX/KoKoMo works well for automated action-path exploration: it defines models (e.g., `fxConnection`), states, and transitions, then uses KoKoMo's engine to randomly walk through valid transitions.

**What FX/KoKoMo does well:**
- Automatic exploration of state transitions
- Built-in model support for connection, statement, and resultset operations
- Proven approach for finding real-world bugs

**What makes FX/KoKoMo challenging:**
| Challenge | Impact |
|-----------|--------|
| **Debugging is hard** — KoKoMo uses internal scheduling, reflection, and annotation-driven execution | When a test fails, stepping through code in a debugger is difficult because the execution flow jumps across framework internals |
| **Reproducing failures is hard** — no built-in seed-based replay | A nightly test fails, but running it again produces a different random sequence; the bug disappears |
| **Framework complexity** — requires understanding KoKoMo's model DSL, annotations, and lifecycle | New team members and other driver teams (C#, C++, Python, Rust) face a steep learning curve |
| **Heavyweight dependency** — KoKoMo is a third-party Java library | Cannot be adopted by non-Java driver teams without rewriting the entire framework |

### The Goal: Simplified, Portable, Debuggable MBT

We wanted to keep the **power** of model-based testing (random exploration of valid state paths) while removing the **complexity** of KoKoMo. The result is this **State Machine Testing Framework** — a lightweight, self-contained system that:

1. **Any engineer can read and understand** in 30 minutes
2. **Any driver team can reimplement** in their language (the entire core is ~200 lines)
3. **Every failure is reproducible** — just save the seed number
4. **Debugging is trivial** — plain classes, no reflection, no annotations, no magic

---

## 2. Approach — How It Works (In Plain Language)

### The Core Idea

Instead of writing 20 fixed test cases, you tell the framework:

> *"Here are the 6 things a user can do with a transaction. Here are the rules for when each thing is valid. Now go run 50 random operations and tell me if anything breaks."*

Each run takes a **different random path** through the state space. Over hundreds of CI runs, the framework explores thousands of unique sequences — far more than any human could write by hand.

### Seed-Based Reproducibility

Every random choice is driven by a **seed number**. The seed is like a recipe:

```
Seed 54321 always produces:  setAutoCommit(false) → insert → commit → rollback → insert → ...
Seed 99999 always produces:  next → previous → first → getString → last → ...
```

When a test fails in CI:
1. The log prints the seed number (e.g., `Seed: 54321`)
2. You paste that seed into the test
3. You get the **exact same sequence**, every time, on any machine
4. You set a breakpoint and step through — the bug reproduces on the first try

This is a significant improvement over FX/KoKoMo, where reproducing a random failure often requires multiple attempts or manual investigation.

### Fuzziness and Chaos Scenarios

The framework introduces **controlled chaos** through two mechanisms:

**1. Weighted Randomness (Fuzz)**  
Actions have different weights — `executeUpdate` (weight 15) is picked 3× more often than `setAutoCommit(false)` (weight 5). This means the framework naturally spends more time in high-risk operations, mimicking real-world usage. But low-weight actions still fire, creating unusual combinations.

**2. Non-Deterministic Valid Action Sets (Chaos)**  
At each step, only actions whose preconditions are satisfied can run. As the state changes, different actions become valid or invalid. The framework doesn't follow a fixed graph — it reacts to whatever state the system is in right now.

For example, after calling `setAutoCommit(false)`, both `commit()` and `rollback()` become valid. After `rollback()`, the data is back to its original state, which means a `SELECT` might return surprising results depending on earlier operations. These chaotic intersections are exactly where bugs hide.

```
Step 1:  [valid: setAutoCommit(false), insert, select]     → picks insert
Step 2:  [valid: setAutoCommit(false), insert, select]     → picks setAutoCommit(false)
Step 3:  [valid: commit, rollback, insert, select, setAC]  → picks rollback
Step 4:  [valid: commit, rollback, insert, select, setAC]  → picks insert
Step 5:  [valid: commit, rollback, insert, select, setAC]  → picks commit
   ...
```

No human would write this exact sequence. But the framework does — and it might find a bug.

### Debuggability — The #1 Design Goal

| Feature | FX / KoKoMo | This Framework |
|---------|--------------|----------------|
| Set a breakpoint in an action | Hard — execution flows through framework internals | Easy — plain `run()` method |
| Step through action selection | Hard — internal scheduler + reflection | Easy — simple `for` loop in Engine |
| Read the action code | Annotations + model DSL | Plain classes with `canRun()` and `run()` |
| Understand execution flow | Requires framework knowledge | Reads like a `while` loop picking random items |
| Reproduce a failure | Re-run and hope for the same path | Paste the seed, get the same path every time |

---

## 3. Architecture — The Five Building Blocks

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              Test Class                                      │
│  (e.g., TransactionStateTest)                                                │
│  - Creates a connection to the real database                                 │
│  - Initializes the state machine with starting state                         │
│  - Registers all actions                                                     │
│  - Calls Engine.run(sm).withSeed(54321).withMaxActions(50).execute()         │
│  - Asserts result.isSuccess()                                                │
└─────────────────────────────────┬───────────────────────────────────────────┘
                                  │ creates & configures
                                  ▼
┌──────────────────────────────────────────────────────────────────────────────┐
│                           StateMachineTest                                    │
│  Holds the shared state (a key-value map) and the list of registered actions  │
│  Think of it as the "whiteboard" that all actions read from and write to      │
└────────────────┬──────────────────────────────────┬─────────────────────────┘
     used by     │                                  │  manages
                 ▼                                  ▼
┌─────────────────────────────┐     ┌──────────────────────────────────────────┐
│          Engine              │     │           Action (×N)                     │
│  The execution loop:         │     │  Each action = one user operation         │
│  1. Get valid actions        │     │  - canRun(): "Am I allowed right now?"    │
│  2. Weighted random pick     │     │  - run(): "Do the thing, update state"    │
│  3. Execute the action       │     │  - weight: "How often should I be picked?"│
│  4. Repeat until done        │     │                                           │
└─────────────────────────────┘     └──────────────────────────────────────────┘
                                                    │
                                      reads/writes  │
                                                    ▼
                                    ┌──────────────────────────────┐
                                    │        StateKey (enum)        │
                                    │  Type-safe keys for the map   │
                                    │  e.g., CONN, AUTO_COMMIT,     │
                                    │       CLOSED, TABLE_NAME      │
                                    └──────────────────────────────┘
```

---

## 4. Detailed Component Explanation

### 4.1 Action — The Heart of the Framework

An **Action** represents one thing a user can do. For a Transaction test, the actions are things like `commit()`, `rollback()`, `setAutoCommit(false)`, and `executeUpdate()`.

Every action answers two questions:

| Question | Method | Purpose |
|----------|--------|---------|
| **"Can I run right now?"** | `canRun()` | Checks if the current state makes this action valid. For example, `commit()` can only run when `autoCommit` is `false` and the connection is not `closed`. If `canRun()` returns `false`, the engine skips this action for this step. |
| **"What do I do?"** | `run()` | Performs the actual operation on the real system (e.g., calls `connection.commit()` on a real database) and then updates the state map so the next action sees the correct state. |

Each action also has a **weight** — an integer that controls how often it gets selected. Think of it like putting more tickets in a raffle:

```
executeUpdate:       15 tickets   ← happens most often (the "hot path")
commit:              10 tickets
rollback:            10 tickets
setAutoCommit(false): 5 tickets   ← happens less often
setAutoCommit(true):  3 tickets   ← happens rarely
```

**Why weight matters:** In production, users call `executeUpdate()` far more often than `setAutoCommit()`. By weighting the actions, the framework simulates realistic workloads — while still exercising rare paths.

**Why `canRun()` matters:** Without preconditions, the framework would try to `commit()` when autoCommit is ON (which throws an exception). `canRun()` is the guard that keeps the framework from executing invalid operations. It encodes the **rules** of the API:

```
commit()     → only valid when autoCommit is OFF and connection is OPEN
rollback()   → only valid when autoCommit is OFF and connection is OPEN
getString()  → only valid when cursor is on a valid row and resultset is OPEN
```

These rules are exactly what the API specification says. By encoding them as `canRun()` checks, we create a **living specification** that is also a test.

**Minimal example (pseudocode — applicable to any language):**

```
Action: "commit"
  weight: 10
  canRun:  NOT closed AND NOT autoCommit
  run:     connection.commit()
```

This is language-agnostic. A C# team writes this in C#. A Python team writes it in Python. The logic is the same.

### 4.2 StateKey — Type-Safe State Variables

The state machine tracks variables like "is the connection closed?" or "is autoCommit on?" These variables are stored in a key-value map.

**The problem with string keys:**  
If you use raw strings (`"conn"`, `"closed"`, `"autocommit"`), a typo like `"autoComit"` silently creates a new key and the test passes incorrectly.

**The solution — StateKey enums:**  
Each test domain defines its own enum. The compiler catches typos at build time:

| Domain | StateKey Enum | Values |
|--------|---------------|--------|
| Transaction | `TransactionState` | `CONN`, `AUTO_COMMIT`, `CLOSED`, `TABLE_NAME` |
| ResultSet | `ResultSetState` | `RS`, `CLOSED`, `ON_VALID_ROW` |

Each enum implements the `StateKey` interface (a single method: `key()` → returns a string). This keeps the core framework generic while giving each domain compile-time safety.

**For non-Java teams:** Use whatever your language provides for named constants — C# enums, Python `Enum`, Rust `enum`, C++ `enum class`. The concept is the same: avoid raw strings for state keys.

### 4.3 StateMachineTest — The Shared Whiteboard

`StateMachineTest` is the central coordinator. Think of it as a **whiteboard** in a conference room:

- **Anyone can write on it:** Actions call `sm.setState(AUTO_COMMIT, false)` to update the state
- **Anyone can read from it:** Actions call `sm.isState(CLOSED)` to check the current state
- **It holds the list of actions:** All registered actions live here
- **It provides the seeded random:** So every random choice is reproducible

Key methods:

| Method | What it does | Example |
|--------|-------------|---------|
| `setState(key, value)` | Write a value to the whiteboard | `sm.setState(CLOSED, true)` |
| `getStateValue(key)` | Read an object from the whiteboard | `sm.getStateValue(CONN)` → returns the `Connection` object |
| `isState(key)` | Read a boolean from the whiteboard | `sm.isState(AUTO_COMMIT)` → `true` or `false` |
| `getStateInt(key)` | Read an integer from the whiteboard | `sm.getStateInt(ROW_POSITION)` → `3` |
| `addAction(action)` | Register an action | `sm.addAction(new CommitAction(sm))` |
| `getValidActions()` | Get all actions whose `canRun()` returns true right now | Used internally by the Engine |

### 4.4 Engine — The Execution Loop

The Engine is the simplest part. In plain language, it does this:

```
1.  Set up the random number generator with the given seed
2.  REPEAT up to maxActions times (or until timeout):
      a.  Ask the state machine: "Which actions can run right now?"
      b.  If no actions can run → stop
      c.  Pick one action randomly (weighted by action weights)
      d.  Execute that action (which updates the state)
      e.  Log which action was executed and what the state looks like now
3.  If any action threw an exception → report failure
4.  Return a Result with: success/fail, action count, seed, duration, log
```

The entire Engine is a single `while` loop. There is no scheduler, no thread pool, no reflection, no annotation processing. You can set a breakpoint on any line and step through.

**Sample console output from a real run:**

```
RealTransaction | Seed:54321 | Max:50 | Timeout:30s
[  1] setAutoCommit(false) | {conn=..., autoCommit=false, closed=false, tableName=SM_Transaction_Test}
[  2] executeUpdate         | {conn=..., autoCommit=false, closed=false, tableName=SM_Transaction_Test}
[  3] rollback              | {conn=..., autoCommit=false, closed=false, tableName=SM_Transaction_Test}
[  4] executeUpdate         | {conn=..., autoCommit=false, closed=false, tableName=SM_Transaction_Test}
[  5] commit                | {conn=..., autoCommit=false, closed=false, tableName=SM_Transaction_Test}
  ...
[ 50] executeQuery          | {conn=..., autoCommit=false, closed=false, tableName=SM_Transaction_Test}
Done: 50 actions in 342ms
```

Every step is visible. Every state is printed. If step 37 fails, you set `withSeed(54321)` and put a breakpoint at step 37's action.

### 4.5 Result — The Test Outcome

After the Engine finishes, it returns a `Result` object containing everything you need:

| Field | Type | What it tells you |
|-------|------|-------------------|
| `success` | boolean | Did all actions complete without exceptions? |
| `actionCount` | int | How many actions were executed (may be less than max if engine stopped early) |
| `seed` | long | The seed used — **save this to reproduce the exact run** |
| `durationMs` | long | Wall-clock time for the entire run |
| `log` | list of strings | Ordered list of action names that were executed |
| `error` | exception | The exception that caused the failure (null if success) |

---

## 5. End-to-End Example — How a Test Works

Here is how the Transaction test runs, step by step:

```
1. JUnit calls TransactionStateTest.testRealDatabaseTransaction()

2. Test opens a real connection to SQL Server

3. Test creates a state machine and sets the initial state:
      CONN        = the real Connection object
      AUTO_COMMIT = true    (SQL Server default)
      CLOSED      = false   (connection is open)
      TABLE_NAME  = "SM_Transaction_Test"

4. Test registers 6 actions:
      setAutoCommit(false)   weight=5    canRun: not closed AND autoCommit is ON
      setAutoCommit(true)    weight=3    canRun: not closed AND autoCommit is OFF
      commit                 weight=10   canRun: not closed AND autoCommit is OFF
      rollback               weight=10   canRun: not closed AND autoCommit is OFF
      executeUpdate          weight=15   canRun: not closed
      executeQuery           weight=10   canRun: not closed

5. Test calls Engine.run(sm).withSeed(54321).withMaxActions(50).execute()

6. Engine loop:
      Step 1:  autoCommit=true  → valid: [setAutoCommit(false), insert, select]
               picks setAutoCommit(false) → autoCommit becomes false
      Step 2:  autoCommit=false → valid: [setAutoCommit(true), commit, rollback, insert, select]
               picks insert → runs INSERT on real database
      Step 3:  picks rollback → undoes the insert
      Step 4:  picks insert → runs INSERT again
      Step 5:  picks commit → makes the insert permanent
      ...
      Step 50: done

7. Test asserts result.isSuccess() — all 50 steps completed without exceptions
```

---

## 6. Adopting This Framework in Other Driver Teams

This framework is designed to be **language-agnostic**. The core concepts are:

| Concept | What to implement | Complexity |
|---------|-------------------|------------|
| Key-value state map | A dictionary / hashmap | ~10 lines |
| Action base class | Abstract class with `canRun()`, `run()`, `weight` | ~15 lines |
| Engine loop | While loop with weighted random selection | ~40 lines |
| State key enum | Enum or named constants for type safety | ~10 lines |
| Result | Simple data class | ~15 lines |

**Total core framework: ~90 lines in any language.**

### What each team needs to do:

1. **Implement the core** (~90 lines) — one-time effort
2. **Define actions for your domain** — e.g., Transaction, ResultSet, Connection
3. **Write one test method** per domain — sets up state, registers actions, calls Engine
4. **Add to CI** — runs automatically, different seed each night

### Portability by language:

| Language | Action | StateKey | Engine | Difficulty |
|----------|--------|----------|--------|------------|
| **C#** | Abstract class | `enum` | While loop + `System.Random` | Easy |
| **Python** | ABC / Protocol | `Enum` | While loop + `random.Random` | Easy |
| **C++** | Virtual base class | `enum class` | While loop + `std::mt19937` | Medium |
| **Rust** | Trait | `enum` | Loop + `rand::Rng` | Medium |

---

## 7. Comparison: Before and After

| Aspect | Hand-Written Tests | FX / KoKoMo | This Framework |
|--------|--------------------|--------------|----------------|
| Paths tested | 5-10 fixed paths | Hundreds (random) | Hundreds (random) |
| Reproducing a failure | Always reproducible | Hard — no seed replay | Easy — paste seed |
| Debugging | Easy — fixed code | Hard — framework internals | Easy — plain classes |
| Learning curve | None | High — annotations, DSL | Low — 5 classes, ~200 lines |
| Portable to C#/C++/Python/Rust | N/A | No — Java-only library | Yes — concepts are universal |
| Third-party dependency | None | KoKoMo library | None |
| Realistic workload simulation | No — paths are synthetic | Some | Yes — weighted randomness |

---

## 8. Running Tests

```bash
# Run all state machine tests
mvn test -Dgroups=stateMachine -Pjre11

# Run a specific test
mvn test -Dtest=TransactionStateTest -Pjre11

# Run with a specific seed (edit the test file, set withSeed value)
# This reproduces the exact same action sequence every time
```

**Prerequisite:** Set the connection string environment variable:
```
MSSQL_JDBC_TEST_CONNECTION_PROPERTIES=jdbc:sqlserver://SERVER;databaseName=test;userName=...;password=...;
```

---

## 9. Directory Structure

```
statemachinetest/
├── README.md                     ← You are here
├── core/                         ← Framework core (language-agnostic logic)
│   ├── Action.java               # Base class: canRun() + run() + weight
│   ├── Engine.java               # Execution loop: weighted random pick, seed, timeout
│   ├── Result.java               # Outcome: success, actionCount, seed, log, error
│   ├── StateKey.java             # Interface for type-safe enum keys
│   └── StateMachineTest.java     # Shared whiteboard: state map + action registry
├── transaction/                  ← Transaction domain (first test domain)
│   ├── TransactionState.java     # Enum: CONN, AUTO_COMMIT, CLOSED, TABLE_NAME
│   ├── TransactionActions.java   # 6 actions: setAutoCommit, commit, rollback, insert, select
│   └── TransactionStateTest.java # JUnit test: 50 random actions against real SQL Server
└── resultset/                    ← ResultSet domain (second test domain)
    ├── ResultSetState.java       # Enum: RS, CLOSED, ON_VALID_ROW
    ├── ResultSetActions.java     # 6 actions: next, previous, first, last, absolute, getString
    └── ResultSetStateTest.java   # JUnit test: 50 random actions against real SQL Server
```

---

## 10. Future Enhancements

- [ ] **More domains:** Connection lifecycle, PreparedStatement, CallableStatement
- [ ] **Longer runs in nightly CI:** 500+ actions per test, random seed each run
- [ ] **State transition coverage:** Track which state→state transitions have been exercised
- [ ] **Visual state diagram:** Auto-generate a diagram from registered actions and states
- [ ] **Cross-driver adoption:** Port the core to C#, C++, Python, Rust driver teams
