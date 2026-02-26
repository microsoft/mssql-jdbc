# State Machine Test Framework Diagrams

## 1. Execution Flowchart

```mermaid
flowchart TB
    subgraph init["1. INITIALIZATION"]
        A[Create StateMachineTest] --> B[Initialize DataCache<br>Row 0 = empty state map]
        B --> C[Set state variables<br>via dataCache.updateValue]
        C --> D[Add Actions<br>auto-links to DataCache & Random]
    end

    subgraph engine["2. ENGINE SETUP"]
        E[Engine.run sm] --> F[withSeed seed]
        F --> G[withMaxActions n]
        G --> H[withTimeout seconds]
        H --> I[execute]
    end

    subgraph loop["3. EXECUTION LOOP"]
        I --> J{Check limits:<br>count < maxActions?<br>time < timeout?}
        J -->|Yes| K[getValidActions<br>filter where canRun=true]
        K --> L{Valid actions<br>exist?}
        L -->|No| M[Exit: No valid actions]
        L -->|Yes| N[Weighted Random Selection<br>higher weight = more likely]
        N --> O[action.execute]
    end

    subgraph action["4. ACTION LIFECYCLE"]
        O --> P[run]
        P --> Q[validate]
        Q --> R[Log action name]
        R --> S[Print state]
        S --> J
    end

    subgraph result["5. RESULT"]
        J -->|No| T[Create Result]
        M --> T
        T --> U[success: bool<br>actionCount: int<br>durationMs: long<br>seed: long<br>log: List<br>error: Exception]
    end

    subgraph error["ERROR HANDLING"]
        O -->|Exception| V[Capture error]
        V --> T
    end

    subgraph components["CORE COMPONENTS"]
        direction LR
        W[StateMachineTest<br>Container] --- X[DataCache<br>State Storage]
        X --- Y[Action<br>Behavior]
        Y --- Z[StateKey<br>Type-safe Keys]
    end

    style init fill:#e1f5fe
    style engine fill:#f3e5f5
    style loop fill:#fff3e0
    style action fill:#e8f5e9
    style result fill:#fce4ec
    style error fill:#ffebee
    style components fill:#f5f5f5
```

## 2. Class Diagram

```mermaid
classDiagram
    class StateMachineTest {
        -DataCache dataCache
        -List~Action~ actions
        -String name
        -Random random
        +StateMachineTest(name)
        +addAction(Action)
        +getValidActions() List~Action~
        +getDataCache() DataCache
        +getRandom() Random
    }

    class Engine {
        -StateMachineTest sm
        -long seed
        -int maxActions
        -int timeout
        +run(StateMachineTest) Engine
        +withSeed(long) Engine
        +withMaxActions(int) Engine
        +withTimeout(int) Engine
        +execute() Result
    }

    class Action {
        <<abstract>>
        +String name
        +int weight
        #DataCache dataCache
        #StateMachineTest sm
        +canRun()* boolean
        +run()* void
        +validate() void
        +execute() void
        +setState(StateKey, Object)
        +getState(StateKey) Object
        +isState(StateKey) boolean
        +getStateInt(StateKey) int
        +getRandom() Random
    }

    class DataCache {
        -List~Map~ rows
        +addRow(Map)
        +getRow(int) Map
        +getValue(int, String) Object
        +updateValue(int, String, Object)
        +getRowCount() int
    }

    class StateKey {
        <<interface>>
        +key() String
    }

    class Result {
        +boolean success
        +int actionCount
        +long durationMs
        +long seed
        +List~String~ log
        +Exception error
    }

    StateMachineTest "1" *-- "1" DataCache : owns
    StateMachineTest "1" *-- "*" Action : contains
    Engine "1" --> "1" StateMachineTest : executes
    Engine "1" ..> "1" Result : produces
    Action --> DataCache : reads/writes
    Action --> StateMachineTest : accesses Random
    Action ..> StateKey : uses for keys
```

## 3. Sequence Diagram

```mermaid
sequenceDiagram
    participant Test as Test Class
    participant SM as StateMachineTest
    participant DC as DataCache
    participant E as Engine
    participant A as Action

    Note over Test,A: SETUP PHASE
    Test->>SM: new StateMachineTest("name")
    SM->>DC: new DataCache()
    SM->>DC: addRow(empty map) [Row 0 = state]
    
    Test->>SM: getDataCache()
    Test->>DC: updateValue(0, key, initialValue)
    
    loop For each Action
        Test->>SM: addAction(action)
        SM->>A: auto-link dataCache
        SM->>A: auto-link sm (for Random)
    end

    Note over Test,A: EXECUTION PHASE
    Test->>E: Engine.run(sm)
    Test->>E: .withSeed(seed)
    Test->>E: .withMaxActions(n)
    Test->>E: .withTimeout(s)
    Test->>E: .execute()
    
    E->>SM: setRandom(seededRandom)
    
    loop While count < max && time < timeout
        E->>SM: getValidActions()
        SM->>A: canRun()? [for each action]
        A->>DC: check state via getValue()
        A-->>SM: true/false
        SM-->>E: List of valid actions
        
        alt No valid actions
            E-->>Test: Result(done)
        else Has valid actions
            E->>E: Weighted random selection
            E->>A: execute()
            A->>A: run() [JDBC operations]
            A->>DC: updateValue() [state changes]
            A->>A: validate() [assertions]
            A-->>E: success or exception
        end
    end
    
    E-->>Test: Result(success, count, seed, log, error)
```

## Component Summary

| Component | Purpose |
|-----------|---------|
| **StateMachineTest** | Container that owns DataCache, Actions, and seeded Random |
| **Engine** | Execution loop with weighted random action selection |
| **Action** | Abstract base with lifecycle: `canRun()` → `run()` → `validate()` |
| **DataCache** | Row 0 = state variables, Rows 1+ = test data |
| **StateKey** | Interface for type-safe state keys (implement as enum) |
| **Result** | Immutable execution result with seed for reproducibility |

## Design Patterns

- **Template Method**: Action's `execute()` calls `run()` then `validate()`
- **Builder**: Engine's fluent API (`withSeed()`, `withMaxActions()`, `withTimeout()`)
- **Strategy**: Actions define their own `canRun()` preconditions and `run()` behavior
- **Weighted Random**: Higher weight actions are more likely to be selected
