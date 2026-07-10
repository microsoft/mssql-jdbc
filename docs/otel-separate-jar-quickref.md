# OTel Separate JAR: Quick Reference & Decision Matrix

**Date:** July 4, 2026  
**Purpose:** High-level overview and decision framework  
**Full Details:** See `docs/otel-separate-jar-architecture.md` and `docs/otel-separate-jar-implementation-guide.md`

---

## TL;DR: The Transformation

### Current State (POC)
- OTel code likely in core driver or as optional-but-bundled
- Dependencies on `opentelemetry-api`, `-sdk`, `-exporter-otlp`
- Registration happens automatically or via config

### Target State (Separate JAR)
- **Core driver:** Zero OTel dependencies
- **New JAR:** `mssql-jdbc-otel` with all OTel code + dependencies
- **Discovery:** ServiceLoader SPI (auto-detects at runtime)
- **Developer pom.xml:** Adds optional dependency if wanted

### Benefits

| Aspect | Separate JAR |
|--------|---|
| **Core driver size** | ✅ Lightweight (no OTel) |
| **Opt-in** | ✅ Yes (add to pom.xml if wanted) |
| **Familiar pattern** | ✅ Like `azure-identity` |
| **Zero code changes** | ✅ Works with existing apps |
| **Backward compatible** | ✅ 100% |
| **Works with app's OTel config** | ✅ Reuses GlobalOpenTelemetry |
| **Multi-implementation support** | ✅ SPI allows alternatives |

---

## High-Level Architecture

```
┌─────────────────────────────────────────────────────┐
│ Developer's Application                             │
│                                                     │
│  ┌──────────────────────────────────────────────┐   │
│  │ pom.xml:                                     │   │
│  │  - com.microsoft.sqlserver:mssql-jdbc        │   │
│  │  - com.microsoft.sqlserver:mssql-jdbc-otel   │   │
│  │                      (optional)               │   │
│  └──────────────────────────────────────────────┘   │
└──────────────────────────────────┬──────────────────┘
                                   │
        ┌──────────────────────────┴──────────────────────────┐
        │                                                     │
        ▼                                                     ▼
   ┌─────────────────┐                         ┌─────────────────────────┐
   │ mssql-jdbc-core │                         │  mssql-jdbc-otel JAR    │
   │ (lightweight)   │                         │  (with OTel SDK)        │
   ├─────────────────┤                         ├─────────────────────────┤
   │ - PerformanceLog│                         │ - OpenTelemetryCallback │
   │   Callback      │                         │ - OTelBridgeFactory     │
   │ - SPI interface │◄────discovery link──────│ - OTel instruments      │
   │ - Callback mgmt │                         └─────────────────────────┘
   └────────┬────────┘
            │
            ▼
   ┌──────────────────────┐
   │ TDS Protocol         │
   │ & Connection Mgmt    │
   └──────────────────────┘
```

---

## POC → Separate JAR Transformation Steps

### 1. **Define SPI Interface** (Core Driver)

New file: `src/main/java/com/microsoft/sqlserver/jdbc/spi/TelemetryBridgeFactory.java`

```java
public interface TelemetryBridgeFactory {
    PerformanceLogCallback createCallback(String connectionString, 
                                          Properties properties) throws Exception;
}
```

### 2. **Add ServiceLoader Discovery** (Core Driver)

New file: `src/main/java/com/microsoft/sqlserver/jdbc/TelemetryBridgeLoader.java`

- Uses `ServiceLoader.load(TelemetryBridgeFactory.class)`
- Called during connection initialization
- Silently ignores if no factory found

### 3. **Create OTel Module**

```
mssql-jdbc-otel/
  ├─ pom.xml (declares opentelemetry-api, -sdk, -exporter-otlp)
  └─ src/main/java/com/microsoft/sqlserver/jdbc/otel/
      ├─ OTelTelemetryBridgeFactory.java (implements SPI)
      ├─ OpenTelemetryPerformanceCallback.java (implements callback)
      └─ (other supporting classes)
```

### 4. **ServiceLoader Registration** (OTel Module)

File: `mssql-jdbc-otel/src/main/resources/META-INF/services/com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory`

```
com.microsoft.sqlserver.jdbc.otel.OTelTelemetryBridgeFactory
```

### 5. **Update Build Files**

- Convert to multi-module Maven project
- Core driver: artifact `mssql-jdbc-core`
- OTel bridge: artifact `mssql-jdbc-otel` (separate release)
- Parent POM aggregates both

### 6. **Update Documentation**

- README shows new usage: add `mssql-jdbc-otel` to pom.xml
- New guide: `docs/otel-integration-guide.md`

---

## Code Flow: Connection → OTel Metrics

```
1. App calls: DriverManager.getConnection(url, user, pass)
                                           ▼
2. SQLServerConnection.__init()
   Parses connection string → contains "otelEndpoint=..."
                                           ▼
3. TelemetryBridgeLoader.tryLoadTelemetryBridge()
   Calls: ServiceLoader.load(TelemetryBridgeFactory.class)
                                           ▼
4. OTelTelemetryBridgeFactory.createCallback() [from mssql-jdbc-otel JAR]
   Creates: OpenTelemetryPerformanceCallback instance
                                           ▼
5. SQLServerDriver.registerPerformanceLogCallback()
   Callback now registered, ready to receive events
                                           ▼
6. [On each JDBC operation]
   PerformanceLog.Scope.close() calls callback.publish()
                                           ▼
7. OpenTelemetryPerformanceCallback.publish()
   Records metric to OTel histogram/counter
                                           ▼
8. [Every N seconds or B events]
   OTel SDK batches & sends to OTLP endpoint
```

---

## Decision Matrix: Which Files to Move/Create

| File/Class | Location | Status | Notes |
|---|---|---|---|
| `PerformanceLogCallback` | Core | KEEP | Interface used by both core and OTel |
| `PerformanceLog` | Core | KEEP | Core callback registration infrastructure |
| `PerformanceActivity` | Core | KEEP | Activity enum, used by both |
| `TelemetryBridgeFactory` | Core | **NEW** | SPI interface for discovery |
| `TelemetryBridgeLoader` | Core | **NEW** | Discovery + registration logic |
| `OpenTelemetryPerformanceCallback` | OTel | **MOVE** | From POC → new JAR |
| `OTelTelemetryBridgeFactory` | OTel | **MOVE** | From POC → new JAR |
| Connection string property parsing | OTel | **MOVE** | All OTel properties handled in new JAR |
| OTel SDK dependencies | OTel | **MOVE** | pom.xml: opentelemetry-api, -sdk, -exporter-otlp |

---

## Deployment Model Comparison

### Before (POC Embedded)

```
Developer's pom.xml
└─ mssql-jdbc:13.5.0
   ├─ opentelemetry-api:1.36.0    ← UNWANTED for non-OTel users
   ├─ opentelemetry-sdk:1.36.0    ← Always pulled in
   └─ (other OTel transitive deps)
```

**Problem:** Every user pulls OTel deps even if not using OTel.

### After (Separate JAR)

```
Developer's pom.xml (No OTel wanted)
└─ mssql-jdbc:13.5.0               ← Lightweight, no OTel

Developer's pom.xml (OTel wanted)
├─ mssql-jdbc:13.5.0               ← Core only
└─ mssql-jdbc-otel:13.5.0          ← Optional bridge
   ├─ opentelemetry-api:1.36.0     ← Only if explicitly added
   ├─ opentelemetry-sdk:1.36.0
   └─ (other OTel transitive deps)
```

**Benefit:** OTel deps only pulled when explicitly requested.

---

## Testing Strategy

### Unit Tests (Core Driver)

**Location:** `src/test/java/com/microsoft/sqlserver/jdbc/`

- ✅ Callback interface (unchanged)
- ✅ ServiceLoader discovery with mock factory
- ✅ SPI interface contract

### Unit Tests (OTel Bridge)

**Location:** `mssql-jdbc-otel/src/test/java/.../otel/`

- ✅ Factory instantiation
- ✅ Callback metrics recording
- ✅ OTel SDK initialization
- ✅ Parse connection properties

### Integration Tests

**Location:** `mssql-jdbc-otel/src/test/java/.../integration/`

- ✅ Core driver + OTel JAR together
- ✅ ServiceLoader discovery
- ✅ Callback registration
- ✅ End-to-end: connection → metrics → OTLP endpoint (mock)

### Regression Tests

- ✅ Existing PerformanceLogCallback tests pass
- ✅ Core driver works without OTel JAR
- ✅ No performance degradation

---

## Rollback & Contingency

| Scenario | Action |
|---|---|
| **OTel bridge fails to load** | Driver logs warning, continues without OTel |
| **Remove `mssql-jdbc-otel` from pom.xml** | Core driver unaffected, OTel disabled |
| **Multiple factories on classpath** | First one wins; explicit ordering via SPI if needed |
| **OTLP endpoint unreachable** | OTel SDK handles it; metrics may be dropped but app continues |

---

## Timeline & Effort Estimate

| Phase | Task | Effort | Dependencies |
|---|---|---|---|
| 1 | Create SPI interface + loader | 2–3 days | Core driver only |
| 2 | Refactor OTel code to new module | 2–3 days | Phase 1 complete |
| 3 | Implement factory + callback | 3–4 days | Phase 2 complete |
| 4 | Multi-module Maven setup | 1–2 days | Parallel with Phase 3 |
| 5 | Unit + integration tests | 3–4 days | Phase 3 complete |
| 6 | Documentation + examples | 2–3 days | Phase 5 complete |
| 7 | Code review + refinement | 2–3 days | Phase 6 complete |
| **Total** | | **~16–22 days** | Roughly 3–4 weeks |

---

## Compatibility & Migration Notes

### Backward Compatibility

✅ **100% backward compatible** with existing mssql-jdbc

- No breaking changes to `PerformanceLogCallback` interface
- Connection string properties are new (not breaking)
- Existing code works unchanged

### For Users Currently Using Performance Callback

**Before (POC):**
```java
SQLServerDriver.registerPerformanceLogCallback(myCallback);
```

**After (separate JAR):**
```java
// Option A: Use built-in OTel (no code change)
// Add mssql-jdbc-otel to pom.xml + set otelEndpoint

// Option B: Use custom callback (unchanged)
SQLServerDriver.registerPerformanceLogCallback(myCallback);
```

Both options work simultaneously.

---

## Success Criteria

✅ Core driver has zero OTel dependencies  
✅ OTel bridge is separate JAR (`mssql-jdbc-otel`)  
✅ Auto-discovery via ServiceLoader SPI  
✅ Opt-in: developers add to pom.xml if wanted  
✅ Works with app's existing OTel config  
✅ All existing tests pass  
✅ New tests for OTel functionality  
✅ Documentation updated  
✅ Backward compatible  
✅ Release as two coordinates in Maven Central  

---

## Related Documentation

| Document | Purpose |
|---|---|
| `docs/otel-separate-jar-architecture.md` | Full architectural design |
| `docs/otel-separate-jar-implementation-guide.md` | Step-by-step code implementation |
| `docs/proposal-server-side-statistics-and-otel-export.md` | Original proposal (Solution 1) |
| [README.md](../../README.md) | Update with OTel section |

---

## Questions & Answers

**Q: Why not keep OTel in the core driver?**  
A: Adds unnecessary dependency for non-OTel users. Separate JAR keeps core lightweight.

**Q: What if app doesn't have OTel configured?**  
A: OTel bridge initializes its own SDK on first connection. Works out of the box.

**Q: What if app already uses OTel?**  
A: Bridge detects and reuses `GlobalOpenTelemetry`. No conflicts.

**Q: How does this compare to azure-identity?**  
A: Exactly the same pattern: optional dependency, auto-discovered, developer adds to pom.xml if needed.

**Q: Can users implement their own telemetry bridge?**  
A: Yes! SPI design allows custom `TelemetryBridgeFactory` implementations.

**Q: What about versioning (OTel 1.x → 2.x)?**  
A: OTel bridge declares version range; developers manage transitive deps in their pom.xml.
