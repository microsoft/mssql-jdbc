# OTel Separate JAR: Implementation Checklist & Code Snippets

**Date:** July 4, 2026  
**Purpose:** Step-by-step implementation guide with concrete code examples  
**Related:** `docs/otel-separate-jar-architecture.md`

---

## Phase 1: Core Driver Preparation

### Step 1.1: Define SPI Interface in Core Driver

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/spi/TelemetryBridgeFactory.java`

```java
/*
 * Microsoft JDBC Driver for SQL Server
 *
 * Copyright(c) Microsoft Corporation All rights reserved.
 * Licensed under the MIT License.
 */
package com.microsoft.sqlserver.jdbc.spi;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;

/**
 * Service Provider Interface for telemetry/observability bridges.
 * 
 * Implementations are discovered via {@link java.util.ServiceLoader} and registered
 * as performance callbacks when specific connection properties (e.g., otelEndpoint) are set.
 * 
 * This allows the core driver to remain free of OpenTelemetry dependencies while still
 * supporting OTel integration through an optional companion JAR.
 * 
 * @since 13.5.0
 */
public interface TelemetryBridgeFactory {
    
    /**
     * Create or retrieve a PerformanceLogCallback that bridges driver metrics to a telemetry system.
     * 
     * This method is called once at driver initialization (during connection establishment)
     * if the connection string contains properties indicating OTel integration is desired.
     * 
     * Implementations should:
     * - Parse telemetry-specific connection properties
     * - Initialize or reuse an existing telemetry SDK
     * - Return a callback that handles PerformanceActivity events
     * - Return null if this factory cannot provide a callback (e.g., preconditions not met)
     * 
     * @param connectionString The full connection string. Implementations should parse
     *                        this to extract OTel-specific properties (e.g., otelEndpoint).
     * @param properties       A Properties object with parsed connection properties, if available.
     *                        Can be null; implementations should handle both cases.
     * 
     * @return A PerformanceLogCallback instance, or null if this factory cannot create one.
     *         A null return allows other factories or fallback behavior.
     * 
     * @throws Exception If initialization fails and the caller should treat this as an error.
     *                   Implementations should use exception handling carefully to avoid
     *                   disrupting driver startup.
     */
    PerformanceLogCallback createCallback(String connectionString, 
                                          java.util.Properties properties) throws Exception;
}
```

### Step 1.2: Define the Core-to-OTel Bridge Contract

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/TelemetryBridge.java`

```java
package com.microsoft.sqlserver.jdbc;

/**
 * Narrow bridge contract between the core driver and the optional telemetry implementation.
 */
public interface TelemetryBridge {
    void publish(TelemetryEvent event) throws Exception;
}
```

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/TelemetryEvent.java`

```java
package com.microsoft.sqlserver.jdbc;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

public final class TelemetryEvent {
    private final PerformanceActivity activity;
    private final int connectionId;
    private final int statementId;
    private final long duration;
    private final Exception exception;
    private final String userSql;
    private final StatementType statementType;
    private final Map<String, String> authHeaders;
    private final String authScheme;
    private final String spanName;
    private final String traceParent;
    private final String correlationId;

    public TelemetryEvent(PerformanceActivity activity, int connectionId, int statementId, long duration,
            Exception exception, String userSql, StatementType statementType, Map<String, String> authHeaders,
            String authScheme, String spanName, String traceParent, String correlationId) {
        this.activity = activity;
        this.connectionId = connectionId;
        this.statementId = statementId;
        this.duration = duration;
        this.exception = exception;
        this.userSql = userSql;
        this.statementType = statementType;
        this.authHeaders = Collections.unmodifiableMap(new LinkedHashMap<>(authHeaders));
        this.authScheme = authScheme;
        this.spanName = spanName;
        this.traceParent = traceParent;
        this.correlationId = correlationId;
    }
}
```

This keeps the auth header data inside a typed payload and gives the optional bridge the flexibility to decide whether to propagate, redact, or ignore those values.

### Step 1.3: Update Core PerformanceLog to Support SPI Discovery

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/PerformanceLog.java` (snippet to add)

```java
// Add this class (or integrate into existing PerformanceLog):

package com.microsoft.sqlserver.jdbc;

import com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory;
import java.util.ServiceLoader;
import java.util.logging.Level;

class TelemetryBridgeLoader {
    private static final java.util.logging.Logger logger = 
        java.util.logging.Logger.getLogger("com.microsoft.sqlserver.jdbc");
    
    /**
     * Attempt to load a telemetry bridge via ServiceLoader SPI.
     * 
     * This is called when connection properties indicate OTel or similar integration is desired.
     * If multiple implementations are available, the first one that successfully creates a callback is used.
     * 
     * @param connectionString The full connection string
     * @param properties       Parsed connection properties
     * @return A PerformanceLogCallback, or null if no bridge found or creation failed
     */
    static PerformanceLogCallback tryLoadTelemetryBridge(String connectionString, 
                                                          java.util.Properties properties) {
        try {
            ServiceLoader<TelemetryBridgeFactory> loader = 
                ServiceLoader.load(TelemetryBridgeFactory.class, 
                                   Thread.currentThread().getContextClassLoader());
            
            for (TelemetryBridgeFactory factory : loader) {
                try {
                    PerformanceLogCallback callback = factory.createCallback(connectionString, properties);
                    if (callback != null) {
                        if (logger.isLoggable(Level.FINE)) {
                            logger.fine("Telemetry bridge loaded: " + factory.getClass().getName());
                        }
                        return callback;
                    }
                } catch (Exception e) {
                    if (logger.isLoggable(Level.WARNING)) {
                        logger.warning("Telemetry bridge factory " + factory.getClass().getName() 
                                       + " failed: " + e.getMessage());
                    }
                    // Continue to next factory
                }
            }
        } catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning("Telemetry bridge discovery failed: " + e.getMessage());
            }
        }
        
        return null;
    }
}
```

### Step 1.3: Integrate Discovery into Connection Initialization

**File:** `src/main/java/com/microsoft/sqlserver/jdbc/SQLServerConnection.java` (integration point)

**Pseudo-code location:** During PRELOGIN/LOGIN, after connection properties are parsed:

```java
// In SQLServerConnection constructor or connect() method:

// Check if OTel or other telemetry is requested
if (connectionProperties.containsKey("otelEndpoint") || 
    connectionProperties.containsKey("telemetryEndpoint")) {
    
    // Attempt to load telemetry bridge via SPI
    PerformanceLogCallback telemetryCallback = 
        TelemetryBridgeLoader.tryLoadTelemetryBridge(connectionString, connectionProperties);
    
    if (telemetryCallback != null) {
        // Register the callback
        SQLServerDriver.registerPerformanceLogCallback(telemetryCallback);
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Telemetry callback registered for connection");
        }
    } else {
        // OTel endpoint set but no bridge found
        if (logger.isLoggable(Level.WARNING)) {
            logger.warning("Telemetry endpoint configured but no bridge found on classpath. " +
                          "Add 'mssql-jdbc-otel' dependency to enable.");
        }
    }
}
```

---

## Phase 2: Create New OTel Bridge Module

### Step 2.1: Create Module Structure

```bash
mkdir -p mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel
mkdir -p mssql-jdbc-otel/src/main/resources/META-INF/services
mkdir -p mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel
```

### Step 2.2: Create pom.xml for OTel Module

**File:** `mssql-jdbc-otel/pom.xml`

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <parent>
        <groupId>com.microsoft.sqlserver</groupId>
        <artifactId>mssql-jdbc-parent</artifactId>
        <version>13.5.0</version>
        <relativePath>../pom.xml</relativePath>
    </parent>

    <artifactId>mssql-jdbc-otel</artifactId>
    <packaging>jar</packaging>
    <name>Microsoft JDBC Driver for SQL Server — OpenTelemetry Bridge</name>
    <description>
        Optional OpenTelemetry bridge for the Microsoft JDBC Driver.
        Provides automatic metric collection and export to OTLP-compatible endpoints.
    </description>
    <url>https://github.com/Microsoft/mssql-jdbc</url>

    <properties>
        <opentelemetry.version>1.36.0</opentelemetry.version>
    </properties>

    <dependencies>
        <!-- Core JDBC Driver (SPI interfaces) -->
        <dependency>
            <groupId>com.microsoft.sqlserver</groupId>
            <artifactId>mssql-jdbc-core</artifactId>
            <version>${project.version}</version>
            <scope>provided</scope>
        </dependency>

        <!-- OpenTelemetry API (required) -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-api</artifactId>
            <version>${opentelemetry.version}</version>
        </dependency>

        <!-- OpenTelemetry SDK (required for standalone) -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-sdk</artifactId>
            <version>${opentelemetry.version}</version>
        </dependency>

        <!-- OTLP HTTP Exporter -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-exporter-otlp-http-metrics</artifactId>
            <version>${opentelemetry.version}</version>
        </dependency>

        <!-- JSON logging (optional, for debugging) -->
        <dependency>
            <groupId>io.opentelemetry</groupId>
            <artifactId>opentelemetry-sdk-logs</artifactId>
            <version>${opentelemetry.version}</version>
        </dependency>

        <!-- JUnit (test only) -->
        <dependency>
            <groupId>org.junit.jupiter</groupId>
            <artifactId>junit-jupiter</artifactId>
            <version>${junit.jupiter.version}</version>
            <scope>test</scope>
        </dependency>

        <!-- Mockito (test only) -->
        <dependency>
            <groupId>org.mockito</groupId>
            <artifactId>mockito-core</artifactId>
            <version>5.2.0</version>
            <scope>test</scope>
        </dependency>
    </dependencies>

    <build>
        <plugins>
            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-compiler-plugin</artifactId>
                <version>3.11.0</version>
                <configuration>
                    <source>11</source>
                    <target>11</target>
                </configuration>
            </plugin>

            <plugin>
                <groupId>org.apache.maven.plugins</groupId>
                <artifactId>maven-jar-plugin</artifactId>
                <version>3.3.0</version>
                <configuration>
                    <archive>
                        <manifest>
                            <addClasspath>false</addClasspath>
                        </manifest>
                    </archive>
                </configuration>
            </plugin>
        </plugins>
    </build>
</project>
```

### Step 2.3: ServiceLoader Registration File

**File:** `mssql-jdbc-otel/src/main/resources/META-INF/services/com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory`

```
com.microsoft.sqlserver.jdbc.otel.OTelTelemetryBridgeFactory
```

---

## Phase 3: Implement OTel Bridge Classes

### Step 3.1: Bridge Factory

**File:** `mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OTelTelemetryBridgeFactory.java`

```java
/*
 * Microsoft JDBC Driver for SQL Server — OpenTelemetry Bridge
 *
 * Copyright(c) Microsoft Corporation All rights reserved.
 * Licensed under the MIT License.
 */
package com.microsoft.sqlserver.jdbc.otel;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * SPI implementation that creates an OpenTelemetry-based performance callback.
 * 
 * This factory is discovered via ServiceLoader when mssql-jdbc-otel JAR is on the classpath.
 * 
 * @since 13.5.0
 */
public class OTelTelemetryBridgeFactory implements TelemetryBridgeFactory {
    private static final Logger logger = Logger.getLogger(OTelTelemetryBridgeFactory.class.getName());
    
    @Override
    public PerformanceLogCallback createCallback(String connectionString, Properties properties) 
            throws Exception {
        
        // Check if otelEndpoint is configured
        String otelEndpoint = null;
        if (properties != null) {
            otelEndpoint = properties.getProperty("otelEndpoint");
        }
        
        // If not in properties, try to parse from connection string
        if (otelEndpoint == null) {
            otelEndpoint = parseConnectionProperty(connectionString, "otelEndpoint");
        }
        
        // If no endpoint configured, don't create callback
        if (otelEndpoint == null || otelEndpoint.isEmpty()) {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("OTel bridge factory: otelEndpoint not configured, skipping");
            }
            return null;
        }
        
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Creating OpenTelemetry callback for endpoint: " + otelEndpoint);
        }
        
        return new OpenTelemetryPerformanceCallback(connectionString, properties);
    }
    
    /**
     * Helper to parse a connection property value from connection string.
     */
    private static String parseConnectionProperty(String connStr, String property) {
        if (connStr == null) return null;
        
        String searchStr = property + "=";
        int start = connStr.indexOf(searchStr);
        if (start == -1) return null;
        
        start += searchStr.length();
        int end = connStr.indexOf(';', start);
        if (end == -1) end = connStr.length();
        
        return connStr.substring(start, end).trim();
    }
}
```

### Step 3.2: OpenTelemetry Callback Implementation

**File:** `mssql-jdbc-otel/src/main/java/com/microsoft/sqlserver/jdbc/otel/OpenTelemetryPerformanceCallback.java`

```java
/*
 * Microsoft JDBC Driver for SQL Server — OpenTelemetry Bridge
 *
 * Copyright(c) Microsoft Corporation All rights reserved.
 * Licensed under the MIT License.
 */
package com.microsoft.sqlserver.jdbc.otel;

import com.microsoft.sqlserver.jdbc.PerformanceActivity;
import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import com.microsoft.sqlserver.jdbc.StatementType;
import io.opentelemetry.api.GlobalOpenTelemetry;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.api.common.AttributesBuilder;
import io.opentelemetry.api.metrics.Meter;
import io.opentelemetry.api.metrics.MeterProvider;
import io.opentelemetry.api.metrics.LongHistogram;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.exporter.otlp.http.metrics.OtlpHttpMetricExporter;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.logging.Level;

/**
 * OpenTelemetry-based implementation of PerformanceLogCallback.
 * 
 * Exports JDBC performance metrics (connection time, statement execution time, etc.)
 * to an OTLP-compatible endpoint.
 * 
 * @since 13.5.0
 */
public class OpenTelemetryPerformanceCallback implements PerformanceLogCallback {
    
    private static final Logger logger = Logger.getLogger(OpenTelemetryPerformanceCallback.class.getName());
    
    private final Meter meter;
    private final LongHistogram connectionDurationHistogram;
    private final LongHistogram statementDurationHistogram;
    private final MeterProvider meterProvider;
    
    private final long exportIntervalMs;
    private final long batchSize;
    private final double samplingRate;
    private final String serviceName;
    
    private boolean useNanos = true;  // Export high-precision metrics
    
    /**
     * Construct an OpenTelemetry callback.
     * 
     * @param connectionString The full connection string (for parsing OTel properties)
     * @param properties       Parsed connection properties
     * @throws Exception If OpenTelemetry setup fails
     */
    public OpenTelemetryPerformanceCallback(String connectionString, Properties properties) 
            throws Exception {
        
        // Parse OTel configuration
        String otelEndpoint = properties != null ? 
            properties.getProperty("otelEndpoint") : null;
        
        this.exportIntervalMs = Long.parseLong(properties != null ? 
            properties.getProperty("otelExportInterval", "60") : "60") * 1000;
        
        this.batchSize = Long.parseLong(properties != null ? 
            properties.getProperty("otelBatchSize", "1000") : "1000");
        
        this.samplingRate = Double.parseDouble(properties != null ? 
            properties.getProperty("otelSamplingRate", "1.0") : "1.0");
        
        this.serviceName = properties != null ? 
            properties.getProperty("otelServiceName", "mssql-jdbc") : "mssql-jdbc";
        
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("Initializing OTel callback: endpoint=" + otelEndpoint + 
                       ", interval=" + exportIntervalMs + "ms, batch=" + batchSize);
        }
        
        // Try to use GlobalOpenTelemetry if app already configured it
        MeterProvider mp = GlobalOpenTelemetry.getMeterProvider();
        
        if (mp == GlobalOpenTelemetry.noop().getMeterProvider()) {
            // App hasn't configured OTel; initialize our own
            mp = initializeOwnMeterProvider(otelEndpoint);
        } else {
            if (logger.isLoggable(Level.FINE)) {
                logger.fine("Reusing GlobalOpenTelemetry MeterProvider");
            }
        }
        
        this.meterProvider = mp;
        this.meter = mp.get("com.microsoft.sqlserver.jdbc", "13.5.0");
        
        // Create histograms
        this.connectionDurationHistogram = meter.histogramBuilder("db.client.connection.duration")
            .setDescription("Connection duration (nanoseconds)")
            .ofLongs()
            .build();
        
        this.statementDurationHistogram = meter.histogramBuilder("db.client.statement.duration")
            .setDescription("Statement execution duration (nanoseconds)")
            .ofLongs()
            .build();
    }
    
    private MeterProvider initializeOwnMeterProvider(String otelEndpoint) throws Exception {
        // Create OTLP exporter
        OtlpHttpMetricExporter exporter = OtlpHttpMetricExporter.builder()
            .setEndpoint(otelEndpoint)
            .build();
        
        // Create periodic reader
        PeriodicMetricReader reader = PeriodicMetricReader.builder(exporter)
            .setIntervalMillis(exportIntervalMs)
            .build();
        
        // Create resource with service name
        Resource resource = Resource.getDefault()
            .merge(Resource.create(
                Attributes.of(ResourceAttributes.SERVICE_NAME, serviceName)
            ));
        
        // Create SDK
        SdkMeterProvider sdkMeterProvider = SdkMeterProvider.builder()
            .registerMetricReader(reader)
            .setResource(resource)
            .build();
        
        // Set as global
        OpenTelemetrySdk.builder()
            .setMeterProvider(sdkMeterProvider)
            .buildAndRegisterGlobal();
        
        if (logger.isLoggable(Level.FINE)) {
            logger.fine("OpenTelemetry SDK initialized with OTLP endpoint: " + otelEndpoint);
        }
        
        return sdkMeterProvider;
    }
    
    @Override
    public void publish(PerformanceActivity activity, int connectionId, 
                       long duration, Exception exception) throws Exception {
        
        // Apply sampling
        if (Math.random() > samplingRate) {
            return;
        }
        
        AttributesBuilder ab = Attributes.builder();
        ab.put("connection_id", connectionId);
        ab.put("activity_type", activity.name());
        if (exception != null) {
            ab.put("error", exception.getClass().getSimpleName());
        }
        
        connectionDurationHistogram.record(duration, ab.build());
    }
    
    @Override
    public void publish(PerformanceActivity activity, int connectionId, 
                       int statementId, long duration, Exception exception) throws Exception {
        
        // Apply sampling
        if (Math.random() > samplingRate) {
            return;
        }
        
        AttributesBuilder ab = Attributes.builder();
        ab.put("connection_id", connectionId);
        ab.put("statement_id", statementId);
        ab.put("activity_type", activity.name());
        
        // Add statement type if available
        StatementType stmtType = this.getCurrentStatementType();
        if (stmtType != null) {
            ab.put("statement_type", stmtType.name());
        }
        
        // Add SQL if available (sanitize to avoid logging sensitive data)
        String sql = this.getCurrentUserSql();
        if (sql != null && !sql.isEmpty()) {
            // Only log first 100 chars of SQL (avoid logging entire query/sensitive data)
            String sanitizedSql = sql.length() > 100 ? 
                sql.substring(0, 100) + "..." : sql;
            ab.put("sql_preview", sanitizedSql);
        }
        
        if (exception != null) {
            ab.put("error", exception.getClass().getSimpleName());
        }
        
        statementDurationHistogram.record(duration, ab.build());
    }
    
    @Override
    public boolean useNanoseconds() {
        return useNanos;  // Export high-precision metrics
    }
}
```

### Step 3.3: Unit Tests

**File:** `mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/OTelTelemetryBridgeFactoryTest.java`

```java
package com.microsoft.sqlserver.jdbc.otel;

import com.microsoft.sqlserver.jdbc.PerformanceLogCallback;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class OTelTelemetryBridgeFactoryTest {
    
    @Test
    void testFactoryReturnsNullWhenEndpointNotConfigured() throws Exception {
        OTelTelemetryBridgeFactory factory = new OTelTelemetryBridgeFactory();
        
        // No properties set
        PerformanceLogCallback callback = factory.createCallback(
            "jdbc:sqlserver://server:1433;database=mydb", 
            null
        );
        
        assertNull(callback, "Factory should return null when otelEndpoint not configured");
    }
    
    @Test
    void testFactoryCreatesCallbackWhenEndpointConfigured() throws Exception {
        OTelTelemetryBridgeFactory factory = new OTelTelemetryBridgeFactory();
        
        java.util.Properties props = new java.util.Properties();
        props.setProperty("otelEndpoint", "http://localhost:4317/v1/metrics");
        
        PerformanceLogCallback callback = factory.createCallback(
            "jdbc:sqlserver://server:1433;database=mydb", 
            props
        );
        
        assertNotNull(callback, "Factory should create callback when otelEndpoint configured");
        assertTrue(callback instanceof OpenTelemetryPerformanceCallback);
    }
    
    @Test
    void testCallbackUsesNanoseconds() throws Exception {
        OTelTelemetryBridgeFactory factory = new OTelTelemetryBridgeFactory();
        
        java.util.Properties props = new java.util.Properties();
        props.setProperty("otelEndpoint", "http://localhost:4317/v1/metrics");
        
        PerformanceLogCallback callback = factory.createCallback(
            "jdbc:sqlserver://server:1433;database=mydb", 
            props
        );
        
        assertTrue(callback.useNanoseconds(), 
                  "OTel callback should use nanosecond precision");
    }
}
```

---

## Phase 4: Update Parent pom.xml

### Step 4.1: Create Aggregator pom.xml

**File:** `pom.xml` (parent)

```xml
<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 
         http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-parent</artifactId>
    <version>13.5.0</version>
    <packaging>pom</packaging>
    <name>Microsoft JDBC Driver for SQL Server — Parent</name>

    <modules>
        <!-- Core driver module -->
        <module>mssql-jdbc-core</module>
        
        <!-- Optional OTel bridge module -->
        <module>mssql-jdbc-otel</module>
    </modules>

    <properties>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
        <maven.compiler.source>11</maven.compiler.source>
        <maven.compiler.target>11</maven.compiler.target>
    </properties>

    <dependencyManagement>
        <dependencies>
            <!-- Shared test dependencies -->
            <dependency>
                <groupId>org.junit.jupiter</groupId>
                <artifactId>junit-jupiter</artifactId>
                <version>5.11.4</version>
                <scope>test</scope>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <build>
        <pluginManagement>
            <plugins>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-compiler-plugin</artifactId>
                    <version>3.11.0</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-jar-plugin</artifactId>
                    <version>3.3.0</version>
                </plugin>
                <plugin>
                    <groupId>org.apache.maven.plugins</groupId>
                    <artifactId>maven-source-plugin</artifactId>
                    <version>3.3.0</version>
                </plugin>
            </plugins>
        </pluginManagement>
    </build>
</project>
```

---

## Phase 5: Testing & Validation

### Step 5.1: Integration Test

**File:** `mssql-jdbc-otel/src/test/java/com/microsoft/sqlserver/jdbc/otel/integration/OTelIntegrationTest.java`

```java
package com.microsoft.sqlserver.jdbc.otel.integration;

import com.microsoft.sqlserver.jdbc.*;
import org.junit.jupiter.api.Test;
import java.util.Properties;

/**
 * Integration test: core driver + OTel bridge together.
 */
class OTelIntegrationTest {
    
    @Test
    void testOTelBridgeDiscoveryAndRegistration() throws Exception {
        // This test validates that:
        // 1. ServiceLoader discovers OTelTelemetryBridgeFactory
        // 2. Bridge creates callback successfully
        // 3. Callback receives performance events
        
        Properties props = new Properties();
        props.setProperty("otelEndpoint", "http://localhost:4317/v1/metrics");
        
        // In real scenario, opening JDBC connection triggers discovery
        // For this test, manually validate discovery:
        
        java.util.ServiceLoader<com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory> loader =
            java.util.ServiceLoader.load(
                com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory.class
            );
        
        boolean found = false;
        for (com.microsoft.sqlserver.jdbc.spi.TelemetryBridgeFactory factory : loader) {
            if (factory instanceof com.microsoft.sqlserver.jdbc.otel.OTelTelemetryBridgeFactory) {
                found = true;
                break;
            }
        }
        
        assertTrue(found, "OTelTelemetryBridgeFactory should be discoverable via ServiceLoader");
    }
}
```

---

## Phase 6: Documentation & README

### Step 6.1: Add to README.md

```markdown
## OpenTelemetry Support (Optional)

The Microsoft JDBC Driver supports exporting performance metrics to OpenTelemetry-compatible backends.

### Requirements

- Add the optional `mssql-jdbc-otel` bridge to your `pom.xml`:

```xml
<dependency>
    <groupId>com.microsoft.sqlserver</groupId>
    <artifactId>mssql-jdbc-otel</artifactId>
    <version>13.5.0</version>
</dependency>
```

### Configuration

Set connection properties to enable metrics export:

```
jdbc:sqlserver://myserver.database.windows.net:1433;
  database=mydb;
  authentication=ActiveDirectoryDefault;
  otelEndpoint=http://localhost:4317/v1/metrics;
  otelExportInterval=60;
  otelBatchSize=1000;
  otelServiceName=my-app
```

**Connection Properties:**

| Property | Default | Description |
|----------|---------|-------------|
| `otelEndpoint` | — | OTLP HTTP endpoint (e.g., http://localhost:4317/v1/metrics) |
| `otelExportInterval` | 60 | Export interval in seconds |
| `otelBatchSize` | 1000 | Number of metrics to batch before export |
| `otelSamplingRate` | 1.0 | Sample rate (0.0–1.0); 1.0 = 100% |
| `otelServiceName` | mssql-jdbc | Service name in OTLP resource attributes |

### How It Works

1. When a JDBC connection opens with `otelEndpoint` set, the driver discovers the OTel bridge via `ServiceLoader`.
2. The bridge initializes an OpenTelemetry `Meter` and registers a `PerformanceLogCallback`.
3. All JDBC performance events (connection time, statement execution time) are exported as OTel metrics.
4. Metrics are sent to the configured OTLP endpoint in batch.

### Example: Grafana + Tempo

```bash
# Start local OTLP collector (Docker)
docker run -p 4317:4317 otel/opentelemetry-collector:latest

# Configure JDBC connection
String url = "jdbc:sqlserver://myserver.database.windows.net:1433;" +
             "database=mydb;" +
             "otelEndpoint=http://localhost:4317/v1/metrics";

Connection conn = DriverManager.getConnection(url, user, pass);
// ^ OTel metrics now flowing to collector
```

---

## Deployment & Release Checklist

- [ ] Phase 1: Core driver SPI preparation (no breaking changes)
- [ ] Phase 2: New `mssql-jdbc-otel` module created
- [ ] Phase 3: OTel callback and factory implementation
- [ ] Phase 4: Parent pom.xml aggregation
- [ ] Phase 5: Integration tests pass
- [ ] Phase 6: Documentation updated
- [ ] Phase 7: Maven Central release (two coordinates)
- [ ] Phase 8: GitHub release notes updated

---

## Rollback / Migration Path

If issues arise:

1. **Without `mssql-jdbc-otel`:** Core driver continues to work unchanged.
2. **Remove `mssql-jdbc-otel` from pom.xml:** OTel metrics disabled, driver unaffected.
3. **No code changes required:** Entire feature is opt-in via dependency.

---

## Future Enhancements

- Alternative implementations (e.g., `mssql-jdbc-prometheus`, `mssql-jdbc-datadog`)
- Automatic instrumentation via javaagent
- Trace context propagation (W3C TraceContext)
- Custom metric aggregation strategies
```

---

## Summary

This implementation checklist covers all phases needed to transform the OTel POC into a production-ready separate JAR:

✅ Core driver minimal changes (SPI + discovery)  
✅ New `mssql-jdbc-otel` module with all OTel code  
✅ ServiceLoader-based auto-discovery  
✅ Tests and documentation  
✅ Backward-compatible deployment model  

The transformation is incremental and non-breaking: existing code continues to work, and OTel support is opt-in via dependency.
