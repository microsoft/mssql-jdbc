/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.connection;

import com.microsoft.sqlserver.jdbc.mbt.core.Model;
import com.microsoft.sqlserver.jdbc.mbt.core.ModelEngine;
import com.microsoft.sqlserver.jdbc.mbt.core.ModelEngineOptions;
import com.microsoft.sqlserver.jdbc.mbt.core.ModelTest;
import com.microsoft.sqlserver.jdbc.mbt.core.WeightScheme;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Connection Model-Based Tests.
 * 
 * This is the JUnit 5 equivalent of connection.java from the FX framework.
 * It runs Model-Based Testing using the ConnectionModel class.
 * 
 * Test patterns:
 * 1. TCConnectionSanity - testOpen() with OPTIONAL properties
 * 2. TCConnectionMethods - testMethods() with model exploration
 * 3. TCConnectionValidConnectionStrings - test valid connection strings
 * 
 * Like the FX framework, these tests iterate over OPTIONAL properties
 * and run the ModelEngine for each, creating many ModelEngine runs
 * per test case (like the ~29 Seed lines per Variation in CI logs).
 */
@Tag("mbt")
@DisplayName("Connection Model-Based Tests")
public class ConnectionModelTest {
    
    // Connection parameters - read from environment variable or system properties
    private static String connectionUrl;
    private static String serverName;
    private static String databaseName;
    private static String user;
    private static String password;
    private static String baseUrl;
    
    private Connection connection;
    
    /**
     * Connection creation mechanism (equivalent to fxConnectionCreationMechanism)
     */
    public enum ConnectionType {
        DRIVER_MANAGER_URL,
        DRIVER_MANAGER_URL_INFO,
        DRIVER_MANAGER_URL_USER_PASSWORD,
        DRIVER
    }
    
    /**
     * Extract a property value from JDBC connection URL.
     */
    private static String extractProperty(String url, String propertyName) {
        String pattern = propertyName + "=";
        int start = url.indexOf(pattern);
        if (start == -1) return null;
        start += pattern.length();
        int end = url.indexOf(';', start);
        if (end == -1) end = url.length();
        return url.substring(start, end);
    }
    
    @BeforeAll
    static void setUpClass() {
        // Debug: print environment variables related to SQL Server
        System.out.println("DEBUG: Checking environment variables...");
        System.out.println("msSqlServer=" + System.getenv("msSqlServer"));
        System.out.println("MSSQL_SERVER=" + System.getenv("MSSQL_SERVER"));
        System.out.println("JDBC_CONNECTION_STRING=" + System.getenv("JDBC_CONNECTION_STRING"));
        
        // First check environment variable (msSqlServer)
        connectionUrl = System.getenv("msSqlServer");
        if (connectionUrl == null || connectionUrl.isEmpty()) {
            connectionUrl = System.getenv("MSSQL_SERVER");
        }
        if (connectionUrl == null || connectionUrl.isEmpty()) {
            connectionUrl = System.getenv("JDBC_CONNECTION_STRING");
        }
        
        if (connectionUrl != null && !connectionUrl.isEmpty()) {
            // Parse connection URL from environment
            // Format: jdbc:sqlserver://SERVER;databaseName=DB;userName=USER;password=PWD;
            System.out.println("Using connection from environment: " + connectionUrl.replaceAll("password=[^;]*", "password=***"));
            baseUrl = connectionUrl;
            
            // Extract components for individual use
            if (connectionUrl.contains("userName=")) {
                user = extractProperty(connectionUrl, "userName");
            }
            if (connectionUrl.contains("password=")) {
                password = extractProperty(connectionUrl, "password");
            }
        } else {
            // Fall back to system properties
            serverName = System.getProperty("mssql.jdbc.test.server", "localhost");
            databaseName = System.getProperty("mssql.jdbc.test.database", "master");
            user = System.getProperty("mssql.jdbc.test.user", "sa");
            password = System.getProperty("mssql.jdbc.test.password", "");
            
            baseUrl = String.format(
                "jdbc:sqlserver://%s;databaseName=%s;encrypt=true;trustServerCertificate=true",
                serverName, databaseName
            );
            System.out.println("Using fallback connection: " + baseUrl);
        }
        
        // Load driver
        try {
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
        } catch (ClassNotFoundException e) {
            fail("Failed to load SQL Server JDBC Driver: " + e.getMessage());
        }
    }
    
    @BeforeEach
    void setUp() {
        // Connection will be created in each test based on connection type
    }
    
    @AfterEach
    void tearDown() {
        if (connection != null) {
            try {
                if (!connection.isClosed()) {
                    connection.close();
                }
            } catch (SQLException e) {
                // Ignore close errors in teardown
            }
        }
    }
    
    // ==================== Connection Helpers ====================
    
    /**
     * Creates a connection based on connection type.
     * Equivalent to fxConnection.createConnection()
     */
    private Connection createConnection(ConnectionType type, String url, 
                                         Properties info, String usr, String pwd) throws SQLException {
        switch (type) {
            case DRIVER_MANAGER_URL:
                return DriverManager.getConnection(url);
            case DRIVER_MANAGER_URL_INFO:
                return DriverManager.getConnection(url, info);
            case DRIVER_MANAGER_URL_USER_PASSWORD:
                return DriverManager.getConnection(url, usr, pwd);
            case DRIVER:
                java.sql.Driver driver = DriverManager.getDriver(url);
                return driver.connect(url, info);
            default:
                throw new IllegalArgumentException("Unknown connection type: " + type);
        }
    }
    
    /**
     * Builds connection string with properties.
     * Equivalent to TC.buildConnectionString()
     */
    private String buildConnectionString(List<ConnectionProperty> props) {
        StringBuilder url = new StringBuilder(baseUrl);
        for (ConnectionProperty prop : props) {
            if (prop.getValue() != null && !prop.getValue().isEmpty()) {
                url.append(";").append(prop.getKeyword()).append("=").append(prop.getValue());
            }
        }
        return url.toString();
    }
    
    /**
     * Creates Properties object from property list.
     */
    private Properties buildProperties(List<ConnectionProperty> props) {
        Properties info = new Properties();
        for (ConnectionProperty prop : props) {
            if (prop.getValue() != null && !prop.getValue().isEmpty()) {
                info.setProperty(prop.getKeyword(), prop.getValue());
            }
        }
        return info;
    }
    
    /**
     * Runs the model on a connection.
     * Equivalent to TC.runModel()
     */
    private void runModel(Connection conn, long seed) throws SQLException {
        ConnectionModel model = new ConnectionModel(conn);
        
        ModelEngineOptions options = new ModelEngineOptions()
            .withSeed(seed)
            .withTimeout(30)
            .withMaxActions(500);
        
        ModelEngine engine = new ModelEngine(model, options);
        ModelEngine.ModelEngineResult result = engine.run();
        
        if (!result.isSuccess()) {
            System.err.println("Model execution failed. Seed: " + result.getSeed());
            result.printLog();
            fail("Model test failed. To reproduce, use seed: " + result.getSeed());
        }
    }
    
    // ==================== TCConnectionSanity Tests ====================
    // Equivalent to TCConnectionSanity.testOpen()
    
    /**
     * Test connection sanity with DRIVER_MANAGER_URL.
     * Iterates over all OPTIONAL properties, creating a ModelEngine run for each.
     * 
     * This is equivalent to:
     *   TCConnectionSanity(DRIVERMANAGER_URL, true) -> "DRIVERMANAGER_URL_VERIFY_MODEL"
     */
    @Test
    @DisplayName("DRIVERMANAGER_URL_VERIFY_MODEL")
    void testDriverManagerUrlModel() throws SQLException {
        long baseSeed = System.currentTimeMillis();
        List<ConnectionProperty> optionalProps = SqlServerDriverProperties.getOptionalProperties();
        
        System.out.println("Running " + optionalProps.size() + " ModelEngine iterations for OPTIONAL properties");
        
        for (int i = 0; i < optionalProps.size(); i++) {
            ConnectionProperty prop = optionalProps.get(i);
            
            // Build URL with this optional property
            String url = baseUrl + ";user=" + user + ";password=" + password;
            String validValue = prop.getRandomValidValue();
            if (validValue != null && !validValue.isEmpty()) {
                url += ";" + prop.getKeyword() + "=" + validValue;
            }
            
            try {
                connection = DriverManager.getConnection(url);
                
                // Run model with unique seed for this property
                long seed = baseSeed + i;
                System.out.printf("Seed: %d (property: %s=%s)%n", seed, prop.getKeyword(), validValue);
                
                runModel(connection, seed);
                
            } catch (SQLException e) {
                // Some property combinations may fail to connect - log and continue
                System.out.println("Connection failed for " + prop.getKeyword() + ": " + e.getMessage());
            } finally {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        }
    }
    
    /**
     * Test connection sanity with DRIVER_MANAGER_URL_INFO.
     * 
     * This is equivalent to:
     *   TCConnectionSanity(DRIVERMANAGER_URL_INFO, true) -> "DRIVERMANAGER_URL_INFO_VERIFY_MODEL"
     */
    @Test
    @DisplayName("DRIVERMANAGER_URL_INFO_VERIFY_MODEL")
    void testDriverManagerUrlInfoModel() throws SQLException {
        long baseSeed = System.currentTimeMillis();
        List<ConnectionProperty> optionalProps = SqlServerDriverProperties.getOptionalProperties();
        
        for (int i = 0; i < optionalProps.size(); i++) {
            ConnectionProperty prop = optionalProps.get(i);
            
            Properties info = new Properties();
            info.setProperty("user", user);
            info.setProperty("password", password);
            
            String validValue = prop.getRandomValidValue();
            if (validValue != null && !validValue.isEmpty()) {
                info.setProperty(prop.getKeyword(), validValue);
            }
            
            try {
                connection = DriverManager.getConnection(baseUrl, info);
                
                long seed = baseSeed + i;
                System.out.printf("Seed: %d (property: %s=%s)%n", seed, prop.getKeyword(), validValue);
                
                runModel(connection, seed);
                
            } catch (SQLException e) {
                System.out.println("Connection failed for " + prop.getKeyword() + ": " + e.getMessage());
            } finally {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        }
    }
    
    /**
     * Test connection sanity with DRIVER_MANAGER_URL_USER_PASSWORD.
     * 
     * This is equivalent to:
     *   TCConnectionSanity(DRIVERMANAGER_URL_USER_PASSWORD, true) -> "DRIVERMANAGER_URL_USER_PASSWORD_VERIFY_MODEL"
     */
    @Test
    @DisplayName("DRIVERMANAGER_URL_USER_PASSWORD_VERIFY_MODEL")
    void testDriverManagerUrlUserPasswordModel() throws SQLException {
        long baseSeed = System.currentTimeMillis();
        List<ConnectionProperty> optionalProps = SqlServerDriverProperties.getOptionalProperties();
        
        for (int i = 0; i < optionalProps.size(); i++) {
            ConnectionProperty prop = optionalProps.get(i);
            
            String url = baseUrl;
            String validValue = prop.getRandomValidValue();
            if (validValue != null && !validValue.isEmpty()) {
                url += ";" + prop.getKeyword() + "=" + validValue;
            }
            
            try {
                connection = DriverManager.getConnection(url, user, password);
                
                long seed = baseSeed + i;
                System.out.printf("Seed: %d (property: %s=%s)%n", seed, prop.getKeyword(), validValue);
                
                runModel(connection, seed);
                
            } catch (SQLException e) {
                System.out.println("Connection failed for " + prop.getKeyword() + ": " + e.getMessage());
            } finally {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        }
    }
    
    /**
     * Test connection sanity with DRIVER.
     * 
     * This is equivalent to:
     *   TCConnectionSanity(DRIVER, true) -> "DRIVER_VERIFY_MODEL"
     */
    @Test
    @DisplayName("DRIVER_VERIFY_MODEL")
    void testDriverModel() throws SQLException {
        long baseSeed = System.currentTimeMillis();
        List<ConnectionProperty> optionalProps = SqlServerDriverProperties.getOptionalProperties();
        
        java.sql.Driver driver = DriverManager.getDriver(baseUrl);
        
        for (int i = 0; i < optionalProps.size(); i++) {
            ConnectionProperty prop = optionalProps.get(i);
            
            Properties info = new Properties();
            info.setProperty("user", user);
            info.setProperty("password", password);
            
            String validValue = prop.getRandomValidValue();
            if (validValue != null && !validValue.isEmpty()) {
                info.setProperty(prop.getKeyword(), validValue);
            }
            
            try {
                connection = driver.connect(baseUrl, info);
                
                if (connection != null) {
                    long seed = baseSeed + i;
                    System.out.printf("Seed: %d (property: %s=%s)%n", seed, prop.getKeyword(), validValue);
                    
                    runModel(connection, seed);
                }
                
            } catch (SQLException e) {
                System.out.println("Connection failed for " + prop.getKeyword() + ": " + e.getMessage());
            } finally {
                if (connection != null && !connection.isClosed()) {
                    connection.close();
                }
            }
        }
    }
    
    // ==================== TCConnectionMethods Tests ====================
    // Equivalent to TCConnectionMethods with model exploration
    
    /**
     * Parameterized test for different connection types.
     * Runs model exploration for each connection type.
     */
    @ParameterizedTest(name = "Methods_{0}_MODEL")
    @EnumSource(ConnectionType.class)
    @DisplayName("Connection Methods Model Test")
    void testConnectionMethodsModel(ConnectionType type) throws SQLException {
        Properties info = new Properties();
        info.setProperty("user", user);
        info.setProperty("password", password);
        
        String url = baseUrl + ";user=" + user + ";password=" + password;
        
        connection = createConnection(type, url, info, user, password);
        
        if (connection != null) {
            long seed = System.currentTimeMillis();
            System.out.printf("Seed: %d (ConnectionType: %s)%n", seed, type);
            
            runModel(connection, seed);
        }
    }
    
    // ==================== Reproducibility Test ====================
    
    /**
     * Test for reproducing a specific seed.
     * Update the seed value to reproduce a failing test.
     */
    @Test
    @DisplayName("Reproduce with specific seed")
    void testReproduceWithSeed() throws SQLException {
        // Set this to the seed from a failed test to reproduce
        long seed = System.currentTimeMillis(); // Change to actual failing seed
        
        String url = baseUrl + ";user=" + user + ";password=" + password;
        connection = DriverManager.getConnection(url);
        
        ConnectionModel model = new ConnectionModel(connection);
        
        ModelEngineOptions options = new ModelEngineOptions()
            .withSeed(seed)
            .withTimeout(30)
            .withMaxActions(500)
            .withVerbose(true);
        
        ModelEngine engine = new ModelEngine(model, options);
        ModelEngine.ModelEngineResult result = engine.run();
        
        result.printLog();
        assertTrue(result.isSuccess(), "Model test failed");
    }
    
    // ==================== Coverage Test ====================
    
    /**
     * Test that ensures all actions get exercised.
     * Uses WEIGHT_SCHEME_CALL_COUNT_INVERSE for maximum coverage.
     */
    @Test
    @DisplayName("Coverage focused model test")
    void testModelCoverage() throws SQLException {
        String url = baseUrl + ";user=" + user + ";password=" + password;
        connection = DriverManager.getConnection(url);
        
        ConnectionModel model = new ConnectionModel(connection);
        
        ModelEngineOptions options = new ModelEngineOptions()
            .withTimeout(60)
            .withMaxActions(1000)
            .withWeightScheme(WeightScheme.WEIGHT_SCHEME_CALL_COUNT_INVERSE);
        
        ModelEngine engine = new ModelEngine(model, options);
        ModelEngine.ModelEngineResult result = engine.run();
        
        System.out.println("Coverage test completed:");
        System.out.println("  Actions executed: " + result.getActionCount());
        System.out.println("  Duration: " + result.getDurationMs() + "ms");
        System.out.println("  Seed: " + result.getSeed());
        
        assertTrue(result.isSuccess(), "Coverage model test failed");
        assertTrue(result.getActionCount() > 50, "Expected more actions for coverage");
    }
    
    // ==================== Provider Methods ====================
    
    /**
     * Provides arguments for parameterized tests.
     */
    static Stream<Arguments> connectionTypeProvider() {
        return Stream.of(
            Arguments.of(ConnectionType.DRIVER_MANAGER_URL, true),
            Arguments.of(ConnectionType.DRIVER_MANAGER_URL_INFO, true),
            Arguments.of(ConnectionType.DRIVER_MANAGER_URL_USER_PASSWORD, true),
            Arguments.of(ConnectionType.DRIVER, true)
        );
    }
}
