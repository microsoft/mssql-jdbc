/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Properties;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

@RunWith(JUnitPlatform.class)
public class TransactionIsolationPropertyTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /** Finds a DriverPropertyInfo by name (case-insensitive); fails if absent. */
    private static DriverPropertyInfo findProperty(DriverPropertyInfo[] props, String name) {
        for (DriverPropertyInfo prop : props) {
            if (name.equalsIgnoreCase(prop.name)) {
                return prop;
            }
        }
        fail("Property '" + name + "' not found in DriverPropertyInfo");
        return null;
    }

    /** Verifies the property appears in DriverPropertyInfo with the correct value. */
    @Test
    public void testPropertyInfo() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = connectionString + ";defaultTransactionIsolation=READ_COMMITTED";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        DriverPropertyInfo prop = findProperty(propertyInfo, "defaultTransactionIsolation");
        assertEquals("READ_COMMITTED", prop.value);
    }

    /** Invalid value must throw R_InvalidConnectionSetting. */
    @Test
    public void testInvalidProperty() {
        String url = connectionString + ";defaultTransactionIsolation=invalid";

        SQLException exception = assertThrows(SQLException.class, () -> {
            PrepUtil.getConnection(url);
        });

        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")), 
                "Actual message: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("defaultTransactionIsolation"));
        assertTrue(exception.getMessage().contains("invalid"));
    }

    /** Unsupported value must throw R_InvalidConnectionSetting. */
    @Test
    public void testOutOfRangeProperty() {
        String url = connectionString + ";defaultTransactionIsolation=NUMERIC_VALUE_UNSUPPORTED";

        SQLException exception = assertThrows(SQLException.class, () -> {
            PrepUtil.getConnection(url);
        });

        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_InvalidConnectionSetting")),
                "Actual message: " + exception.getMessage());
        assertTrue(exception.getMessage().contains("defaultTransactionIsolation"));
        assertTrue(exception.getMessage().contains("NUMERIC_VALUE_UNSUPPORTED"));
    }

    /** SNAPSHOT is a valid choice in DriverPropertyInfo. */
    @Test
    public void testSnapshotPropertyInfo() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = connectionString + ";defaultTransactionIsolation=SNAPSHOT";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        DriverPropertyInfo prop = findProperty(propertyInfo, "defaultTransactionIsolation");
        assertEquals("SNAPSHOT", prop.value);
    }

    /** DataSource getter/setter round-trips the value. */
    @Test
    public void testDataSourceProperty() {
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setDefaultTransactionIsolation("READ_UNCOMMITTED");
        assertEquals("READ_UNCOMMITTED", ds.getDefaultTransactionIsolation());
    }

    /** DataSource returns null when the property is not set. */
    @Test
    public void testDataSourceDefaultProperty() {
        SQLServerDataSource ds = new SQLServerDataSource();
        assertNull(ds.getDefaultTransactionIsolation());
    }

    /** Empty and whitespace-only values fall back to the default isolation level. */
    @Test
    public void testEdgeCasePropertyValues() throws Exception {
        // Empty value — falls back to default.
        String emptyUrl = connectionString + ";defaultTransactionIsolation=";
        try (Connection con = PrepUtil.getConnection(emptyUrl)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
        }

        // Whitespace — falls back to default.
        String whitespaceUrl = connectionString + ";defaultTransactionIsolation= ";
        try (Connection con = PrepUtil.getConnection(whitespaceUrl)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
        }
    }

    /** Mixed-case input is normalized to uppercase in DriverPropertyInfo. */
    @Test
    public void testCaseInsensitiveProperty() throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        Properties info = new Properties();
        String url = connectionString + ";defaultTransactionIsolation=read_Committed";

        DriverPropertyInfo[] propertyInfo = driver.getPropertyInfo(url, info);
        assertNotNull(propertyInfo);

        DriverPropertyInfo prop = findProperty(propertyInfo, "defaultTransactionIsolation");
        assertEquals("READ_COMMITTED", prop.value);
    }

    /** All five supported levels are applied correctly to a live connection. */
    @Test
    public void testTransactionIsolationApplied() throws Exception {
        String[] levels = {"READ_UNCOMMITTED", "READ_COMMITTED", "REPEATABLE_READ", "SERIALIZABLE", "SNAPSHOT"};
        int[] expectedConstants = {
            Connection.TRANSACTION_READ_UNCOMMITTED,
            Connection.TRANSACTION_READ_COMMITTED,
            Connection.TRANSACTION_REPEATABLE_READ,
            Connection.TRANSACTION_SERIALIZABLE,
            ISQLServerConnection.TRANSACTION_SNAPSHOT
        };

        for (int i = 0; i < levels.length; i++) {
            String url = TestUtils.addOrOverrideProperty(connectionString, "defaultTransactionIsolation", levels[i]);
            try (Connection con = PrepUtil.getConnection(url)) {
                assertEquals(expectedConstants[i], con.getTransactionIsolation(),
                        "Isolation level " + levels[i] + " was not applied correctly.");
            }
        }
    }

    /** Mixed-case value is applied correctly to a live connection. */
    @Test
    public void testMixedCaseTransactionIsolationApplied() throws Exception {
        String url = connectionString + ";defaultTransactionIsolation=reAd_comMitted";
        try (Connection con = PrepUtil.getConnection(url)) {
            assertEquals(Connection.TRANSACTION_READ_COMMITTED, con.getTransactionIsolation());
        }
    }

    /** Runtime setTransactionIsolation() overrides the connection property with real semantic effect. */
    @Test
    public void testRuntimeOverridesConnectionProperty() throws Exception {
        String tableName1 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("txnIsoOverride1"));
        String tableName2 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("txnIsoOverride2"));

        // Scenario 1: Connect SERIALIZABLE, override to READ_UNCOMMITTED — dirty read allowed.
        String serializableUrl = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "SERIALIZABLE");
        String readUncommittedUrl = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "READ_UNCOMMITTED");

        try (Connection con1 = PrepUtil.getConnection(serializableUrl);
             Connection con2 = PrepUtil.getConnection(serializableUrl)) {

            try (Statement s = con1.createStatement()) {
                TestUtils.dropTableIfExists(tableName1, s);
                s.execute("CREATE TABLE " + tableName1 + " (id INT)");
            }
            con1.setAutoCommit(false);
            try (Statement s = con1.createStatement()) {
                s.executeUpdate("INSERT INTO " + tableName1 + " VALUES (1)");
            }

            con2.setTransactionIsolation(Connection.TRANSACTION_READ_UNCOMMITTED);
            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, con2.getTransactionIsolation());

            try (Statement s = con2.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + tableName1)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Dirty read should see uncommitted row.");
            }

            con1.rollback();
            con1.setAutoCommit(true);
            try (Statement s = con1.createStatement()) {
                TestUtils.dropTableIfExists(tableName1, s);
            }
        }

        // Scenario 2: Connect READ_UNCOMMITTED, override to SERIALIZABLE — lock contention.
        try (Connection con1 = PrepUtil.getConnection(readUncommittedUrl);
             Connection con2 = PrepUtil.getConnection(readUncommittedUrl)) {

            try (Statement s = con1.createStatement()) {
                TestUtils.dropTableIfExists(tableName2, s);
                s.execute("CREATE TABLE " + tableName2 + " (id INT)");
                s.executeUpdate("INSERT INTO " + tableName2 + " VALUES (1)");
            }

            con1.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            con1.setAutoCommit(false);
            try (Statement s = con1.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM " + tableName2)) {
                while (rs.next()) {}
            }

            con2.setTransactionIsolation(Connection.TRANSACTION_SERIALIZABLE);
            assertEquals(Connection.TRANSACTION_SERIALIZABLE, con2.getTransactionIsolation());

            try (Statement s = con2.createStatement()) {
                s.execute("SET LOCK_TIMEOUT 2000");
                assertThrows(SQLException.class, () -> {
                    s.executeUpdate("INSERT INTO " + tableName2 + " VALUES (2)");
                }, "Insert should be blocked by SERIALIZABLE range lock.");
            }

            con1.rollback();
            con1.setAutoCommit(true);
            try (Statement s = con1.createStatement()) {
                TestUtils.dropTableIfExists(tableName2, s);
            }
        }
    }
    /** READ_UNCOMMITTED via connection property allows dirty reads of uncommitted data. */
    @Test
    public void testReadUncommittedSemantics() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("txnIsoRU"));
        String url = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "READ_UNCOMMITTED");

        try (Connection writer = PrepUtil.getConnection(connectionString);
             Connection reader = PrepUtil.getConnection(url)) {

            assertEquals(Connection.TRANSACTION_READ_UNCOMMITTED, reader.getTransactionIsolation());

            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
                s.execute("CREATE TABLE " + tableName + " (id INT)");
            }

            writer.setAutoCommit(false);
            try (Statement s = writer.createStatement()) {
                s.executeUpdate("INSERT INTO " + tableName + " VALUES (1)");
            }

            // Dirty read: reader sees the uncommitted row.
            try (Statement s = reader.createStatement();
                 ResultSet rs = s.executeQuery("SELECT COUNT(*) FROM " + tableName)) {
                assertTrue(rs.next());
                assertEquals(1, rs.getInt(1), "Dirty read should see uncommitted row.");
            }

            writer.rollback();
            writer.setAutoCommit(true);
            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
            }
        }
    }

    /**
     * READ_COMMITTED via connection property blocks dirty reads.
     * RCSI: reader sees old committed value. Non-RCSI: reader blocks until lock timeout.
     */
    @Test
    public void testReadCommittedSemantics() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("txnIsoRC"));
        String url = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "READ_COMMITTED");

        try (Connection writer = PrepUtil.getConnection(connectionString);
             Connection reader = PrepUtil.getConnection(url)) {

            assertEquals(Connection.TRANSACTION_READ_COMMITTED, reader.getTransactionIsolation());

            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
                s.execute("CREATE TABLE " + tableName + " (id INT, val INT)");
                s.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 100)");
            }

            writer.setAutoCommit(false);
            try (Statement s = writer.createStatement()) {
                s.executeUpdate("UPDATE " + tableName + " SET val = 999 WHERE id = 1");
            }

            // Reader must NOT see the uncommitted value 999.
            try (Statement s = reader.createStatement()) {
                s.execute("SET LOCK_TIMEOUT 2000");
                try (ResultSet rs = s.executeQuery("SELECT val FROM " + tableName + " WHERE id = 1")) {
                    assertTrue(rs.next());
                    assertEquals(100, rs.getInt(1),
                            "READ_COMMITTED should not see the dirty update.");
                }
            } catch (SQLException e) {
                // Non-RCSI: lock timeout confirms dirty read was blocked.
                String msg = e.getMessage().toLowerCase();
                assertTrue(msg.contains("lock") || msg.contains("timeout") || msg.contains("time out"),
                        "Expected lock timeout but got: " + e.getMessage());
            }

            writer.rollback();
            writer.setAutoCommit(true);
            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
            }
        }
    }

    /** SERIALIZABLE via connection property acquires range locks that block concurrent inserts. */
    @Test
    public void testSerializableSemantics() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("txnIsoSer"));
        String url = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "SERIALIZABLE");

        try (Connection reader = PrepUtil.getConnection(url);
             Connection writer = PrepUtil.getConnection(connectionString)) {

            assertEquals(Connection.TRANSACTION_SERIALIZABLE, reader.getTransactionIsolation());

            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
                s.execute("CREATE TABLE " + tableName + " (id INT)");
                s.executeUpdate("INSERT INTO " + tableName + " VALUES (1)");
            }

            // Reader scans under SERIALIZABLE — acquires range locks.
            reader.setAutoCommit(false);
            try (Statement s = reader.createStatement();
                 ResultSet rs = s.executeQuery("SELECT * FROM " + tableName)) {
                while (rs.next()) {}
            }

            // Writer's insert must be blocked by the range lock.
            try (Statement s = writer.createStatement()) {
                s.execute("SET LOCK_TIMEOUT 2000");
                assertThrows(SQLException.class, () -> {
                    s.executeUpdate("INSERT INTO " + tableName + " VALUES (2)");
                }, "SERIALIZABLE range lock should block the insert.");
            }

            reader.rollback();
            reader.setAutoCommit(true);
            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
            }
        }
    }

    /** SNAPSHOT via connection property provides a point-in-time view unaffected by concurrent commits. */
    @Test
    public void testSnapshotSemantics() throws Exception {
        String tableName = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("txnIsoSnap"));
        String url = TestUtils.addOrOverrideProperty(connectionString,
                "defaultTransactionIsolation", "SNAPSHOT");

        try (Connection setup = PrepUtil.getConnection(connectionString)) {
            try (Statement s = setup.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
                s.execute("CREATE TABLE " + tableName + " (id INT, val INT)");
                s.executeUpdate("INSERT INTO " + tableName + " VALUES (1, 100)");
            }
        }

        try (Connection reader = PrepUtil.getConnection(url);
             Connection writer = PrepUtil.getConnection(connectionString)) {

            assertEquals(ISQLServerConnection.TRANSACTION_SNAPSHOT, reader.getTransactionIsolation());

            // Reader takes a snapshot.
            reader.setAutoCommit(false);
            try (Statement s = reader.createStatement();
                 ResultSet rs = s.executeQuery("SELECT val FROM " + tableName + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1));
            }

            // Writer commits a new value.
            try (Statement s = writer.createStatement()) {
                s.executeUpdate("UPDATE " + tableName + " SET val = 200 WHERE id = 1");
            }

            // Reader still sees the snapshot value, not the committed update.
            try (Statement s = reader.createStatement();
                 ResultSet rs = s.executeQuery("SELECT val FROM " + tableName + " WHERE id = 1")) {
                assertTrue(rs.next());
                assertEquals(100, rs.getInt(1),
                        "SNAPSHOT should see point-in-time value, not the concurrent commit.");
            }

            reader.rollback();
            try (Statement s = writer.createStatement()) {
                TestUtils.dropTableIfExists(tableName, s);
            }
        }
    }
}