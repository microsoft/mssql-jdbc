/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.cts;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;

/**
 * CTS compliance tests for DatabaseMetaData: capability reporting, supported features,
 * type conversion support, transaction isolation, result set holdability, SQL compliance levels.
 * Ported from FX metaDataClient.java CTS tests.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.legacyFx)
@Tag(Constants.legacyFxCTS)
public class MetadataCTSTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    /**
     * Tests that supportsStoredProcedures returns true for SQL Server.
     * Ported from FX CTS metaDataClient supportsStoredProcedures variations.
     */
    @Test
    public void testSupportsStoredProcedures() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsStoredProcedures());
        }
    }

    /**
     * Tests allProceduresAreCallable metadata call completes without error.
     * Ported from FX CTS metaDataClient allProceduresAreCallable variations.
     */
    @Test
    public void testAllProceduresAreCallable() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            // SQL Server driver returns true
            dbmd.allProceduresAreCallable();
        }
    }

    /**
     * Tests allTablesAreSelectable metadata call completes without error.
     * Ported from FX CTS metaDataClient allTablesAreSelectable variations.
     */
    @Test
    public void testAllTablesAreSelectable() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            dbmd.allTablesAreSelectable();
        }
    }

    /**
     * Tests getDatabaseProductName returns a string containing 'Microsoft SQL Server'.
     * Ported from FX CTS metaDataClient getDatabaseProductName variations.
     */
    @Test
    public void testGetDatabaseProductName() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String name = dbmd.getDatabaseProductName();
            assertNotNull(name);
            assertTrue(name.contains("Microsoft SQL Server"));
        }
    }

    /**
     * Tests getDatabaseProductVersion returns a non-empty version string.
     * Ported from FX CTS metaDataClient getDatabaseProductVersion variations.
     */
    @Test
    public void testGetDatabaseProductVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String version = dbmd.getDatabaseProductVersion();
            assertNotNull(version);
            assertTrue(version.length() > 0);
        }
    }

    /**
     * Tests getDriverName returns a non-empty driver name.
     * Ported from FX CTS metaDataClient getDriverName variations.
     */
    @Test
    public void testGetDriverName() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String name = dbmd.getDriverName();
            assertNotNull(name);
            assertTrue(name.length() > 0);
        }
    }

    /**
     * Tests getDriverVersion returns a non-empty version string.
     * Ported from FX CTS metaDataClient getDriverVersion variations.
     */
    @Test
    public void testGetDriverVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String version = dbmd.getDriverVersion();
            assertNotNull(version);
            assertTrue(version.length() > 0);
        }
    }

    /**
     * Tests getDriverMajorVersion returns a non-negative value.
     * Ported from FX CTS metaDataClient getDriverMajorVersion variations.
     */
    @Test
    public void testGetDriverMajorVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int major = dbmd.getDriverMajorVersion();
            assertTrue(major >= 0);
        }
    }

    /**
     * Tests getDriverMinorVersion returns a non-negative value.
     * Ported from FX CTS metaDataClient getDriverMinorVersion variations.
     */
    @Test
    public void testGetDriverMinorVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int minor = dbmd.getDriverMinorVersion();
            assertTrue(minor >= 0);
        }
    }

    /**
     * Tests getDatabaseMajorVersion returns a value >= 10 for SQL Server.
     * Ported from FX CTS metaDataClient getDatabaseMajorVersion variations.
     */
    @Test
    public void testGetDatabaseMajorVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int major = dbmd.getDatabaseMajorVersion();
            assertTrue(major >= 10); // SQL Server 2008+
        }
    }

    /**
     * Tests getDatabaseMinorVersion returns a non-negative value.
     * Ported from FX CTS metaDataClient getDatabaseMinorVersion variations.
     */
    @Test
    public void testGetDatabaseMinorVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int minor = dbmd.getDatabaseMinorVersion();
            assertTrue(minor >= 0);
        }
    }

    /**
     * Tests getJDBCMajorVersion returns at least 4 for JDBC 4.x compliance.
     * Ported from FX CTS metaDataClient getJDBCMajorVersion variations.
     */
    @Test
    public void testGetJDBCMajorVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int major = dbmd.getJDBCMajorVersion();
            assertTrue(major >= 4);
        }
    }

    /**
     * Tests getJDBCMinorVersion returns a non-negative value.
     * Ported from FX CTS metaDataClient getJDBCMinorVersion variations.
     */
    @Test
    public void testGetJDBCMinorVersion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int minor = dbmd.getJDBCMinorVersion();
            assertTrue(minor >= 0);
        }
    }

    /**
     * Tests getURL returns a connection URL starting with 'jdbc:sqlserver://'.
     * Ported from FX CTS metaDataClient getURL variations.
     */
    @Test
    public void testGetURL() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String url = dbmd.getURL();
            assertNotNull(url);
            assertTrue(url.startsWith("jdbc:sqlserver://"));
        }
    }

    /**
     * Tests getUserName returns a non-null user name.
     * Ported from FX CTS metaDataClient getUserName variations.
     */
    @Test
    public void testGetUserName() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            String user = dbmd.getUserName();
            assertNotNull(user);
        }
    }

    /**
     * Tests that supportsMinimumSQLGrammar returns true.
     * Ported from FX CTS metaDataClient supportsMinimumSQLGrammar variations.
     */
    @Test
    public void testSupportsMinimumSQLGrammar() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsMinimumSQLGrammar());
        }
    }

    /**
     * Tests that supportsCoreSQLGrammar returns true.
     * Ported from FX CTS metaDataClient supportsCoreSQLGrammar variations.
     */
    @Test
    public void testSupportsCoreSQLGrammar() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsCoreSQLGrammar());
        }
    }

    /**
     * Tests that supportsANSI92EntryLevelSQL returns true.
     * Ported from FX CTS metaDataClient supportsANSI92EntryLevelSQL variations.
     */
    @Test
    public void testSupportsANSI92EntryLevelSQL() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsANSI92EntryLevelSQL());
        }
    }

    /**
     * Tests support for TRANSACTION_READ_UNCOMMITTED isolation level.
     * Ported from FX CTS metaDataClient supportsTransactionIsolationLevel variations.
     */
    @Test
    public void testSupportsTransactionIsolationReadUncommitted() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_UNCOMMITTED));
        }
    }

    /**
     * Tests support for TRANSACTION_READ_COMMITTED isolation level.
     * Ported from FX CTS metaDataClient supportsTransactionIsolationLevel variations.
     */
    @Test
    public void testSupportsTransactionIsolationReadCommitted() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_READ_COMMITTED));
        }
    }

    /**
     * Tests support for TRANSACTION_REPEATABLE_READ isolation level.
     * Ported from FX CTS metaDataClient supportsTransactionIsolationLevel variations.
     */
    @Test
    public void testSupportsTransactionIsolationRepeatableRead() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_REPEATABLE_READ));
        }
    }

    /**
     * Tests support for TRANSACTION_SERIALIZABLE isolation level.
     * Ported from FX CTS metaDataClient supportsTransactionIsolationLevel variations.
     */
    @Test
    public void testSupportsTransactionIsolationSerializable() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsTransactionIsolationLevel(Connection.TRANSACTION_SERIALIZABLE));
        }
    }

    /**
     * Tests support for HOLD_CURSORS_OVER_COMMIT result set holdability.
     * Ported from FX CTS metaDataClient supportsResultSetHoldability variations.
     */
    @Test
    public void testSupportsResultSetHoldability() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            // SQL Server supports HOLD_CURSORS_OVER_COMMIT
            assertTrue(dbmd.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        }
    }

    /**
     * Tests getResultSetHoldability returns a valid holdability constant.
     * Ported from FX CTS metaDataClient getResultSetHoldability variations.
     */
    @Test
    public void testGetResultSetHoldability() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int holdability = dbmd.getResultSetHoldability();
            assertTrue(holdability == ResultSet.HOLD_CURSORS_OVER_COMMIT
                    || holdability == ResultSet.CLOSE_CURSORS_AT_COMMIT);
        }
    }

    /**
     * Tests that supportsSavepoints returns true.
     * Ported from FX CTS metaDataClient supportsSavepoints variations.
     */
    @Test
    public void testSupportsSavepoints() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsSavepoints());
        }
    }

    /**
     * Tests that supportsNamedParameters returns true.
     * Ported from FX CTS metaDataClient supportsNamedParameters variations.
     */
    @Test
    public void testSupportsNamedParameters() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsNamedParameters());
        }
    }

    /**
     * Tests supportsMultipleOpenResults call completes without error.
     * Ported from FX CTS metaDataClient supportsMultipleOpenResults variations.
     */
    @Test
    public void testSupportsMultipleOpenResults() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            // Returns true when MARS is enabled
            dbmd.supportsMultipleOpenResults();
        }
    }

    /**
     * Tests that supportsGetGeneratedKeys returns true.
     * Ported from FX CTS metaDataClient supportsGetGeneratedKeys variations.
     */
    @Test
    public void testSupportsGetGeneratedKeys() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsGetGeneratedKeys());
        }
    }

    /**
     * Tests that at least one null sorting behavior is reported as true.
     * Ported from FX CTS metaDataClient null sorting variations.
     */
    @Test
    public void testNullSorting() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            // At least one should be true
            boolean anyNullSort = dbmd.nullsAreSortedHigh() || dbmd.nullsAreSortedLow()
                    || dbmd.nullsAreSortedAtStart() || dbmd.nullsAreSortedAtEnd();
            assertTrue(anyNullSort);
        }
    }

    /**
     * Tests that at least one identifier storage case mode is reported.
     * Ported from FX CTS metaDataClient identifier storage variations.
     */
    @Test
    public void testIdentifierStorage() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            // SQL Server stores mixed case identifiers
            assertTrue(dbmd.storesMixedCaseIdentifiers() || dbmd.storesUpperCaseIdentifiers()
                    || dbmd.storesLowerCaseIdentifiers());
        }
    }

    /**
     * Tests supportsConvert for generic and specific INTEGER-to-VARCHAR conversion.
     * Ported from FX CTS metaDataClient supportsConvert variations.
     */
    @Test
    public void testSupportsConvert() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            // Test generic supportsConvert
            dbmd.supportsConvert();
            // Test specific conversion: INTEGER to VARCHAR
            dbmd.supportsConvert(java.sql.Types.INTEGER, java.sql.Types.VARCHAR);
        }
    }

    /**
     * Tests getPrimaryKeys returns a valid ResultSet without error.
     * Ported from FX CTS metaDataClient getPrimaryKeys variations.
     */
    @Test
    public void testGetPrimaryKeys() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getPrimaryKeys(null, null, "%")) {
                assertNotNull(rs);
                // Just validate it returns without error
                ResultSet.class.isAssignableFrom(rs.getClass());
            }
        }
    }

    /**
     * Tests getTables returns at least one TABLE entry.
     * Ported from FX CTS metaDataClient getTables variations.
     */
    @Test
    public void testGetTables() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTables(null, null, "%", new String[] {"TABLE"})) {
                assertNotNull(rs);
                assertTrue(rs.next()); // Should have at least one table
            }
        }
    }

    /**
     * Tests getColumns returns at least one column entry.
     * Ported from FX CTS metaDataClient getColumns variations.
     */
    @Test
    public void testGetColumns() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getColumns(null, null, "%", "%")) {
                assertNotNull(rs);
                assertTrue(rs.next()); // Should have columns
            }
        }
    }

    /**
     * Tests getTypeInfo returns type information with valid TYPE_NAME entries.
     * Ported from FX CTS metaDataClient getTypeInfo variations.
     */
    @Test
    public void testGetTypeInfo() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTypeInfo()) {
                assertNotNull(rs);
                assertTrue(rs.next()); // Should list SQL types
                assertNotNull(rs.getString("TYPE_NAME"));
            }
        }
    }

    /**
     * Tests getSchemas returns at least one schema.
     * Ported from FX CTS metaDataClient getSchemas variations.
     */
    @Test
    public void testGetSchemas() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getSchemas()) {
                assertNotNull(rs);
                assertTrue(rs.next()); // Should have schemas
            }
        }
    }

    /**
     * Tests getCatalogs returns at least one catalog/database.
     * Ported from FX CTS metaDataClient getCatalogs variations.
     */
    @Test
    public void testGetCatalogs() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getCatalogs()) {
                assertNotNull(rs);
                assertTrue(rs.next()); // Should have catalogs/databases
            }
        }
    }

    /**
     * Tests getTableTypes returns at least one table type.
     * Ported from FX CTS metaDataClient getTableTypes variations.
     */
    @Test
    public void testGetTableTypes() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            try (ResultSet rs = dbmd.getTableTypes()) {
                assertNotNull(rs);
                assertTrue(rs.next());
            }
        }
    }

    /**
     * Tests support for TYPE_FORWARD_ONLY ResultSet type.
     * Ported from FX CTS metaDataClient supportsResultSetType variations.
     */
    @Test
    public void testSupportsResultSetTypeForwardOnly() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_FORWARD_ONLY));
        }
    }

    /**
     * Tests support for TYPE_SCROLL_INSENSITIVE ResultSet type.
     * Ported from FX CTS metaDataClient supportsResultSetType variations.
     */
    @Test
    public void testSupportsResultSetTypeScrollInsensitive() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_SCROLL_INSENSITIVE));
        }
    }

    /**
     * Tests support for TYPE_SCROLL_SENSITIVE ResultSet type.
     * Ported from FX CTS metaDataClient supportsResultSetType variations.
     */
    @Test
    public void testSupportsResultSetTypeScrollSensitive() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsResultSetType(ResultSet.TYPE_SCROLL_SENSITIVE));
        }
    }

    /**
     * Tests getMaxConnections returns a non-negative value.
     * Ported from FX CTS metaDataClient getMaxConnections variations.
     */
    @Test
    public void testMaxConnections() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int max = dbmd.getMaxConnections();
            assertTrue(max >= 0); // 0 means no limit
        }
    }

    /**
     * Tests getMaxStatements returns a non-negative value.
     * Ported from FX CTS metaDataClient getMaxStatements variations.
     */
    @Test
    public void testMaxStatements() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int max = dbmd.getMaxStatements();
            assertTrue(max >= 0);
        }
    }

    /**
     * Tests getMaxTableNameLength returns at least 128 for SQL Server.
     * Ported from FX CTS metaDataClient getMaxTableNameLength variations.
     */
    @Test
    public void testMaxTableNameLength() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int max = dbmd.getMaxTableNameLength();
            assertTrue(max >= 128); // SQL Server supports at least 128
        }
    }

    /**
     * Tests getMaxColumnNameLength returns at least 128 for SQL Server.
     * Ported from FX CTS metaDataClient getMaxColumnNameLength variations.
     */
    @Test
    public void testMaxColumnNameLength() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            int max = dbmd.getMaxColumnNameLength();
            assertTrue(max >= 128);
        }
    }

    /**
     * Tests that supportsSubqueriesInComparisons returns true.
     * Ported from FX CTS metaDataClient supportsSubqueriesInComparisons variations.
     */
    @Test
    public void testSupportsSubqueriesInComparisons() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsSubqueriesInComparisons());
        }
    }

    /**
     * Tests that supportsSubqueriesInExists returns true.
     * Ported from FX CTS metaDataClient supportsSubqueriesInExists variations.
     */
    @Test
    public void testSupportsSubqueriesInExists() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsSubqueriesInExists());
        }
    }

    /**
     * Tests that supportsSubqueriesInIns returns true.
     * Ported from FX CTS metaDataClient supportsSubqueriesInIns variations.
     */
    @Test
    public void testSupportsSubqueriesInIns() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsSubqueriesInIns());
        }
    }

    /**
     * Tests that supportsGroupBy returns true.
     * Ported from FX CTS metaDataClient supportsGroupBy variations.
     */
    @Test
    public void testSupportsGroupBy() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsGroupBy());
        }
    }

    /**
     * Tests that supportsOrderByUnrelated returns true.
     * Ported from FX CTS metaDataClient supportsOrderByUnrelated variations.
     */
    @Test
    public void testSupportsOrderByUnrelated() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsOrderByUnrelated());
        }
    }

    /**
     * Tests that supportsUnion returns true.
     * Ported from FX CTS metaDataClient supportsUnion variations.
     */
    @Test
    public void testSupportsUnion() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsUnion());
        }
    }

    /**
     * Tests that supportsUnionAll returns true.
     * Ported from FX CTS metaDataClient supportsUnionAll variations.
     */
    @Test
    public void testSupportsUnionAll() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsUnionAll());
        }
    }

    /**
     * Tests that supportsOuterJoins returns true.
     * Ported from FX CTS metaDataClient supportsOuterJoins variations.
     */
    @Test
    public void testSupportsOuterJoins() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsOuterJoins());
        }
    }

    /**
     * Tests that supportsFullOuterJoins returns true.
     * Ported from FX CTS metaDataClient supportsFullOuterJoins variations.
     */
    @Test
    public void testSupportsFullOuterJoins() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsFullOuterJoins());
        }
    }

    /**
     * Tests that supportsBatchUpdates returns true.
     * Ported from FX CTS metaDataClient supportsBatchUpdates variations.
     */
    @Test
    public void testSupportsBatchUpdates() throws SQLException {
        try (Connection conn = getConnection()) {
            DatabaseMetaData dbmd = conn.getMetaData();
            assertTrue(dbmd.supportsBatchUpdates());
        }
    }
}
