package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;

public class DatabaseMetadataTest extends AbstractTest {

    private static String tableName = AbstractSQLGenerator.escapeIdentifier("TestTable");
    private static String col1Name = AbstractSQLGenerator.escapeIdentifier("p1");
    private static String col2Name = AbstractSQLGenerator.escapeIdentifier("p2");
    private static String col3Name = AbstractSQLGenerator.escapeIdentifier("p3");

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void init() throws SQLException {
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String createTableSQL = "CREATE TABLE " + tableName + " (" +
                                    col1Name + " INT, " +
                                    col2Name + " INT, " +
                                    col3Name + " INT)";
            stmt.executeUpdate(createTableSQL);

            String createClusteredIndexSQL = "CREATE CLUSTERED INDEX IDX_Clustered ON " + tableName + "(" + col1Name + ")";
            stmt.executeUpdate(createClusteredIndexSQL);

            String createNonClusteredIndexSQL = "CREATE NONCLUSTERED INDEX IDX_NonClustered ON " + tableName + "(" + col2Name + ")";
            stmt.executeUpdate(createNonClusteredIndexSQL);

            String createColumnstoreIndexSQL = "CREATE NONCLUSTERED COLUMNSTORE INDEX IDX_Columnstore ON " + tableName + "(" + col3Name + ")";
            stmt.executeUpdate(createColumnstoreIndexSQL);

        } catch (SQLException e) {
            fail("Exception occurred while testing getIndexInfo: " + e.getMessage());
        }
    }

    @AfterEach
    public void terminate() throws SQLException {
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String dropTableSQL = "DROP TABLE IF EXISTS " + AbstractSQLGenerator.escapeIdentifier(tableName);
            stmt.executeUpdate(dropTableSQL);
        }
    }

    @Test
    public void testGetIndexInfo() throws SQLException, SQLServerException {
        ResultSet rs = null;
        try (Connection connection = getConnection()) {
            String catalog = connection.getCatalog();
            String schema = "dbo";
            String table = tableName;

            DatabaseMetaData dbMetadata = connection.getMetaData();
            rs = dbMetadata.getIndexInfo(catalog, schema, table, false, false);

            boolean hasClusteredIndex = false;
            boolean hasNonClusteredIndex = false;
            boolean hasColumnstoreIndex = false;
            
            while (rs.next()) {
                String indexType = rs.getString("IndexType");
                String indexName = rs.getString("IndexName");

                if (indexType.contains("COLUMNSTORE")) {
                    hasColumnstoreIndex = true;
                } else if (indexType.contains("CLUSTERED")) {
                    hasClusteredIndex = true;
                } else if (indexType.contains("NONCLUSTERED")) {
                    hasNonClusteredIndex = true;
                }
            }

            assertTrue(hasClusteredIndex);
            assertTrue(hasNonClusteredIndex);
            assertTrue(hasColumnstoreIndex);
        } catch (SQLException e) {
            fail("Exception occurred while testing getIndexInfo: " + e.getMessage());
        }
    }

    @Test
    public void testGetIndexInfoNonExistentTable() throws SQLException, SQLServerException {
        String nonExistentTable = AbstractSQLGenerator.escapeIdentifier("NonExistentTable");

        try (Connection connection = getConnection()) {
            String catalog = connection.getCatalog();
            String schema = "dbo";
            String table = nonExistentTable;

            DatabaseMetaData dbMetadata = connection.getMetaData();
            ResultSet rs = dbMetadata.getIndexInfo(catalog, schema, table, false, false);

            fail("Expected SQLException when calling getIndexInfo on a non-existent table, but no exception was thrown.");
        } catch (SQLException e) {
            assertNotNull(e);
        }
    }
}
