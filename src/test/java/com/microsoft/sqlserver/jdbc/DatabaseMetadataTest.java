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
    private static String col4Name = AbstractSQLGenerator.escapeIdentifier("p4");
    
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void init() throws SQLException {
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String createTableSQL = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(tableName) + " (" +
                                    "id INT PRIMARY KEY, " +
                                    col1Name + " NVARCHAR(50), " +
                                    col2Name + " INT, " +
                                    col3Name + " DECIMAL(10, 2), " +
                                    col4Name + " DATE)";
            stmt.executeUpdate(createTableSQL);

            String createClusteredIndexSQL = "CREATE CLUSTERED INDEX IDX_Clustered ON " + AbstractSQLGenerator.escapeIdentifier(tableName) + "(id)";
            stmt.executeUpdate(createClusteredIndexSQL);

            String createNonClusteredIndexSQL = "CREATE NONCLUSTERED INDEX IDX_NonClustered ON " + AbstractSQLGenerator.escapeIdentifier(tableName) + "(" + col2Name + ")";
            stmt.executeUpdate(createNonClusteredIndexSQL);

            String createColumnstoreIndexSQL = "CREATE NONCLUSTERED COLUMNSTORE INDEX IDX_Columnstore ON " + AbstractSQLGenerator.escapeIdentifier(tableName) + "(" + col3Name + ")";
            stmt.executeUpdate(createColumnstoreIndexSQL);
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
        try {
            try (Connection connection = getConnection()) {
                String catalog = connection.getCatalog();
                String schema = "dbo";
                String table = tableName;

                DatabaseMetaData dbMetdata = connection.getMetaData();
                rs = dbMetdata.getIndexInfo(catalog, schema, table, false, false);

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

                assertTrue(hasClusteredIndex, "CLUSTERED index found.");
                assertTrue(hasNonClusteredIndex, "NONCLUSTERED index found.");
                assertTrue(hasColumnstoreIndex, "COLUMNSTORE index found.");
            }
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

            DatabaseMetaData dbMetdata = connection.getMetaData();
            ResultSet rs = dbMetdata.getIndexInfo(catalog, schema, table, false, false);
            
            fail("Expected SQLException when calling getIndexInfo on a non-existent table, but no exception was thrown.");
        } catch (SQLException e) {
        	assertNotNull(e);
        }
    }
}
