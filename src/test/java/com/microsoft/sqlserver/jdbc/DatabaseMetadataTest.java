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

    private static String tableName = AbstractSQLGenerator.escapeIdentifier("DBMetadataTestTable");
    private static String col1Name = AbstractSQLGenerator.escapeIdentifier("p1");
    private static String col2Name = AbstractSQLGenerator.escapeIdentifier("p2");
    private static String indexName = AbstractSQLGenerator.escapeIdentifier( "indx_col");
    
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }
    
    @BeforeEach
    public void init() throws Exception {
        try (Connection con = getConnection()) {
            con.setAutoCommit(false);
            try (Statement stmt = con.createStatement()) {
            	TestUtils.dropTableIfExists(tableName, stmt);
            	String createTableSQL = "CREATE TABLE " + tableName + " (" +
                                        col1Name + " INT, " +
                                        col2Name + " INT)";
            	
            	stmt.executeUpdate(createTableSQL);
            	assertNull(connection.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Connection.");
            	assertNull(stmt.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Statement.");
            	
            	String createIndexSQL = "CREATE COLUMNSTORE INDEX " + indexName + " ON " + tableName + " (" + col2Name + ")";
            	stmt.executeUpdate(createIndexSQL);
            	assertNull(connection.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Connection.");
                assertNull(stmt.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Statement.");
            }
            con.commit();
        }
    }
    
    @AfterEach
    public void terminate() throws Exception {
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            try {
                TestUtils.dropTableIfExists(tableName, stmt);
            } catch (SQLException e) {
                fail(TestResource.getResource("R_unexpectedException") + e.getMessage());
            }
        }
    }

    @Test
    public void testGetIndexInfo() throws SQLException, SQLServerException {
        ResultSet rs = null;
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String catalog = connection.getCatalog();
            String schema = "dbo";

            DatabaseMetaData dbMetadata = connection.getMetaData();
			rs = dbMetadata.getIndexInfo(catalog, schema, tableName, false, false);

            boolean hasColumnstoreIndex = false;
            
            System.out.println("Testing getIndexInfo " + rs);
            if (rs.next()) {
                String indexType = rs.getString("IndexType");
                String indexName = rs.getString("IndexName");
                System.out.println(indexType + " " + indexName);

                if (indexType.contains("COLUMNSTORE")) {
                    hasColumnstoreIndex = true;
                }
            }

            assertTrue(hasColumnstoreIndex, "COLUMNSTORE index not found.");
        }
    }
}
