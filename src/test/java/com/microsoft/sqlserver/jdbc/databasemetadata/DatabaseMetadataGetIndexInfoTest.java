package com.microsoft.sqlserver.jdbc.databasemetadata;

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
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;

public class DatabaseMetadataGetIndexInfoTest extends AbstractTest {

	private static String tableName = AbstractSQLGenerator.escapeIdentifier("DBMetadataTestTable");
    private static String col1Name = AbstractSQLGenerator.escapeIdentifier("p1");
    private static String col2Name = AbstractSQLGenerator.escapeIdentifier("p2");
    private static String col3Name = AbstractSQLGenerator.escapeIdentifier( "p3");
    
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
                        col2Name + " INT, " +
                        col3Name + " INT)";
            	
            	stmt.executeUpdate(createTableSQL);
            	assertNull(connection.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Connection.");
            	assertNull(stmt.getWarnings(), "Expecting NO SQLWarnings from 'create table', at Statement.");
                
                String createClusteredIndexSQL = "CREATE CLUSTERED INDEX IDX_Clustered ON " + tableName + "(" + col1Name + ")";
    			stmt.executeUpdate(createClusteredIndexSQL);
    			assertNull(connection.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Connection.");
                assertNull(stmt.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Statement.");
    			
    			String createNonClusteredIndexSQL = "CREATE NONCLUSTERED INDEX IDX_NonClustered ON " + tableName + "(" + col2Name + ")";
    			stmt.executeUpdate(createNonClusteredIndexSQL);
    			assertNull(connection.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Connection.");
                assertNull(stmt.getWarnings(), "Expecting NO SQLWarnings from 'create index', at Statement.");
                
                String createColumnstoreIndexSQL = "CREATE COLUMNSTORE INDEX IDX_Columnstore ON " + tableName + "(" + col3Name + ")";
    			stmt.executeUpdate(createColumnstoreIndexSQL);
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
        ResultSet rs1, rs2 = null;
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String catalog = connection.getCatalog();
            String schema = "dbo";
            String table = "DBMetadataTestTable";
            DatabaseMetaData dbMetadata = connection.getMetaData();
			rs1 = dbMetadata.getIndexInfo(catalog, schema, table, false, false);

			boolean hasClusteredIndex = false;
            boolean hasNonClusteredIndex = false;
            boolean hasColumnstoreIndex = false;
            
			String query = 
		                "SELECT " +
		                "    db_name() AS CatalogName, " +
		                "    sch.name AS SchemaName, " +
		                "    t.name AS TableName, " +
		                "    i.name AS IndexName, " +
		                "    i.type_desc AS IndexType, " +
		                "    i.is_unique AS IsUnique, " +
		                "    c.name AS ColumnName, " +
		                "    ic.key_ordinal AS ColumnOrder " +
		                "FROM " +
		                "    sys.indexes i " +
		                "INNER JOIN " +
		                "    sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id " +
		                "INNER JOIN " +
		                "    sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id " +
		                "INNER JOIN " +
		                "    sys.tables t ON i.object_id = t.object_id " +
		                "INNER JOIN " +
		                "    sys.schemas sch ON t.schema_id = sch.schema_id " +
		
		                "WHERE t.name = '" + table + "' " +
		                      "AND sch.name = '" + schema + "' " +
		                "ORDER BY " +
		                "    t.name, i.name, ic.key_ordinal;";
			rs2 = stmt.executeQuery(query);
			
            while (rs1.next() && rs2.next()) {
                String indexType = rs1.getString("IndexType");
                String indexName = rs1.getString("IndexName");
                
                assertEquals(rs1.getString("CatalogName"), rs2.getString("CatalogName"));
                assertEquals(rs1.getString("SchemaName"), rs2.getString("SchemaName"));
                assertEquals(rs1.getString("TableName"), rs2.getString("TableName"));
                assertEquals(indexName, rs2.getString("IndexName"));
                assertEquals(indexType, rs2.getString("IndexType"));
                assertEquals(rs1.getString("IsUnique"), rs2.getString("IsUnique"));
                assertEquals(rs1.getString("ColumnName"), rs2.getString("ColumnName"));
                assertEquals(rs1.getString("ColumnOrder"), rs2.getString("ColumnOrder"));

                if (indexType.contains("COLUMNSTORE")) {
                    hasColumnstoreIndex = true;
                } else if (indexType.equals("CLUSTERED")) {
                    hasClusteredIndex = true;
                } else if (indexType.equals("NONCLUSTERED")) {
                    hasNonClusteredIndex = true;
                }
            }

            assertTrue(hasColumnstoreIndex, "COLUMNSTORE index not found.");
            assertTrue(hasClusteredIndex, "CLUSTERED index not found.");
            assertTrue(hasNonClusteredIndex, "NONCLUSTERED index not found.");
        }
    }
    
    @Test
    public void testGetIndexInfoCaseSensitivity() throws SQLException, SQLServerException {
        ResultSet rs1, rs2 = null;
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
            String catalog = connection.getCatalog();
            String schema = "dbo";
            String table = "DBMetadataTestTable";
            
            DatabaseMetaData dbMetadata = connection.getMetaData();
			rs1 = dbMetadata.getIndexInfo(catalog, schema, table, false, false);
			rs2 = dbMetadata.getIndexInfo(catalog, schema, table.toUpperCase(), false, false);

			while (rs1.next() && rs2.next()) {
                String indexType = rs1.getString("IndexType");
                String indexName = rs1.getString("IndexName");
                
                assertEquals(rs1.getString("CatalogName"), rs2.getString("CatalogName"));
                assertEquals(rs1.getString("SchemaName"), rs2.getString("SchemaName"));
                assertEquals(rs1.getString("TableName"), rs2.getString("TableName"));
                assertEquals(indexName, rs2.getString("IndexName"));
                assertEquals(indexType, rs2.getString("IndexType"));
                assertEquals(rs1.getString("IsUnique"), rs2.getString("IsUnique"));
                assertEquals(rs1.getString("ColumnName"), rs2.getString("ColumnName"));
                assertEquals(rs1.getString("ColumnOrder"), rs2.getString("ColumnOrder"));
            }
        }
    }
}
