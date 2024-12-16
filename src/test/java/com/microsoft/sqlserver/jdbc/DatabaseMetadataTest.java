package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
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

    @Test
    public void testGetIndexInfo() throws SQLException, SQLServerException {
        ResultSet rs = null;
        try (Connection connection = getConnection(); Statement stmt = connection.createStatement()) {
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
		
        	String catalog = connection.getCatalog();
        	String schema = "dbo";

        	DatabaseMetaData dbMetadata = connection.getMetaData();
		rs = dbMetadata.getIndexInfo(catalog, schema, tableName, false, false);

        	boolean hasColumnstoreIndex = false;
        	System.out.println("Testing getIndexInfo " + rs);
		
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
                   
                    "WHERE t.name = '" + tableName + "' " +
                          "AND sch.name = '" + schema + "' " +
                    "ORDER BY " +
                    "    t.name, i.name, ic.key_ordinal;";
        	ResultSet rs1 = stmt.executeQuery(query);
        	System.out.println("Testing query " + rs1);
		
		if (rs.next()) {
			System.out.println("Testing getIndexInfo " + rs);
            		String indexType = rs.getString("IndexType");
			String indexName = rs.getString("IndexName");
			System.out.println("Testing query and Index Type: " + rs + " " + indexType);
			System.out.println("Testing function and Index Type: " + rs + " " + indexType);
			System.out.println(indexType + " " + indexName);

			if (indexType.contains("COLUMNSTORE")) {
				hasColumnstoreIndex = true;
			}
        	}
	        assertTrue(hasColumnstoreIndex, "COLUMNSTORE index not found.");
        }
    }
}
