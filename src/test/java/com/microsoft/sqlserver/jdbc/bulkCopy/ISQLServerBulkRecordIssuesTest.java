/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

@RunWith(JUnitPlatform.class)
public class ISQLServerBulkRecordIssuesTest extends AbstractTest {

    static Statement stmt = null;
    static PreparedStatement pStmt = null;
    static String query;
    static SQLServerConnection con = null;
    static String srcTable = "sourceTable";
    static String destTable = "destTable";
    String variation;

    /**
     * Testing that sending a bigger varchar(3) to varchar(2) is thowing the proper error message.
     * 
     * @throws Exception
     */
    @Test
    public void testVarchar() throws Exception {
        variation = "testVarchar";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + destTable + " (smallDATA varchar(2))";
        stmt.executeUpdate(query);

        try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
            bcOperation.setDestinationTableName(destTable);
            bcOperation.writeToServer(bData);
            bcOperation.close();
            fail("BulkCopy executed for testVarchar when it it was expected to fail");
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                assertTrue(e.getMessage().contains("The given value of type"), "Invalid Error message: " + e.toString());
            }
            else {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Testing that setting scale and precision 0 in column meta data for smalldatetime should work
     * 
     * @throws Exception
     */
    @Test
    public void testSmalldatetime() throws Exception {
        variation = "testSmalldatetime";
        BulkData bData = new BulkData(variation);
        String value = ("1954-05-22 02:44:00.0").toString();
        query = "CREATE TABLE " + destTable + " (smallDATA smalldatetime)";
        stmt.executeUpdate(query);

        try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
	        bcOperation.setDestinationTableName(destTable);
	        bcOperation.writeToServer(bData);
	
	        try (ResultSet rs = stmt.executeQuery("select * from " + destTable)) {
		        while (rs.next()) {
		            assertEquals(rs.getString(1), value);
		        }
	        }
        }
    }

    /**
     * Testing that setting out of range value for small datetime is throwing the proper message
     * 
     * @throws Exception
     */
    @Test
    public void testSmalldatetimeOutofRange() throws Exception {
        variation = "testSmalldatetimeOutofRange";
        BulkData bData = new BulkData(variation);
        
        query = "CREATE TABLE " + destTable + " (smallDATA smalldatetime)";
        stmt.executeUpdate(query);

        try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
            bcOperation.setDestinationTableName(destTable);
            bcOperation.writeToServer(bData);
            fail("BulkCopy executed for testSmalldatetimeOutofRange when it it was expected to fail");
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                assertTrue(e.getMessage().contains("Conversion failed when converting character string to smalldatetime data type"),
                        "Invalid Error message: " + e.toString());
            }
            else {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Test binary out of length (sending length of 6 to binary (5))
     * 
     * @throws Exception
     */
    @Test
    public void testBinaryColumnAsByte() throws Exception {
        variation = "testBinaryColumnAsByte";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + destTable + " (col1 binary(5))";
        stmt.executeUpdate(query);

        try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
            bcOperation.setDestinationTableName(destTable);
            bcOperation.writeToServer(bData);
            fail("BulkCopy executed for testBinaryColumnAsByte when it it was expected to fail");
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                assertTrue(e.getMessage().contains("The given value of type"), "Invalid Error message: " + e.toString());
            }
            else {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Test sending longer value for binary column while data is sent as string format
     * 
     * @throws Exception
     */
    @Test
    public void testBinaryColumnAsString() throws Exception {
        variation = "testBinaryColumnAsString";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + destTable + " (col1 binary(5))";
        stmt.executeUpdate(query);

        try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
            bcOperation.setDestinationTableName(destTable);
            bcOperation.writeToServer(bData);
            fail("BulkCopy executed for testBinaryColumnAsString when it it was expected to fail");
        }
        catch (Exception e) {
            if (e instanceof SQLException) {
                assertTrue(e.getMessage().contains("The given value of type"), "Invalid Error message: " + e.toString());
            }
            else {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Verify that sending valid value in string format for binary column is successful
     * 
     * @throws Exception
     */
    @Test
    public void testSendValidValueforBinaryColumnAsString() throws Exception {
        variation = "testSendValidValueforBinaryColumnAsString";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + destTable + " (col1 binary(5))";
        stmt.executeUpdate(query);

        try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
            bcOperation.setDestinationTableName(destTable);
            bcOperation.writeToServer(bData);
            
            try (ResultSet rs = stmt.executeQuery("select * from " + destTable)) {
	            while (rs.next()) {
	                assertEquals(rs.getString(1), "0101010000");
	            }
            }
        }
        catch (Exception e) {
           fail(e.getMessage());
        }
    }

    /**
     * Prepare test
     * 
     * @throws SQLException
     * @throws SecurityException
     * @throws IOException
     */
    @BeforeAll
    public static void setupHere() throws SQLException, SecurityException, IOException {
        con = (SQLServerConnection) DriverManager.getConnection(connectionString);
        stmt = con.createStatement();
        Utils.dropTableIfExists(destTable, stmt);
        Utils.dropTableIfExists(srcTable, stmt);
    }

    /**
     * Clean up
     * 
     * @throws SQLException
     */
    @AfterEach
    public void afterEachTests() throws SQLException {
        Utils.dropTableIfExists(destTable, stmt);
        Utils.dropTableIfExists(srcTable, stmt);
    }

    @AfterAll
    public static void afterAllTests() throws SQLException {
        if (null != stmt) {
            stmt.close();
        }
        if (null != con) {
            con.close();
        }
    }

}

class BulkData implements ISQLServerBulkRecord {
    boolean isStringData = false;

    private class ColumnMetadata {
        String columnName;
        int columnType;
        int precision;
        int scale;

        ColumnMetadata(String name,
                int type,
                int precision,
                int scale) {
            columnName = name;
            columnType = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    Map<Integer, ColumnMetadata> columnMetadata;
    ArrayList<Timestamp> dateData;
    ArrayList<String> stringData;
    ArrayList<byte[]> byteData;

    int counter = 0;
    int rowCount = 1;

    BulkData(String variation) {
        if (variation.equalsIgnoreCase("testVarchar")) {
            isStringData = true;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("varchar(2)", java.sql.Types.VARCHAR, 0, 0));

            stringData = new ArrayList<>();
            stringData.add(new String("aaa"));
            rowCount = stringData.size();
        }
        else if (variation.equalsIgnoreCase("testSmalldatetime")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("smallDatetime", java.sql.Types.TIMESTAMP, 0, 0));

            dateData = new ArrayList<>();
            dateData.add(Timestamp.valueOf("1954-05-22 02:43:37.123"));
            rowCount = dateData.size();
        }
        else if (variation.equalsIgnoreCase("testSmalldatetimeOutofRange")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("smallDatetime", java.sql.Types.TIMESTAMP, 0, 0));

            dateData = new ArrayList<>();
            dateData.add(Timestamp.valueOf("1954-05-22 02:43:37.1234"));
            rowCount = dateData.size();

        }
        else if (variation.equalsIgnoreCase("testBinaryColumnAsByte")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("binary(5)", java.sql.Types.BINARY, 5, 0));

            byteData = new ArrayList<>();
            byteData.add("helloo".getBytes());
            rowCount = byteData.size();

        }
        else if (variation.equalsIgnoreCase("testBinaryColumnAsString")) {
            isStringData = true;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("binary(5)", java.sql.Types.BINARY, 5, 0));

            stringData = new ArrayList<>();
            stringData.add("616368697412");
            rowCount = stringData.size();

        }

        else if (variation.equalsIgnoreCase("testSendValidValueforBinaryColumnAsString")) {
            isStringData = true;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("binary(5)", java.sql.Types.BINARY, 5, 0));

            stringData = new ArrayList<>();
            stringData.add("010101");
            rowCount = stringData.size();

        }
        counter = 0;

    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getColumnOrdinals()
     */
    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnMetadata.keySet();
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getColumnName(int)
     */
    @Override
    public String getColumnName(int column) {
        return columnMetadata.get(column).columnName;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getColumnType(int)
     */
    @Override
    public int getColumnType(int column) {
        return columnMetadata.get(column).columnType;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getPrecision(int)
     */
    @Override
    public int getPrecision(int column) {
        return columnMetadata.get(column).precision;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getScale(int)
     */
    @Override
    public int getScale(int column) {
        return columnMetadata.get(column).scale;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#isAutoIncrement(int)
     */
    @Override
    public boolean isAutoIncrement(int column) {
        return false;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#getRowData()
     */
    @Override
    public Object[] getRowData() throws SQLServerException {
        Object[] dataRow = new Object[columnMetadata.size()];
        if (isStringData)
            dataRow[0] = stringData.get(counter);
        else {
            if (null != dateData)
                dataRow[0] = dateData.get(counter);
            else if (null != byteData)
                dataRow[0] = byteData.get(counter);
        }
        counter++;
        return dataRow;
    }

    /*
     * (non-Javadoc)
     * 
     * @see com.microsoft.sqlserver.jdbc.ISQLServerBulkRecord#next()
     */
    @Override
    public boolean next() throws SQLServerException {
        if (counter < rowCount) {
            return true;
        }
        return false;
    }

}
