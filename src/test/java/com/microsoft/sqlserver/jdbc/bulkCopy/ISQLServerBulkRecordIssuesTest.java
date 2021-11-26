/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.IOException;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@SuppressWarnings("deprecation")
@RunWith(JUnitPlatform.class)
public class ISQLServerBulkRecordIssuesTest extends AbstractTest {

    static String query;
    static String srcTable = RandomUtil.getIdentifier("sourceTable");
    static String destTable = RandomUtil.getIdentifier("destTable");

    String variation;

    @BeforeAll
    public static void setupTests() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();
    }

    /**
     * Testing that sending a bigger varchar(3) to varchar(2) is thowing the proper error message.
     * 
     * @throws Exception
     */
    @Test
    public void testVarchar() throws Exception {
        variation = "testVarchar";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (smallDATA varchar(2))";
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);
                bcOperation.close();
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                if (e instanceof SQLException) {
                    assertTrue(e.getMessage().contains(TestResource.getResource("R_givenValueType")),
                            TestResource.getResource("R_invalidErrorMessage") + e.getMessage());
                } else {
                    fail(e.getMessage());
                }
            }
        }
    }

    /**
     * Testing that setting scale and precision 0 in column meta data for smalldatetime should work
     * 
     * @throws Exception
     */
    @Test
    @Tag(Constants.xAzureSQLDW)
    public void testSmalldatetime() throws Exception {
        variation = "testSmalldatetime";
        BulkData bData = new BulkData(variation);
        String value = ("1954-05-22 02:44:00.0").toString();
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (smallDATA smalldatetime)";
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);

                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
                    while (rs.next()) {
                        assertEquals(rs.getString(1), value);
                    }
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
    @Tag(Constants.xAzureSQLDW)
    public void testSmalldatetimeOutofRange() throws Exception {
        variation = "testSmalldatetimeOutofRange";
        BulkData bData = new BulkData(variation);

        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (smallDATA smalldatetime)";
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);
                fail("BulkCopy executed for testSmalldatetimeOutofRange when it it was expected to fail");
            } catch (Exception e) {
                if (e instanceof SQLException) {
                    MessageFormat form = new MessageFormat(TestResource.getResource("R_conversionFailed"));
                    Object[] msgArgs = {"character string", "smalldatetime"};

                    assertTrue(e.getMessage().contains(form.format(msgArgs)),
                            TestResource.getResource("R_invalidErrorMessage") + e.getMessage());
                } else {
                    fail(e.getMessage());
                }
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
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (col1 binary(5))";
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                if (e instanceof SQLException) {
                    assertTrue(e.getMessage().contains(TestResource.getResource("R_givenValueType")),
                            TestResource.getResource("R_invalidErrorMessage") + e.getMessage());
                } else {
                    fail(e.getMessage());
                }
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
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (col1 binary(5))";
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);
                fail(TestResource.getResource("R_expectedFailPassed"));
            } catch (Exception e) {
                if (e instanceof SQLException) {
                    assertTrue(e.getMessage().contains(TestResource.getResource("R_givenValueType")),
                            TestResource.getResource("R_invalidErrorMessage") + e.getMessage());
                } else {
                    fail(e.getMessage());
                }
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
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (col1 binary(5))";
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);

                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
                    while (rs.next()) {
                        assertEquals(rs.getString(1), "0101010000");
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Testing that sending valid values of LocalDateTime with precision 3 for datetime2 column are successful
     *
     * @throws Exception
     */
    @Test
    public void testSendValidValueforDatetime3ColumnAsLocalDateTime() throws Exception {
        variation = "testSendValidValueforDatetime3ColumnAsLocalDateTime";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (col1 datetime2(7))";
        int counter = 0;
        String[] result = {"2021-01-01 00:00:00.0000000", "2021-01-01 12:00:00.0000000", "2021-01-01 12:30:00.0000000",
                "2021-01-01 12:30:44.0000000", "2021-01-01 12:30:44.0000000", "2021-01-01 12:30:44.0030000",
                "2021-01-01 12:30:44.1000000", "2021-01-01 12:30:44.1230000", "2021-01-01 12:30:44.9990000"};
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);

                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
                    while (rs.next()) {
                        assertEquals(rs.getString(1), result[counter]);
                        counter++;
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Testing that sending valid values of LocalDateTime with precision 7 for datetime2 column are successful
     *
     * @throws Exception
     */
    @Test
    public void testSendValidValueforDatetime7ColumnAsLocalDateTime() throws Exception {
        variation = "testSendValidValueforDatetime7ColumnAsLocalDateTime";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (col1 datetime2(7))";
        int counter = 0;
        String[] result = {"2021-01-01 00:00:00.0000000", "2021-01-01 12:00:00.0000000", "2021-01-01 12:30:00.0000000",
                "2021-01-01 12:30:44.0000000", "2021-01-01 12:30:44.0000000", "2021-01-01 12:30:44.0000007",
                "2021-01-01 12:30:44.0030000", "2021-01-01 12:30:44.1000000", "2021-01-01 12:30:44.1230000",
                "2021-01-01 12:30:44.1234567", "2021-01-01 12:30:44.9999999"};
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);

                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
                    while (rs.next()) {
                        assertEquals(rs.getString(1), result[counter]);
                        counter++;
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
        }
    }

    /**
     * Testing that sending valid values of LocalTime for datetime2 column are successful
     *
     * @throws Exception
     */
    @Test
    public void testSendValidValueforDatetime2ColumnAsLocalTime() throws Exception {
        variation = "testSendValidValueforDatetime2ColumnAsLocalTime";
        BulkData bData = new BulkData(variation);
        query = "CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(destTable) + " (col1 datetime2(7))";
        int counter = 0;
        String[] result = {"1900-01-01 00:00:00.0000000", "1900-01-01 12:00:00.0000000", "1900-01-01 12:30:00.0000000",
                "1900-01-01 12:30:44.0000000", "1900-01-01 12:30:44.0000007", "1900-01-01 12:30:44.0030000",
                "1900-01-01 12:30:44.1000000", "1900-01-01 12:30:44.1230000", "1900-01-01 12:30:44.1234567",
                "1900-01-01 12:30:44.9999999"};
        try (Connection con = getConnection(); Statement stmt = con.createStatement()) {
            stmt.executeUpdate(query);

            try (SQLServerBulkCopy bcOperation = new SQLServerBulkCopy(connectionString)) {
                bcOperation.setDestinationTableName(AbstractSQLGenerator.escapeIdentifier(destTable));
                bcOperation.writeToServer(bData);

                try (ResultSet rs = stmt
                        .executeQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(destTable))) {
                    while (rs.next()) {
                        assertEquals(rs.getString(1), result[counter]);
                        counter++;
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            }
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
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(destTable), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(srcTable), stmt);
        }
    }

    /**
     * Clean up
     * 
     * @throws SQLException
     */
    @AfterEach
    public void afterEachTests() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(destTable), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(srcTable), stmt);
        }
    }
}

class BulkData implements ISQLServerBulkData {
    private static final long serialVersionUID = 1L;
    boolean isStringData = false;

    private class ColumnMetadata {
        String columnName;
        int columnType;
        int precision;
        int scale;

        ColumnMetadata(String name, int type, int precision, int scale) {
            columnName = name;
            columnType = type;
            this.precision = precision;
            this.scale = scale;
        }
    }

    Map<Integer, ColumnMetadata> columnMetadata;
    ArrayList<Timestamp> dateData;
    ArrayList<LocalDateTime> datetime3Data;
    ArrayList<LocalDateTime> datetime7Data;
    ArrayList<LocalTime> timeData;
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
        } else if (variation.equalsIgnoreCase("testSmalldatetime")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("smallDatetime", java.sql.Types.TIMESTAMP, 0, 0));

            dateData = new ArrayList<>();
            dateData.add(Timestamp.valueOf("1954-05-22 02:43:37.123"));
            rowCount = dateData.size();
        } else if (variation.equalsIgnoreCase("testSmalldatetimeOutofRange")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("smallDatetime", java.sql.Types.TIMESTAMP, 0, 0));

            dateData = new ArrayList<>();
            dateData.add(Timestamp.valueOf("1954-05-22 02:43:37.1234"));
            rowCount = dateData.size();
        } else if (variation.equalsIgnoreCase("testBinaryColumnAsByte")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("binary(5)", java.sql.Types.BINARY, 5, 0));

            byteData = new ArrayList<>();
            byteData.add("helloo".getBytes());
            rowCount = byteData.size();
        } else if (variation.equalsIgnoreCase("testBinaryColumnAsString")) {
            isStringData = true;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("binary(5)", java.sql.Types.BINARY, 5, 0));

            stringData = new ArrayList<>();
            stringData.add("616368697412");
            rowCount = stringData.size();
        } else if (variation.equalsIgnoreCase("testSendValidValueforBinaryColumnAsString")) {
            isStringData = true;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("binary(5)", java.sql.Types.BINARY, 5, 0));

            stringData = new ArrayList<>();
            stringData.add("010101");
            rowCount = stringData.size();
        } else if (variation.equalsIgnoreCase("testSendValidValueforDatetime3ColumnAsLocalDateTime")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("datetime2(3)", java.sql.Types.TIMESTAMP, 23, 0));

            datetime3Data = new ArrayList<>();
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 0, 0));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 0));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 0));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 3000000));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 100000000));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 123000000));
            datetime3Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 999000000));
            rowCount = datetime3Data.size();
        } else if (variation.equalsIgnoreCase("testSendValidValueforDatetime7ColumnAsLocalDateTime")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("datetime2(7)", java.sql.Types.TIMESTAMP, 27, 0));

            datetime7Data = new ArrayList<>();
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 0, 0));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 0));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 0));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 700));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 3000000));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 100000000));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 123000000));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 123456700));
            datetime7Data.add(LocalDateTime.of(2021, 1, 1, 12, 30, 44, 999999900));
            rowCount = datetime7Data.size();
        } else if (variation.equalsIgnoreCase("testSendValidValueforDatetime2ColumnAsLocalTime")) {
            isStringData = false;
            columnMetadata = new HashMap<>();

            columnMetadata.put(1, new ColumnMetadata("datetime2(7)", java.sql.Types.TIME, 16, 0));

            timeData = new ArrayList<>();
            timeData.add(LocalTime.of(0, 0));
            timeData.add(LocalTime.of(12, 0));
            timeData.add(LocalTime.of(12, 30));
            timeData.add(LocalTime.of(12, 30, 44));
            timeData.add(LocalTime.of(12, 30, 44, 700));
            timeData.add(LocalTime.of(12, 30, 44, 3000000));
            timeData.add(LocalTime.of(12, 30, 44, 100000000));
            timeData.add(LocalTime.of(12, 30, 44, 123000000));
            timeData.add(LocalTime.of(12, 30, 44, 123456700));
            timeData.add(LocalTime.of(12, 30, 44, 999999900));
            rowCount = timeData.size();
        }

        counter = 0;
    }

    @Override
    public Set<Integer> getColumnOrdinals() {
        return columnMetadata.keySet();
    }

    @Override
    public String getColumnName(int column) {
        return columnMetadata.get(column).columnName;
    }

    @Override
    public int getColumnType(int column) {
        return columnMetadata.get(column).columnType;
    }

    @Override
    public int getPrecision(int column) {
        return columnMetadata.get(column).precision;
    }

    @Override
    public int getScale(int column) {
        return columnMetadata.get(column).scale;
    }

    @Override
    public Object[] getRowData() throws SQLServerException {
        Object[] dataRow = new Object[columnMetadata.size()];
        if (isStringData)
            dataRow[0] = stringData.get(counter);
        else {
            if (null != dateData)
                dataRow[0] = dateData.get(counter);
            else if (null != datetime3Data)
                dataRow[0] = datetime3Data.get(counter);
            else if (null != datetime7Data)
                dataRow[0] = datetime7Data.get(counter);
            else if (null != timeData)
                dataRow[0] = timeData.get(counter);
            else if (null != byteData)
                dataRow[0] = byteData.get(counter);
        }
        counter++;
        return dataRow;
    }

    @Override
    public boolean next() throws SQLServerException {
        if (counter < rowCount) {
            return true;
        }
        return false;
    }
}
