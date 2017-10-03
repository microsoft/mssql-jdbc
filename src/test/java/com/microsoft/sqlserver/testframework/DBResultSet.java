/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Arrays;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import com.microsoft.sqlserver.testframework.Utils.DBBinaryStream;
import com.microsoft.sqlserver.testframework.Utils.DBCharacterStream;

/**
 * wrapper class for ResultSet
 * 
 * @author Microsoft
 *
 */

public class DBResultSet extends AbstractParentWrapper implements AutoCloseable {

    // TODO: add cursors
    // TODO: add resultSet level holdability
    // TODO: add concurrency control
    public static final Logger log = Logger.getLogger("DBResultSet");

    public static final int TYPE_DYNAMIC = ResultSet.TYPE_SCROLL_SENSITIVE + 1;
    public static final int CONCUR_OPTIMISTIC = ResultSet.CONCUR_UPDATABLE + 2;
    public static final int TYPE_CURSOR_FORWARDONLY = ResultSet.TYPE_FORWARD_ONLY + 1001;
    public static final int TYPE_FORWARD_ONLY = ResultSet.TYPE_FORWARD_ONLY;
    public static final int CONCUR_READ_ONLY = ResultSet.CONCUR_READ_ONLY;
    public static final int TYPE_SCROLL_INSENSITIVE = ResultSet.TYPE_SCROLL_INSENSITIVE;
    public static final int TYPE_SCROLL_SENSITIVE = ResultSet.TYPE_SCROLL_SENSITIVE;
    public static final int CONCUR_UPDATABLE = ResultSet.CONCUR_UPDATABLE;
    public static final int TYPE_DIRECT_FORWARDONLY = ResultSet.TYPE_FORWARD_ONLY + 1000;

    protected DBTable currentTable;
    public int _currentrow = -1;       // The row this rowset is currently pointing to

    ResultSet resultSet = null;
    DBResultSetMetaData metaData;

    DBResultSet(DBStatement dbstatement,
            ResultSet internal) {
        super(dbstatement, internal, "resultSet");
        resultSet = internal;
    }

    DBResultSet(DBStatement dbstatement,
            ResultSet internal,
            DBTable table) {
        super(dbstatement, internal, "resultSet");
        resultSet = internal;
        currentTable = table;
    }

    DBResultSet(DBPreparedStatement dbpstmt,
            ResultSet internal) {
        super(dbpstmt, internal, "resultSet");
        resultSet = internal;
    }

    /**
     * Close the ResultSet object
     * 
     * @throws SQLException
     */
    public void close() throws SQLException {
        if (null != resultSet) {
            resultSet.close();
        }
    }

    /**
     * 
     * @return true new row is valid
     * @throws SQLException
     */
    public boolean next() throws SQLException {
        _currentrow++;
        return resultSet.next();
    }

    /**
     * 
     * @param index
     * @return Object with the column value
     * @throws SQLException
     */
    public Object getObject(int index) throws SQLException {
        // call individual getters based on type
        return resultSet.getObject(index);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public InputStream getBinaryStream(int x) throws SQLException {
        return resultSet.getBinaryStream(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public InputStream getBinaryStream(String x) throws SQLException {
        return resultSet.getBinaryStream(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public Reader getCharacterStream(int x) throws SQLException {
        return resultSet.getCharacterStream(x);
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public Reader getCharacterStream(String x) throws SQLException {
        return resultSet.getCharacterStream(x);
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public String getString(int index) throws SQLException {
        // call individual getters based on type
        return resultSet.getString(index);
    }

    /**
     * 
     * @param index
     * @return
     */
    public void updateObject(int index) throws SQLException {
        // TODO: update object based on cursor type
    }

    /**
     * 
     * @throws SQLException
     */
    public void verify(DBTable table) throws SQLException {
        currentTable = table;
        metaData = this.getMetaData();
        metaData.verify();

        while (this.next())
            this.verifyCurrentRow(table);
    }

    /**
     * @throws SQLException
     * 
     */
    public void verifyCurrentRow(DBTable table) throws SQLException {
        currentTable = table;
        int totalColumns = ((ResultSet) product()).getMetaData().getColumnCount();

        Class _class = Object.class;
        for (int i = 0; i < totalColumns; i++)
            verifydata(i, _class);
    }

    /**
     * 
     * @param ordinal
     * @param coercion
     * @throws SQLException
     * @throws Exception
     */
    public void verifydata(int ordinal,
            Class coercion) throws SQLException {
        Object expectedData = currentTable.columns.get(ordinal).getRowValue(_currentrow);

        // getXXX - default mapping
        Object retrieved = this.getXXX(ordinal + 1, coercion);

        // Verify
        // TODO: Check the intermittent verification error
        // verifydata(ordinal, coercion, expectedData, retrieved);
    }

    /**
     * verifies data
     * 
     * @param ordinal
     * @param coercion
     * @param expectedData
     * @param retrieved
     * @throws SQLException
     */
    public void verifydata(int ordinal,
            Class coercion,
            Object expectedData,
            Object retrieved) throws SQLException {
        metaData = this.getMetaData();
        switch (metaData.getColumnType(ordinal + 1)) {
            case java.sql.Types.BIGINT:
                assertTrue((((Long) expectedData).longValue() == ((Long) retrieved).longValue()),
                        "Unexpected bigint value, expected: " + (Long) expectedData + " .Retrieved: " + (Long) retrieved);
                break;

            case java.sql.Types.INTEGER:
                assertTrue((((Integer) expectedData).intValue() == ((Integer) retrieved).intValue()), "Unexpected int value, expected : "
                        + (Integer) expectedData + " ,received: " + (Integer) retrieved);
                break;

            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                assertTrue((((Short) expectedData).shortValue() == ((Short) retrieved).shortValue()), "Unexpected smallint/tinyint value, expected: "
                        + " " + (Short) expectedData + " received: " + (Short) retrieved);
                break;

            case java.sql.Types.BIT:
                if (expectedData.equals(1))
                    expectedData = true;
                else
                    expectedData = false;
                assertTrue((((Boolean) expectedData).booleanValue() == ((Boolean) retrieved).booleanValue()), "Unexpected bit value, expected: "
                        + (Boolean) expectedData + " ,received: " + (Boolean) retrieved);
                break;

            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                assertTrue(0 == (((BigDecimal) expectedData).compareTo((BigDecimal) retrieved)), "Unexpected decimal/numeric/money/smallmoney value "
                        + ",expected: " + (BigDecimal) expectedData + " received: " + (BigDecimal) retrieved);
                break;

            case java.sql.Types.DOUBLE:
                assertTrue((((Double) expectedData).doubleValue() == ((Double) retrieved).doubleValue()), "Unexpected float value, expected: "
                        + (Double) expectedData + " received: " + (Double) retrieved);
                break;

            case java.sql.Types.REAL:
                assertTrue((((Float) expectedData).floatValue() == ((Float) retrieved).floatValue()),
                        "Unexpected real value, expected: " + (Float) expectedData + " received: " + (Float) retrieved);
                break;

            case java.sql.Types.VARCHAR:
            case java.sql.Types.NVARCHAR:
                assertTrue((((String) expectedData).trim().equalsIgnoreCase(((String) retrieved).trim())), "Unexpected varchar/nvarchar value, "
                        + "expected:  " + ((String) expectedData).trim() + " ,received: " + ((String) retrieved).trim());
                break;
            case java.sql.Types.CHAR:
            case java.sql.Types.NCHAR:

                assertTrue((((String) expectedData).trim().equalsIgnoreCase(((String) retrieved).trim())), "Unexpected char/nchar value, "
                        + "expected:  " + ((String) expectedData).trim() + " ,received: " + ((String) retrieved).trim());
                break;

            case java.sql.Types.TIMESTAMP:
                if (metaData.getColumnTypeName(ordinal + 1).equalsIgnoreCase("datetime")) {
                    assertTrue((((Timestamp) roundDatetimeValue(expectedData)).getTime() == (((Timestamp) retrieved).getTime())),
                            "Unexpected datetime value, expected: " + ((Timestamp) roundDatetimeValue(expectedData)).getTime() + " , received: "
                                    + (((Timestamp) retrieved).getTime()));
                    break;
                }
                else if (metaData.getColumnTypeName(ordinal + 1).equalsIgnoreCase("smalldatetime")) {
                    assertTrue((((Timestamp) roundSmallDateTimeValue(expectedData)).getTime() == (((Timestamp) retrieved).getTime())),
                            "Unexpected smalldatetime value, expected: " + ((Timestamp) roundSmallDateTimeValue(expectedData)).getTime()
                                    + " ,received: " + (((Timestamp) retrieved).getTime()));
                    break;
                }
                else
                    assertTrue(("" + Timestamp.valueOf((LocalDateTime) expectedData)).equalsIgnoreCase("" + retrieved), "Unexpected datetime2 value, "
                            + "expected: " + Timestamp.valueOf((LocalDateTime) expectedData) + " ,received: " + retrieved);
                break;

            case java.sql.Types.DATE:
                assertTrue((("" + expectedData).equalsIgnoreCase("" + retrieved)),
                        "Unexpected date value, expected: " + expectedData + " ,received: " + retrieved);
                break;

            case java.sql.Types.TIME:
                assertTrue(("" + Time.valueOf((LocalTime) expectedData)).equalsIgnoreCase("" + retrieved),
                        "Unexpected time value, exptected: " + Time.valueOf((LocalTime) expectedData) + " ,received: " + retrieved);
                break;

            case microsoft.sql.Types.DATETIMEOFFSET:
                assertTrue(("" + expectedData).equals("" + retrieved),
                        " unexpected DATETIMEOFFSET value, expected: " + expectedData + " ,received: " + retrieved);
                break;

            case java.sql.Types.BINARY:
                assertTrue(Utils.parseByte((byte[]) expectedData, (byte[]) retrieved),
                        " unexpected BINARY value, expected: " + expectedData + " ,received: " + retrieved);
                break;

            case java.sql.Types.VARBINARY:
                assertTrue(Arrays.equals((byte[]) expectedData, (byte[]) retrieved),
                        " unexpected BINARY value, expected: " + expectedData + " ,received: " + retrieved);
                break;
            default:
                fail("Unhandled JDBCType " + JDBCType.valueOf(metaData.getColumnType(ordinal + 1)));
                break;
        }
    }

    /**
     * 
     * @param idx
     * @param coercion
     * @return
     * @throws SQLException
     */
    public Object getXXX(Object idx,
            Class coercion) throws SQLException {
        int intOrdinal = 0;
        String strOrdinal = "";
        boolean isInteger = false;

        if (idx == null) {
            strOrdinal = null;
        }
        else if (idx instanceof Integer) {
            isInteger = true;
            intOrdinal = (Integer) idx;
        }
        else {
            // Otherwise
            throw new SQLException("Unhandled ordinal type: " + idx.getClass());
        }

        if (coercion == Object.class) {
            return this.getObject(intOrdinal);
        }
        else if (coercion == DBBinaryStream.class) {
            return isInteger ? this.getBinaryStream(intOrdinal) : this.getBinaryStream(strOrdinal);
        }
        else if (coercion == DBCharacterStream.class) {
            return isInteger ? this.getCharacterStream(intOrdinal) : this.getCharacterStream(strOrdinal);
        }
        else {
            if (log.isLoggable(Level.FINE)) {
                log.fine("coercion not supported! ");
            }
            else {
                log.fine("coercion + " + coercion.toString() + " is not supported!");
            }
        }
        return null;
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public DBResultSetMetaData getMetaData() throws SQLException {
        DBResultSetMetaData metaData = new DBResultSetMetaData(this);
        return metaData.getMetaData();
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public int getRow() throws SQLException {
        int product = ((ResultSet) product()).getRow();
        return product;
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public boolean previous() throws SQLException {

        boolean validrow = ((ResultSet) product()).previous();

        if (_currentrow > 0) {
            _currentrow--;
        }
        return (validrow);
    }

    /**
     * 
     * @throws SQLException
     */
    public void afterLast() throws SQLException {
        ((ResultSet) product()).afterLast();
        _currentrow = currentTable.getTotalRows();
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean absolute(int x) throws SQLException {
        boolean validrow = ((ResultSet) product()).absolute(x);
        _currentrow = x - 1;
        return validrow;
    }

    private static Object roundSmallDateTimeValue(Object value) {
        if (value == null) {
            return null;
        }

        Calendar cal;
        java.sql.Timestamp ts = null;
        int nanos = -1;

        if (value instanceof Calendar) {
            cal = (Calendar) value;
        }
        else {
            ts = (java.sql.Timestamp) value;
            cal = Calendar.getInstance();
            cal.setTimeInMillis(ts.getTime());
            nanos = ts.getNanos();
        }

        // round to the nearest minute
        double seconds = cal.get(Calendar.SECOND) + (nanos == -1 ? ((double) cal.get(Calendar.MILLISECOND) / 1000) : ((double) nanos / 1000000000));
        if (seconds > 29.998) {
            cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1);
        }
        cal.set(Calendar.SECOND, 0);
        cal.set(Calendar.MILLISECOND, 0);
        nanos = 0;

        // required to force computation
        cal.getTimeInMillis();

        // return appropriate value
        if (value instanceof Calendar) {
            return cal;
        }
        else {
            ts.setTime(cal.getTimeInMillis());
            ts.setNanos(nanos);
            return ts;
        }
    }

    private static Object roundDatetimeValue(Object value) {
        if (value == null)
            return null;
        Timestamp ts = value instanceof Timestamp ? (Timestamp) value : new Timestamp(((Calendar) value).getTimeInMillis());
        int millis = ts.getNanos() / 1000000;
        int lastDigit = (int) (millis % 10);
        switch (lastDigit) {
            // 0, 1 -> 0
            case 1:
                ts.setNanos((millis - 1) * 1000000);
                break;

            // 2, 3, 4 -> 3
            case 2:
                ts.setNanos((millis + 1) * 1000000);
                break;
            case 4:
                ts.setNanos((millis - 1) * 1000000);
                break;

            // 5, 6, 7, 8 -> 7
            case 5:
                ts.setNanos((millis + 2) * 1000000);
                break;
            case 6:
                ts.setNanos((millis + 1) * 1000000);
                break;
            case 8:
                ts.setNanos((millis - 1) * 1000000);
                break;

            // 9 -> 0 with overflow
            case 9:
                ts.setNanos(0);
                ts.setTime(ts.getTime() + millis + 1);
                break;

            // default, i.e. 0, 3, 7 -> 0, 3, 7
            // don't change the millis but make sure that any
            // sub-millisecond digits are zeroed out
            default:
                ts.setNanos((millis) * 1000000);
        }
        if (value instanceof Calendar) {
            ((Calendar) value).setTimeInMillis(ts.getTime());
            ((Calendar) value).getTimeInMillis();
            return value;
        }
        return ts;
    }

    /**
     * @param i
     * @return
     * @throws SQLException
     */
    public int getInt(int index) throws SQLException {
        return resultSet.getInt(index);
    }

    /**
     * 
     * @return
     */
    public DBStatement statement() {
        if (parent instanceof DBStatement) {
            return ((DBStatement) parent);
        }
        return (null);
    }

}