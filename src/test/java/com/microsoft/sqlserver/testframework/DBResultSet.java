// ---------------------------------------------------------------------------------------------------------------------------------
// File: DBResultSet.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.Calendar;

/**
 * wrapper class for ResultSet
 * 
 * @author Microsoft
 *
 */

public class DBResultSet extends AbstractParentWrapper {

    // TODO: add cursors
    // TODO: add resultSet level holdability
    // TODO: add concurrency control

    public static final int TYPE_DYNAMIC = ResultSet.TYPE_SCROLL_SENSITIVE + 1;
    public static final int CONCUR_OPTIMISTIC = ResultSet.CONCUR_UPDATABLE + 2;
    public static final int TYPE_CURSOR_FORWARDONLY = ResultSet.TYPE_FORWARD_ONLY + 1001;
    public static final int TYPE_FORWARD_ONLY = ResultSet.TYPE_FORWARD_ONLY;
    public static final int CONCUR_READ_ONLY = ResultSet.CONCUR_READ_ONLY;
    public static final int TYPE_SCROLL_INSENSITIVE = ResultSet.TYPE_SCROLL_INSENSITIVE;
    public static final int TYPE_SCROLL_SENSITIVE = ResultSet.TYPE_SCROLL_SENSITIVE;
    public static final int CONCUR_UPDATABLE = ResultSet.CONCUR_UPDATABLE;

    protected DBTable currentTable;
    public int _currentrow = -1;       // The row this rowset is currently pointing to

    ResultSet resultSet = null;
    DBResultSetMetaData metaData;

    DBResultSet(DBStatement dbstatement, ResultSet internal) {
        super(dbstatement, internal, "resultSet");
        resultSet = internal;
    }

    DBResultSet(DBPreparedStatement dbpstmt, ResultSet internal) {
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
            verifydata(i, _class, null);

    }

    /**
     * 
     * @param ordinal
     * @param coercion
     * @param arg
     * @throws SQLException
     */
    public void verifydata(int ordinal, Class coercion, Object arg) throws SQLException {
        Object expectedData = currentTable.columns.get(ordinal).getRowValue(_currentrow);

        // getXXX - default mapping
        Object retrieved = this.getXXX(ordinal + 1, coercion);

        // Verify
        verifydata(ordinal, coercion, expectedData, retrieved);
    }

    public void verifydata(int ordinal, Class coercion, Object expectedData, Object retrieved) throws SQLException {
        metaData = this.getMetaData();
        switch (metaData.getColumnType(ordinal + 1)) {
            case java.sql.Types.BIGINT:
                assertTrue((((Long) expectedData).longValue() == ((Long) retrieved).longValue()),
                        "Unexpected bigint value, expected: " + ((Long) expectedData).longValue() + " .Retrieved: " + ((Long) retrieved).longValue());
                break;

            case java.sql.Types.INTEGER:
                assertTrue((((Integer) expectedData).intValue() == ((Integer) retrieved).intValue()), "Unexpected int value, expected : "
                        + ((Integer) expectedData).intValue() + " ,received: " + ((Integer) retrieved).intValue());
                break;

            case java.sql.Types.SMALLINT:
            case java.sql.Types.TINYINT:
                assertTrue((((Short) expectedData).shortValue() == ((Short) retrieved).shortValue()), "Unexpected smallint/tinyint value, expected: "
                        + " " + ((Short) expectedData).shortValue() + " received: " + ((Short) retrieved).shortValue());
                break;

            case java.sql.Types.BIT:
                if (expectedData.equals(1))
                    expectedData = true;
                else
                    expectedData = false;
                assertTrue((((Boolean) expectedData).booleanValue() == ((Boolean) retrieved).booleanValue()), "Unexpected bit value, expected: "
                        + ((Boolean) expectedData).booleanValue() + " ,received: " + ((Boolean) retrieved).booleanValue());
                break;

            case java.sql.Types.DECIMAL:
            case java.sql.Types.NUMERIC:
                assertTrue(0 == (((BigDecimal) expectedData).compareTo((BigDecimal) retrieved)), "Unexpected decimal/numeric/money/smallmoney value "
                        + ",expected: " + (BigDecimal) expectedData + " received: " + (BigDecimal) retrieved);
                break;

            case java.sql.Types.DOUBLE:
                assertTrue((((Double) expectedData).doubleValue() == ((Double) retrieved).doubleValue()), "Unexpected float value, expected: "
                        + ((Double) expectedData).doubleValue() + " received: " + ((Double) retrieved).doubleValue());
                break;

            case java.sql.Types.REAL:
                assertTrue((((Float) expectedData).floatValue() == ((Float) retrieved).floatValue()),
                        "Unexpected real value, expected: " + ((Float) expectedData).floatValue() + " received: " + ((Float) retrieved).floatValue());
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

            default:
                fail("Unhandled JDBCType " + JDBCType.valueOf(metaData.getColumnType(ordinal + 1)));
                break;
        }
    }

    private Object getXXX(int idx, Class coercion) throws SQLException {
        if (coercion == Object.class) {
            return this.getObject(idx);
        }
        return null;
    }

    /**
     * 
     * @return
     */
    public DBResultSetMetaData getMetaData() {
        ResultSetMetaData product = null;
        DBResultSetMetaData wrapper = null;
        try {
            product = resultSet.getMetaData();
            wrapper = new DBResultSetMetaData(parent, product, name);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }

        return wrapper;
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
        _currentrow = DBTable.getTotalRows();
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
}
