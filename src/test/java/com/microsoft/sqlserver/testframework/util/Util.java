package com.microsoft.sqlserver.testframework.util;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;

/**
 * Utility class for testing
 */
public class Util {

    /**
     * Utility method for generating a prepared statement
     * 
     * @param connection
     *            connection object
     * @param sql
     *            SQL string
     * @param stmtColEncSetting
     *            SQLServerStatementColumnEncryptionSetting object
     * @return
     */
    public static PreparedStatement getPreparedStmt(Connection connection,
            String sql,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException {
        if (null == stmtColEncSetting) {
            return ((SQLServerConnection) connection).prepareStatement(sql);
        }
        else {
            return ((SQLServerConnection) connection).prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    connection.getHoldability(), stmtColEncSetting);
        }
    }

    /**
     * Utility method for a statement
     * 
     * @param connection
     *            connection object
     * @param sql
     *            SQL string
     * @param stmtColEncSetting
     *            SQLServerStatementColumnEncryptionSetting object
     * @return
     */
    public static Statement getStatement(Connection connection,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException {
        // default getStatement assumes resultSet is type_forward_only and concur_read_only
        if (null == stmtColEncSetting) {
            return ((SQLServerConnection) connection).createStatement();
        }
        else {
            return ((SQLServerConnection) connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    connection.getHoldability(), stmtColEncSetting);
        }
    }

    /**
     * Utility method for a scrollable statement
     * 
     * @param connection
     *            connection object
     * @return
     */
    public static Statement getScrollableStatement(Connection connection) throws SQLException {
        return ((SQLServerConnection) connection).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
    }

    /**
     * Utility method for a scrollable statement
     * 
     * @param connection
     *            connection object
     * @param stmtColEncSetting
     *            SQLServerStatementColumnEncryptionSetting object
     * @return
     */
    public static Statement getScrollableStatement(Connection connection,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException {
        return ((SQLServerConnection) connection).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE,
                ResultSet.CONCUR_UPDATABLE, stmtColEncSetting);
    }

    /**
     * Utility method for a statement
     * 
     * @param connection
     *            connection object
     * @param stmtColEncSetting
     *            SQLServerStatementColumnEncryptionSetting object
     * @param rsScrollSensitivity
     * @param rsConcurrence
     * @return
     */
    public static Statement getStatement(Connection connection,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting,
            int rsScrollSensitivity,
            int rsConcurrence) throws SQLException {
        // overloaded getStatement allows setting resultSet type
        if (null == stmtColEncSetting) {
            return ((SQLServerConnection) connection).createStatement(rsScrollSensitivity, rsConcurrence, connection.getHoldability());
        }
        else {
            return ((SQLServerConnection) connection).createStatement(rsScrollSensitivity, rsConcurrence, connection.getHoldability(),
                    stmtColEncSetting);
        }
    }

    /**
     * Utility method for a callable statement
     * 
     * @param connection
     *            connection object
     * @param stmtColEncSetting
     *            SQLServerStatementColumnEncryptionSetting object
     * @param sql
     * @return
     */
    public static CallableStatement getCallableStmt(Connection connection,
            String sql,
            SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException {
        if (null == stmtColEncSetting) {
            return ((SQLServerConnection) connection).prepareCall(sql);
        }
        else {
            return ((SQLServerConnection) connection).prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY,
                    connection.getHoldability(), stmtColEncSetting);
        }
    }

    /**
     * Utility method for a datetime value
     * 
     * @param value
     * @return
     */
    public static Object roundSmallDateTimeValue(Object value) {
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

    /**
     * Utility method for a datetime value
     * 
     * @param value
     * @return
     */
    public static Object roundDatetimeValue(Object value) {
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
     * Utility function for safely closing open resultset/statement/connection
     * 
     * @param ResultSet
     * @param Statement
     * @param Connection
     */
    public static void close(ResultSet rs,
            Statement stmt,
            Connection con) {
        if (rs != null) {
            try {
                rs.close();

            }
            catch (SQLException e) {
                System.out.println("The result set cannot be closed.");
            }
        }
        if (stmt != null) {
            try {
                stmt.close();
            }
            catch (SQLException e) {
                System.out.println("The statement cannot be closed.");
            }
        }
        if (con != null) {
            try {
                con.close();
            }
            catch (SQLException e) {
                System.out.println("The data source connection cannot be closed.");
            }
        }
    }

    /**
     * Utility function for checking if the system supports JDBC 4.2
     * 
     * @param con
     * @return
     */
    public static boolean supportJDBC42(Connection con) throws SQLException {
        SQLServerDatabaseMetaData meta = (SQLServerDatabaseMetaData) con.getMetaData();
        return (meta.getJDBCMajorVersion() >= 4 && meta.getJDBCMinorVersion() >= 2);
    }

    /**
     * Utility function for checking if the system supports JDBC 4.3
     * 
     * @param con
     * @return
     */
    public static boolean supportJDBC43(Connection con) throws SQLException {
        SQLServerDatabaseMetaData meta = (SQLServerDatabaseMetaData) con.getMetaData();
        return (meta.getJDBCMajorVersion() >= 4 && meta.getJDBCMinorVersion() >= 3);
    }

    /**
     * 
     * @param b
     *            byte value
     * @param length
     *            length of the array
     * @return
     */
    final static char[] hexChars = {'0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E', 'F'};

    public static String bytesToHexString(byte[] b,
            int length) {
        StringBuilder sb = new StringBuilder(length * 2);
        for (int i = 0; i < length; i++) {
            int hexVal = b[i] & 0xFF;
            sb.append(hexChars[(hexVal & 0xF0) >> 4]);
            sb.append(hexChars[(hexVal & 0x0F)]);
        }
        return sb.toString();
    }

    /**
     * conversion routine valid values 0-9 a-f A-F throws exception when failed to convert
     * 
     * @param value
     *            charArray
     * @return
     * @throws SQLException
     */
    static byte CharToHex(char value) throws SQLException {
        byte ret = 0;
        if (value >= 'A' && value <= 'F') {
            ret = (byte) (value - 'A' + 10);
        }
        else if (value >= 'a' && value <= 'f') {
            ret = (byte) (value - 'a' + 10);
        }
        else if (value >= '0' && value <= '9') {
            ret = (byte) (value - '0');
        }
        else {
            throw new IllegalArgumentException("The string  is not in a valid hex format. ");
        }
        return ret;
    }

    /**
     * Converts a string to an array of bytes
     * 
     * @param hexV
     *            a hexized string representation of bytes
     * @return
     * @throws SQLException
     */
    public static byte[] hexStringToByte(String hexV) throws SQLException {
        int len = hexV.length();
        char orig[] = hexV.toCharArray();
        if ((len % 2) != 0) {
            throw new IllegalArgumentException("The string is not in a valid hex format: " + hexV);
        }
        byte[] bin = new byte[len / 2];
        for (int i = 0; i < len / 2; i++) {
            bin[i] = (byte) ((CharToHex(orig[2 * i]) << 4) + CharToHex(orig[2 * i + 1]));
        }
        return bin;
    }
}
