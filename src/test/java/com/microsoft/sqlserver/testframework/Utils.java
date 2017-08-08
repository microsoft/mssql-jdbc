/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.net.URI;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerDatabaseMetaData;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;
import com.microsoft.sqlserver.testframework.sqlType.SqlBigInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlBinary;
import com.microsoft.sqlserver.testframework.sqlType.SqlBit;
import com.microsoft.sqlserver.testframework.sqlType.SqlChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlDate;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTime;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTime2;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTimeOffset;
import com.microsoft.sqlserver.testframework.sqlType.SqlDecimal;
import com.microsoft.sqlserver.testframework.sqlType.SqlFloat;
import com.microsoft.sqlserver.testframework.sqlType.SqlInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlMoney;
import com.microsoft.sqlserver.testframework.sqlType.SqlNChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlNVarChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlNVarCharMax;
import com.microsoft.sqlserver.testframework.sqlType.SqlNumeric;
import com.microsoft.sqlserver.testframework.sqlType.SqlReal;
import com.microsoft.sqlserver.testframework.sqlType.SqlSmallDateTime;
import com.microsoft.sqlserver.testframework.sqlType.SqlSmallInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlSmallMoney;
import com.microsoft.sqlserver.testframework.sqlType.SqlTime;
import com.microsoft.sqlserver.testframework.sqlType.SqlTinyInt;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarBinary;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarBinaryMax;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarChar;
import com.microsoft.sqlserver.testframework.sqlType.SqlVarCharMax;

/**
 * Generic Utility class which we can access by test classes.
 * 
 * @since 6.1.2
 */
public class Utils {
    public static final Logger log = Logger.getLogger("Utils");

    // 'SQL' represents SQL Server, while 'SQLAzure' represents SQL Azure.
    public static final String SERVER_TYPE_SQL_SERVER = "SQL";
    public static final String SERVER_TYPE_SQL_AZURE = "SQLAzure";
    // private static SqlType types = null;
    private static ArrayList<SqlType> types = null;
    
    private final static int SQL_SERVER_2012_VERSION = 11;

    /**
     * Returns serverType
     * 
     * @return
     */
    public static String getServerType() {
        String serverType = null;

        String serverTypeProperty = getConfiguredProperty("server.type");
        if (null == serverTypeProperty) {
            // default to SQL Server
            serverType = SERVER_TYPE_SQL_SERVER;
        }
        else if (serverTypeProperty.equalsIgnoreCase(SERVER_TYPE_SQL_AZURE)) {
            serverType = SERVER_TYPE_SQL_AZURE;
        }
        else if (serverTypeProperty.equalsIgnoreCase(SERVER_TYPE_SQL_SERVER)) {
            serverType = SERVER_TYPE_SQL_SERVER;
        }
        else {
            if (log.isLoggable(Level.FINE)) {
                log.fine("Server.type '" + serverTypeProperty + "' is not supported yet. Default to SQL Server");
            }
            serverType = SERVER_TYPE_SQL_SERVER;
        }
        return serverType;
    }

    /**
     * Read variable from property files if found null try to read from env.
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key) {
        String value = System.getProperty(key);

        if (value == null) {
            value = System.getenv(key);
        }

        return value;
    }

    /**
     * Convenient method for {@link #getConfiguredProperty(String)}
     * 
     * @param key
     * @return Value
     */
    public static String getConfiguredProperty(String key,
            String defaultValue) {
        String value = getConfiguredProperty(key);

        if (value == null) {
            value = defaultValue;
        }

        return value;
    }

    /**
     * 
     * @param javatype
     * @return
     */
    public static SqlType find(Class javatype) {
        if (null != types) {
            types();
            for (int i = 0; i < types.size(); i++) {
                SqlType type = types.get(i);
                if (type.getType() == javatype)
                    return type;
            }
        }
        return null;
    }

    /**
     * 
     * @param name
     * @return
     */
    public static SqlType find(String name) {
        if (null == types)
            types();
        if (null != types) {
            for (int i = 0; i < types.size(); i++) {
                SqlType type = types.get(i);
                if (type.getName().equalsIgnoreCase(name))
                    return type;
            }
        }
        return null;
    }

    /**
     * 
     * @return
     */
    public static ArrayList<SqlType> types() {
        if (null == types) {
            types = new ArrayList<SqlType>();

            types.add(new SqlInt());
            types.add(new SqlSmallInt());
            types.add(new SqlTinyInt());
            types.add(new SqlBit());
            types.add(new SqlDateTime());
            types.add(new SqlSmallDateTime());
            types.add(new SqlDecimal());
            types.add(new SqlNumeric());
            types.add(new SqlReal());
            types.add(new SqlFloat());
            types.add(new SqlMoney());
            types.add(new SqlSmallMoney());
            types.add(new SqlVarChar());
            types.add(new SqlChar());
            // types.add(new SqlText());
            types.add(new SqlBinary());
            types.add(new SqlVarBinary());
            // types.add(new SqlImage());
            // types.add(new SqlTimestamp());

            types.add(new SqlNVarChar());
            types.add(new SqlNChar());
            // types.add(new SqlNText());
            // types.add(new SqlGuid());

            types.add(new SqlBigInt());
            // types.add(new SqlVariant(this));

            // 9.0 types
            types.add(new SqlVarCharMax());
            types.add(new SqlNVarCharMax());
            types.add(new SqlVarBinaryMax());
            // types.add(new SqlXml());

            // 10.0 types
            types.add(new SqlDate());
            types.add(new SqlDateTime2());
            types.add(new SqlTime());
            types.add(new SqlDateTimeOffset());
        }
        return types;
    }

    /**
     * Wrapper Class for BinaryStream
     *
     */
    public static class DBBinaryStream extends ByteArrayInputStream {
        byte[] data;

        // Constructor
        public DBBinaryStream(byte[] value) {
            super(value);
            data = value;
        }

    }

    /**
     * Wrapper for CharacterStream
     *
     */
    public static class DBCharacterStream extends CharArrayReader {
        String localValue;

        /**
         * Constructor
         * 
         * @param value
         */
        public DBCharacterStream(String value) {
            super(value.toCharArray());
            localValue = value;
        }

    }

    /**
     * Wrapper for NCharacterStream
     */
    class DBNCharacterStream extends DBCharacterStream {
        // Constructor
        public DBNCharacterStream(String value) {
            super(value);
        }
    }
    
    /**
     * 
     * @return location of resource file
     */
    public static String getCurrentClassPath() {
        try {
            String className = new Object() {
            }.getClass().getEnclosingClass().getName();
            String location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath() + "/";
            URI uri = new URI(location.toString());
            return uri.getPath();
        }
        catch (Exception e) {
            fail("Failed to get CSV file path. " + e.getMessage());
        }
        return null;
    }

    /**
     * mimic "DROP TABLE IF EXISTS ..." for older versions of SQL Server
     */
    public static void dropTableIfExists(String tableName, java.sql.Statement stmt) throws SQLException {
        dropObjectIfExists(tableName, "IsTable", stmt);
    }

    /**
     * mimic "DROP PROCEDURE IF EXISTS ..." for older versions of SQL Server
     */
    public static void dropProcedureIfExists(String procName, java.sql.Statement stmt) throws SQLException {
        dropObjectIfExists(procName, "IsProcedure", stmt);
    }

    /**
     * actually perform the "DROP TABLE / PROCEDURE"
     */
    private static void dropObjectIfExists(String objectName, String objectProperty, java.sql.Statement stmt) throws SQLException {
        StringBuilder sb = new StringBuilder();
        if (!objectName.startsWith("[")) { sb.append("["); }
        sb.append(objectName);
        if (!objectName.endsWith("]")) { sb.append("]"); }
        String bracketedObjectName = sb.toString();
        String sql = String.format(
                "IF EXISTS " +
                        "( " +
                            "SELECT * from sys.objects " +
                            "WHERE object_id = OBJECT_ID(N'%s') AND OBJECTPROPERTY(object_id, N'%s') = 1 " +
                        ") " +
                    "DROP %s %s ",
                bracketedObjectName,
                objectProperty,
                "IsProcedure".equals(objectProperty)  ? "PROCEDURE" : "TABLE",
                bracketedObjectName);
        stmt.executeUpdate(sql);
    }

    public static boolean parseByte(byte[] expectedData,
            byte[] retrieved) {
        assertTrue(Arrays.equals(expectedData, Arrays.copyOf(retrieved, expectedData.length)), " unexpected BINARY value, expected");
        for (int i = expectedData.length; i < retrieved.length; i++) {
            assertTrue(0 == retrieved[i], "unexpected data BINARY");
        }
        return true;
    }
    
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
     * With Java 7, skip tests
     */
    public static void skipTestForJava7(SQLServerConnection con) throws TestAbortedException, SQLException {
        assumeTrue(supportJDBC42(con));
    }

    /**
     * skip tests on SQL Server 2008
     */
    public static void skipTestForSQLServer2008(SQLServerConnection con) throws TestAbortedException, SQLException {

        DatabaseMetaData meta = con.getMetaData();
        String serverVersionString = meta.getDatabaseProductVersion();

        String[] versions = serverVersionString.split("\\.");
        int serverVersion = Integer.parseInt(versions[0]);

        assumeTrue(serverVersion >= SQL_SERVER_2012_VERSION, "Skipping test case on SQL Server 2008.");
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
}