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

import java.io.ByteArrayInputStream;
import java.io.CharArrayReader;
import java.net.URI;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.logging.Logger;

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
            for (SqlType type : types) {
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
            for (SqlType type : types) {
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
            types = new ArrayList<>();

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
    
}