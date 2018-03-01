/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.resultset;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLXML;
import java.sql.Statement;
import java.util.UUID;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerResultSet;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class ResultSetTest extends AbstractTest {
    private static final String tableName = "[" + RandomUtil.getIdentifier("StatementParam") + "]";

    /**
     * Tests proper exception for unsupported operation
     * 
     * @throws SQLException
     */
    @Test
    public void testJdbc41ResultSetMethods() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString);
             Statement stmt = con.createStatement()) {
            stmt.executeUpdate("create table " + tableName + " ( "
                    + "col1 int, "
                    + "col2 varchar(512), "
                    + "col3 float, "
                    + "col4 decimal(10,5), "
                    + "col5 uniqueidentifier, "
                    + "col6 xml, "
                    + "col7 varbinary(max), "
                    + "col8 text, "
                    + "col9 ntext, "
                    + "col10 varbinary(max), "
                    + "col11 date, "
                    + "col12 time, "
                    + "col13 datetime2, "
                    + "col14 datetimeoffset, "
                    + "order_column int identity(1,1) primary key)");
            try {
    
                stmt.executeUpdate("Insert into " + tableName + " values("
                        + "1, " // col1
                        + "'hello', " // col2
                        + "2.0, " // col3
                        + "123.45, " // col4
                        + "'6F9619FF-8B86-D011-B42D-00C04FC964FF', " // col5
                        + "'<test/>', " // col6
                        + "0x63C34D6BCAD555EB64BF7E848D02C376, " // col7
                        + "'text', " // col8
                        + "'ntext', " // col9
                        + "0x63C34D6BCAD555EB64BF7E848D02C376," // col10
                        + "'2017-05-19'," // col11
                        + "'10:47:15.1234567'," // col12
                        + "'2017-05-19T10:47:15.1234567'," // col13
                        + "'2017-05-19T10:47:15.1234567+02:00'" // col14
                        + ")");
    
                stmt.executeUpdate("Insert into " + tableName + " values("
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null, "
                        + "null)");
    
                try (ResultSet rs = stmt.executeQuery("select * from " + tableName + " order by order_column")) {
                    // test non-null values
                    assertTrue(rs.next());
                    assertEquals(Byte.valueOf((byte) 1), rs.getObject(1, Byte.class));
                    assertEquals(Byte.valueOf((byte) 1), rs.getObject("col1", Byte.class));
                    assertEquals(Short.valueOf((short) 1), rs.getObject(1, Short.class));
                    assertEquals(Short.valueOf((short) 1), rs.getObject("col1", Short.class));
                    assertEquals(Integer.valueOf(1), rs.getObject(1, Integer.class));
                    assertEquals(Integer.valueOf(1), rs.getObject("col1", Integer.class));
                    assertEquals(Long.valueOf(1), rs.getObject(1, Long.class));
                    assertEquals(Long.valueOf(1), rs.getObject("col1", Long.class));
                    assertEquals(Boolean.TRUE, rs.getObject(1, Boolean.class));
                    assertEquals(Boolean.TRUE, rs.getObject("col1", Boolean.class));

                    assertEquals("hello", rs.getObject(2, String.class));
                    assertEquals("hello", rs.getObject("col2", String.class));

                    assertEquals(2.0f, rs.getObject(3, Float.class), 0.0001f);
                    assertEquals(2.0f, rs.getObject("col3", Float.class), 0.0001f);
                    assertEquals(2.0d, rs.getObject(3, Double.class), 0.0001d);
                    assertEquals(2.0d, rs.getObject("col3", Double.class), 0.0001d);

                    // BigDecimal#equals considers the number of decimal places
                    assertEquals(0, rs.getObject(4, BigDecimal.class).compareTo(new BigDecimal("123.45")));
                    assertEquals(0, rs.getObject("col4", BigDecimal.class).compareTo(new BigDecimal("123.45")));

                    assertEquals(UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF"), rs.getObject(5, UUID.class));
                    assertEquals(UUID.fromString("6F9619FF-8B86-D011-B42D-00C04FC964FF"), rs.getObject("col5", UUID.class));

                    SQLXML sqlXml;
                    sqlXml = rs.getObject(6, SQLXML.class);
                    try {
                        assertEquals("<test/>", sqlXml.getString());
                    } finally {
                        sqlXml.free();
                    }

                    Blob blob;
                    blob = rs.getObject(7, Blob.class);
                    try {
                        assertArrayEquals(new byte[] {0x63, (byte) 0xC3, 0x4D, 0x6B, (byte) 0xCA, (byte) 0xD5, 0x55, (byte) 0xEB, 0x64, (byte) 0xBF, 0x7E, (byte) 0x84, (byte) 0x8D, 0x02, (byte) 0xC3, 0x76},
                                blob.getBytes(1, 16));
                    } finally {
                        blob.free();
                    }

                    Clob clob;
                    clob = rs.getObject(8, Clob.class);
                    try {
                        assertEquals("text", clob.getSubString(1, 4));
                    } finally {
                        clob.free();
                    }

                    NClob nclob;
                    nclob = rs.getObject(9, NClob.class);
                    try {
                        assertEquals("ntext", nclob.getSubString(1, 5));
                    } finally {
                        nclob.free();
                    }

                    assertArrayEquals(new byte[] {0x63, (byte) 0xC3, 0x4D, 0x6B, (byte) 0xCA, (byte) 0xD5, 0x55, (byte) 0xEB, 0x64, (byte) 0xBF, 0x7E, (byte) 0x84, (byte) 0x8D, 0x02, (byte) 0xC3, 0x76},
                            rs.getObject(10, byte[].class));

                    assertEquals(java.sql.Date.valueOf("2017-05-19"), rs.getObject(11, java.sql.Date.class));
                    assertEquals(java.sql.Date.valueOf("2017-05-19"), rs.getObject("col11", java.sql.Date.class));

                    java.sql.Time expectedTime = new java.sql.Time(java.sql.Time.valueOf("10:47:15").getTime() + 123L);
                    assertEquals(expectedTime, rs.getObject(12, java.sql.Time.class));
                    assertEquals(expectedTime, rs.getObject("col12", java.sql.Time.class));

                    assertEquals(java.sql.Timestamp.valueOf("2017-05-19 10:47:15.1234567"), rs.getObject(13, java.sql.Timestamp.class));
                    assertEquals(java.sql.Timestamp.valueOf("2017-05-19 10:47:15.1234567"), rs.getObject("col13", java.sql.Timestamp.class));

                    assertEquals("2017-05-19 10:47:15.1234567 +02:00", rs.getObject(14, microsoft.sql.DateTimeOffset.class).toString());
                    assertEquals("2017-05-19 10:47:15.1234567 +02:00", rs.getObject("col14", microsoft.sql.DateTimeOffset.class).toString());


                    // test null values, mostly to verify primitive wrappers do not return default values
                    assertTrue(rs.next());
                    assertNull(rs.getObject("col1", Boolean.class));
                    assertNull(rs.getObject(1, Boolean.class));
                    assertNull(rs.getObject("col1", Byte.class));
                    assertNull(rs.getObject(1, Byte.class));
                    assertNull(rs.getObject("col1", Short.class));
                    assertNull(rs.getObject(1, Short.class));
                    assertNull(rs.getObject(1, Integer.class));
                    assertNull(rs.getObject("col1", Integer.class));
                    assertNull(rs.getObject(1, Long.class));
                    assertNull(rs.getObject("col1", Long.class));

                    assertNull(rs.getObject(2, String.class));
                    assertNull(rs.getObject("col2", String.class));

                    assertNull(rs.getObject(3, Float.class));
                    assertNull(rs.getObject("col3", Float.class));
                    assertNull(rs.getObject(3, Double.class));
                    assertNull(rs.getObject("col3", Double.class));

                    assertNull(rs.getObject(4, BigDecimal.class));
                    assertNull(rs.getObject("col4", BigDecimal.class));

                    assertNull(rs.getObject(5, UUID.class));
                    assertNull(rs.getObject("col5", UUID.class));

                    assertNull(rs.getObject(6, SQLXML.class));
                    assertNull(rs.getObject("col6", SQLXML.class));

                    assertNull(rs.getObject(7, Blob.class));
                    assertNull(rs.getObject("col7", Blob.class));

                    assertNull(rs.getObject(8, Clob.class));
                    assertNull(rs.getObject("col8", Clob.class));

                    assertNull(rs.getObject(9, NClob.class));
                    assertNull(rs.getObject("col9", NClob.class));

                    assertNull(rs.getObject(10, byte[].class));
                    assertNull(rs.getObject("col10", byte[].class));
                    
                    assertNull(rs.getObject(11, java.sql.Date.class));
                    assertNull(rs.getObject("col11", java.sql.Date.class));
                    
                    assertNull(rs.getObject(12, java.sql.Time.class));
                    assertNull(rs.getObject("col12", java.sql.Time.class));
                    
                    assertNull(rs.getObject(13, java.sql.Timestamp.class));
                    assertNull(rs.getObject("col14", java.sql.Timestamp.class));

                    assertNull(rs.getObject(14, microsoft.sql.DateTimeOffset.class));
                    assertNull(rs.getObject("col14", microsoft.sql.DateTimeOffset.class));

                    assertFalse(rs.next());
                }
            } finally {
                stmt.executeUpdate("drop table " + tableName);
            }
        }
    }

    /**
     * Tests ResultSet#isWrapperFor and ResultSet#unwrap.
     * 
     * @throws SQLException
     */
    @Test
    public void testResultSetWrapper() throws SQLException {
        try (Connection con = DriverManager.getConnection(connectionString);
             Statement stmt = con.createStatement()) {
            
            stmt.executeUpdate("create table " + tableName + " (col1 int, col2 text, col3 int identity(1,1) primary key)");
            
            try (ResultSet rs = stmt.executeQuery("select * from " + tableName)) {
                assertTrue(rs.isWrapperFor(ResultSet.class));
                assertTrue(rs.isWrapperFor(ISQLServerResultSet.class));

                assertSame(rs, rs.unwrap(ResultSet.class));
                assertSame(rs, rs.unwrap(ISQLServerResultSet.class));
            } finally {
                Utils.dropTableIfExists(tableName, stmt);
            }
        }
    }
    
    /**
     * Tests calling any getter on a null column should work regardless of their type.
     * 
     * @throws SQLException
     */
    @Test
    public void testGetterOnNull() throws SQLException {
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;
        try {
            con = DriverManager.getConnection(connectionString);
            stmt = con.createStatement();
            rs = stmt.executeQuery("select null");
            rs.next();
            assertEquals(null, rs.getTime(1));
        }
        finally {
            if (con != null) {
                con.close();
            }
            if (stmt != null) {
                stmt.close();
            }
            if (rs != null) {
                rs.close();
            }
        }
    }
    
}
