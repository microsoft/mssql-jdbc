/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.lobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Reader;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.TestUtils.DBBinaryStream;
import com.microsoft.sqlserver.jdbc.TestUtils.DBCharacterStream;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBCoercion;
import com.microsoft.sqlserver.testframework.DBColumn;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBInvalidUtil;
import com.microsoft.sqlserver.testframework.DBResultSet;
import com.microsoft.sqlserver.testframework.DBStatement;
import com.microsoft.sqlserver.testframework.DBTable;
import com.microsoft.sqlserver.testframework.sqlType.SqlType;


/**
 * This class tests lobs (Blob, Clob and NClob) and their APIs
 *
 */
@RunWith(JUnitPlatform.class)
public class LobsTest extends AbstractTest {
    static Connection conn = null;
    static Statement stmt = null;
    static String tableName;
    static String escapedTableName;
    int datasize;
    int packetSize = 1000;
    int precision = 2000;
    long streamLength = -1; // Used to verify exceptions
    public static final Logger log = Logger.getLogger("lobs");
    Class lobClass = null;
    boolean isResultSet = false;
    DBTable table = null;

    private static final int clobType = 0;
    private static final int nClobType = 1;
    private static final int blobType = 2;

    @BeforeAll
    public static void init() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        tableName = RandomUtil.getIdentifier("LOBS");
        escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);
    }

    @AfterAll
    public static void terminate() throws SQLException {
        if (null != conn)
            conn.close();
        if (null != stmt)
            stmt.close();
    }

    @TestFactory
    public Collection<DynamicTest> executeDynamicTests() {
        List<Class> classes = new ArrayList<Class>(
                Arrays.asList(Blob.class, Clob.class, DBBinaryStream.class, DBCharacterStream.class));
        List<Boolean> isResultSetTypes = new ArrayList<>(Arrays.asList(true, false));
        Collection<DynamicTest> dynamicTests = new ArrayList<>();

        for (Class aClass : classes) {
            for (Boolean isResultSetType : isResultSetTypes) {
                final Class lobClass = aClass;
                final boolean isResultSet = isResultSetType;
                Executable exec = new Executable() {
                    @Override
                    public void execute() throws Throwable {
                        testInvalidLobs(lobClass, isResultSet);
                    }
                };
                // create a test display name
                String testName = " Test: " + lobClass + (isResultSet ? " isResultSet" : " isPreparedStatement");
                // create dynamic test
                DynamicTest dTest = DynamicTest.dynamicTest(testName, exec);

                // add the dynamic test to collection
                dynamicTests.add(dTest);
            }
        }
        return dynamicTests;
    }

    /**
     * Tests invalid lobs
     * 
     * @param lobClass
     * @param isResultSet
     * @throws SQLException
     */
    private void testInvalidLobs(Class lobClass, boolean isResultSet) throws SQLException {
        String clobTypes[] = {"varchar(max)", "nvarchar(max)"};
        String blobTypes[] = {"varbinary(max)"};
        int choose = ThreadLocalRandom.current().nextInt(3);
        switch (choose) {
            case 0:
                datasize = packetSize;
                break;
            case 1:
                datasize = packetSize + ThreadLocalRandom.current().nextInt(packetSize) + 1;
                break;
            default:
                datasize = packetSize - ThreadLocalRandom.current().nextInt(packetSize);
        }

        int coercionType = isResultSet ? DBCoercion.UPDATE : DBCoercion.SET;
        Object updater = null;
        Statement stmt = null;
        try {
            if (clobType == classType(lobClass) || nClobType == classType(lobClass)) {
                table = createTable(table, clobTypes, true);
            } else {
                table = createTable(table, blobTypes, true);
            }
            for (int i = 0; i < table.getColumns().size(); i++) {
                DBColumn col = table.getColumns().get(i);
                try (DBConnection conn = new DBConnection(connectionString)) {
                    if (!col.getSqlType().canConvert(lobClass, coercionType, conn))
                        continue;
                }
                // re-create LOB since it might get closed
                Object lob = this.createLob(lobClass);
                if (isResultSet) {
                    stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                    updater = stmt.executeQuery("Select " + table.getEscapedTableName() + ".[" + col.getColumnName()
                            + "]" + " from " + table.getEscapedTableName());
                    ((ResultSet) updater).next();
                } else
                    updater = conn.prepareStatement("update " + table.getEscapedTableName() + " set " + ".["
                            + col.getColumnName() + "]" + "=?");
                try {
                    this.updateLob(lob, updater, 1);
                } catch (SQLException e) {
                    boolean verified = false;

                    if (lobClass == Clob.class)
                        streamLength = ((DBInvalidUtil.InvalidClob) lob).length;
                    else if (lobClass == Blob.class)
                        streamLength = ((DBInvalidUtil.InvalidBlob) lob).length;

                    // Case 1: Invalid length value is passed as LOB length
                    if (streamLength < 0 || streamLength == Long.MAX_VALUE) {
                        // Applies to all LOB types ("The length {0} is not valid}
                        assertTrue(e.getMessage().startsWith("The length"),
                                TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
                        assertTrue(e.getMessage().endsWith("is not valid."),
                                TestResource.getResource("R_unexpectedExceptionContent") + ": " + e.getMessage());
                        verified = true;

                    }

                    // Case 2: CharacterStream or Clob.getCharacterStream threw IOException
                    if (lobClass == DBCharacterStream.class
                            || (lobClass == Clob.class && ((DBInvalidUtil.InvalidClob) lob).stream != null)) {
                        try (DBInvalidUtil.InvalidCharacterStream stream = lobClass == DBCharacterStream.class ? ((DBInvalidUtil.InvalidCharacterStream) lob)
                                                                                                               : ((DBInvalidUtil.InvalidClob) lob).stream) {
                            if (stream.threwException) {
                                // CharacterStream threw IOException
                                String[] args = {
                                        "java.io.IOException: " + DBInvalidUtil.InvalidCharacterStream.IOExceptionMsg};
                                assertTrue(e.getMessage().contains(args[0]));
                                verified = true;
                            }
                        }
                    }
                    if (!verified) {
                        // Odd CharacterStream length will throw this exception
                        if (!e.getMessage().contains(TestResource.getResource("R_badStreamLength"))) {
                            assertTrue(e.getMessage().contains(TestResource.getResource("R_streamReadError")));
                        }

                    }
                }
            }
        } catch (Exception e) {
            dropTables(table);
            e.printStackTrace();
        } finally {
            dropTables(table);
            if (null != updater) {
                if (isResultSet) {
                    ((ResultSet) updater).close();
                } else {
                    ((PreparedStatement) updater).close();
                }
            }
            if (null != stmt) {
                stmt.close();
            }
        }
    }

    @Test
    @DisplayName("testFreedBlobs")
    private void testFreedBlobs(Class lobClass, boolean isResultSet) throws SQLException {
        String types[] = {"varbinary(max)"};
        try {
            table = createTable(table, types, false); // create empty table
            int size = 10000;

            byte[] data = new byte[size];
            ThreadLocalRandom.current().nextBytes(data);

            Blob blob = null;
            for (int i = 0; i < 5; i++) {
                try (PreparedStatement ps = conn
                        .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
                    blob = conn.createBlob();
                    blob.setBytes(1, data);
                    ps.setInt(1, i + 1);
                    ps.setBlob(2, blob);
                    ps.executeUpdate();
                }
            }

            try (ResultSet rs = stmt.executeQuery("select * from " + table.getEscapedTableName())) {
                for (int i = 0; i < 5; i++) {
                    rs.next();

                    blob = rs.getBlob(2);
                    try (InputStream stream = blob.getBinaryStream()) {
                        while (stream.available() > 0)
                            stream.read();
                        blob.free();
                    }
                    try (InputStream stream = blob.getBinaryStream()) {} catch (SQLException e) {
                        assertTrue(e.getMessage().contains(TestResource.getResource("R_blobFreed")));
                    }
                }
            }
            try (InputStream stream = blob.getBinaryStream()) {} catch (SQLException e) {
                assertTrue(e.getMessage().contains(TestResource.getResource("R_blobFreed")));
            }
        } catch (Exception e) {
            dropTables(table);
            e.printStackTrace();
        } finally {
            dropTables(table);
        }
    }

    @Test
    @DisplayName("testMultipleCloseCharacterStream")
    public void testMultipleCloseCharacterStream() throws Exception {
        testMultipleClose(DBCharacterStream.class);
    }

    @Test
    @DisplayName("MultipleCloseBinaryStream")
    public void MultipleCloseBinaryStream() throws Exception {
        testMultipleClose(DBBinaryStream.class);
    }

    /**
     * Tests stream closures
     * 
     * @param streamClass
     * @throws Exception
     */
    private void testMultipleClose(Class streamClass) throws Exception {
        String[] types = {"varchar(max)", "nvarchar(max)", "varbinary(max)"};
        try (DBConnection conn = new DBConnection(connectionString);
                DBStatement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE)) {
            table = createTable(table, types, true);

            String query = "select * from " + table.getEscapedTableName() + " ORDER BY "
                    + table.getEscapedColumnName(0);
            try (DBResultSet rs = stmt.executeQuery(query)) {
                while (rs.next()) {
                    for (int i = 0; i < types.length + 1; i++) { // +1 for RowId
                        if (i == 0) {
                            rs.getInt(1);
                        } else {
                            DBColumn col = table.getColumns().get(i);
                            try (DBConnection con = new DBConnection(connectionString)) {
                                if (!col.getSqlType().canConvert(streamClass, DBCoercion.GET, con))
                                    continue;
                            }

                            if (streamClass == DBCharacterStream.class) {
                                try (Reader stream = (Reader) rs.getXXX(i + 1, streamClass)) {
                                    if (null == stream) {
                                        assertEquals(stream, rs.getObject(i + 1),
                                                TestResource.getResource("R_streamNull"));
                                    }
                                }
                            } else {
                                try (InputStream stream = (InputStream) rs.getXXX(i + 1, streamClass)) {
                                    if (null == stream) {
                                        assertEquals(stream, rs.getObject(i + 1),
                                                TestResource.getResource("R_streamNull"));
                                    }
                                }
                            }
                        }
                    }
                }
            }
        } finally {
            if (null != table) {
                dropTables(table);
            }
        }
    }

    /**
     * Tests Insert Retrieve on nclob
     * 
     * @throws Exception
     */
    @Test
    @DisplayName("testlLobsInsertRetrieve")
    public void testNClob() throws Exception {
        String types[] = {"nvarchar(max)"};
        testLobsInsertRetrieve(types, NClob.class);
    }

    /**
     * Tests Insert Retrieve on blob
     * 
     * @throws Exception
     */
    @Test
    @DisplayName("testlLobsInsertRetrieve")
    public void testBlob() throws Exception {
        String types[] = {"varbinary(max)"};
        testLobsInsertRetrieve(types, Blob.class);
    }

    /**
     * Tests Insert Retrieve on clob
     * 
     * @throws Exception
     */
    @Test
    @DisplayName("testlLobsInsertRetrieve")
    public void testClob() throws Exception {
        String types[] = {"varchar(max)"};
        testLobsInsertRetrieve(types, Clob.class);
    }

    private void testLobsInsertRetrieve(String types[], Class lobClass) throws Exception {
        table = createTable(table, types, false); // create empty table
        int size = 10000;

        byte[] data = new byte[size];
        ThreadLocalRandom.current().nextBytes(data);

        Clob clob = null;
        Blob blob = null;
        NClob nclob = null;
        try (PreparedStatement ps = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            if (clobType == classType(lobClass)) {
                String stringData = new String(data);
                size = stringData.length();
                clob = conn.createClob();
                clob.setString(1, stringData);
                ps.setInt(1, 1);
                ps.setClob(2, clob);
            } else if (nClobType == classType(lobClass)) {
                String stringData = new String(data);
                size = stringData.length();
                nclob = conn.createNClob();
                nclob.setString(1, stringData);
                ps.setInt(1, 1);
                ps.setNClob(2, nclob);
            }

            else {
                blob = conn.createBlob();
                blob.setBytes(1, data);
                ps.setInt(1, 1);
                ps.setBlob(2, blob);
            }
            ps.executeUpdate();

            byte[] chunk = new byte[size];
            try (ResultSet rs = stmt.executeQuery("select * from " + table.getEscapedTableName())) {
                while (rs.next()) {
                    if (clobType == classType(lobClass)) {
                        String stringData = new String(data);
                        size = stringData.length();
                        clob = conn.createClob();
                        clob.setString(1, stringData);
                        rs.getClob(2);
                        try (InputStream stream = clob.getAsciiStream()) {
                            assertEquals(clob.length(), size);
                        }

                    } else if (nClobType == classType(lobClass)) {
                        nclob = rs.getNClob(2);
                        assertEquals(nclob.length(), size);
                        try (InputStream stream = nclob.getAsciiStream();
                                BufferedInputStream is = new BufferedInputStream(stream)) {
                            is.read(chunk);
                            assertEquals(chunk.length, size);
                        }
                    } else {
                        blob = rs.getBlob(2);
                        try (InputStream stream = blob.getBinaryStream();
                                ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                            int read = 0;
                            while ((read = stream.read(chunk)) > 0)
                                buffer.write(chunk, 0, read);
                            assertEquals(chunk.length, size);
                        }
                    }
                }

            }
        } finally {
            if (null != clob)
                clob.free();
            if (null != blob)
                blob.free();
            if (null != nclob)
                nclob.free();
            dropTables(table);
        }
    }

    @Test
    @DisplayName("testUpdatorNClob")
    public void testUpdatorNClob() throws Exception {
        String types[] = {"nvarchar(max)"};
        testUpdateLobs(types, NClob.class);
    }

    @Test
    @DisplayName("testUpdatorBlob")
    public void testUpdatorBlob() throws Exception {
        String types[] = {"varbinary(max)"};
        testUpdateLobs(types, Blob.class);
    }

    @Test
    @DisplayName("testUpdatorClob")
    public void testUpdatorClob() throws Exception {
        String types[] = {"varchar(max)"};
        testUpdateLobs(types, Clob.class);
    }

    @Test
    @DisplayName("readBlobStreamAfterClosingRS")
    public void readBlobStreamAfterClosingRS() throws Exception {
        String types[] = {"varbinary(max)"};
        table = createTable(table, types, false); // create empty table
        int size = 10000;

        byte[] data = new byte[size];
        ThreadLocalRandom.current().nextBytes(data);

        Blob blob = null;
        try (PreparedStatement ps = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            blob = conn.createBlob();
            blob.setBytes(1, data);
            ps.setInt(1, 1);
            ps.setBlob(2, blob);
            ps.executeUpdate();

            byte[] chunk = new byte[size];
            try (ResultSet rs = stmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();

                blob = rs.getBlob(2);
                try (InputStream stream = blob.getBinaryStream();
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    int read = 0;
                    while ((read = stream.read(chunk)) > 0)
                        buffer.write(chunk, 0, read);
                    assertEquals(chunk.length, size);
                    rs.close();
                }
                try (InputStream stream = blob.getBinaryStream();
                        ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    int read = 0;
                    while ((read = stream.read(chunk)) > 0)
                        buffer.write(chunk, 0, read);
                    assertEquals(chunk.length, size);
                }
            }
        } finally {
            if (null != blob)
                blob.free();
            dropTables(table);
        }
    }

    @Test
    @DisplayName("readMultipleBlobStreamsThenCloseRS")
    public void readMultipleBlobStreamsThenCloseRS() throws Exception {
        String types[] = {"varbinary(max)"};
        table = createTable(table, types, false);
        int size = 10000;

        byte[] data = new byte[size];
        Blob[] blobs = {null, null, null, null, null};
        InputStream stream = null;
        for (int i = 0; i < 5; i++)// create 5 blobs
        {
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
                blobs[i] = conn.createBlob();
                ThreadLocalRandom.current().nextBytes(data);
                blobs[i].setBytes(1, data);
                ps.setInt(1, i + 1);
                ps.setBlob(2, blobs[i]);
                ps.executeUpdate();
            }
        }

        byte[] chunk = new byte[size];
        try (ResultSet rs = stmt.executeQuery(
                "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
            for (int i = 0; i < 5; i++) {
                rs.next();
                blobs[i] = rs.getBlob(2);
                stream = blobs[i].getBinaryStream();
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    int read = 0;
                    while ((read = stream.read(chunk)) > 0)
                        buffer.write(chunk, 0, read);
                    assertEquals(chunk.length, size);
                }
            }

            for (int i = 0; i < 5; i++) {
                stream = blobs[i].getBinaryStream();
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    int read = 0;
                    while ((read = stream.read(chunk)) > 0)
                        buffer.write(chunk, 0, read);
                    assertEquals(chunk.length, size);
                }
            }
        } finally {
            if (null != stream) {
                stream.close();
            }
            dropTables(table);
        }
    }

    private void testUpdateLobs(String types[], Class lobClass) throws Exception {
        table = createTable(table, types, false); // create empty table
        int size = 10000;

        byte[] data = new byte[size];
        ThreadLocalRandom.current().nextBytes(data);

        Clob clob = null;
        Blob blob = null;
        NClob nclob = null;
        try (PreparedStatement ps = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            if (clobType == classType(lobClass)) {
                String stringData = new String(data);
                size = stringData.length();
                clob = conn.createClob();
                clob.setString(1, stringData);
                ps.setInt(1, 1);
                ps.setClob(2, clob);
            } else if (nClobType == classType(lobClass)) {
                String stringData = new String(data);
                size = stringData.length();
                nclob = conn.createNClob();
                nclob.setString(1, stringData);
                ps.setInt(1, 1);
                ps.setNClob(2, nclob);
            }

            else {
                blob = conn.createBlob();
                blob.setBytes(1, data);
                ps.setInt(1, 1);
                ps.setBlob(2, blob);
            }
            ps.executeUpdate();

            try (Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                    ResultSet rs = stmt.executeQuery("select * from " + table.getEscapedTableName())) {
                while (rs.next()) {
                    if (clobType == classType(lobClass)) {
                        String stringData = new String(data);
                        size = stringData.length();
                        clob = conn.createClob();
                        clob.setString(1, stringData);
                        rs.updateClob(2, clob);
                    } else if (nClobType == classType(lobClass)) {
                        String stringData = new String(data);
                        size = stringData.length();
                        nclob = conn.createNClob();
                        nclob.setString(1, stringData);
                        rs.updateClob(2, nclob);
                    } else {
                        blob = conn.createBlob();
                        rs.updateBlob(2, blob);

                    }
                    rs.updateRow();
                }
            }
        } finally {
            if (null != clob)
                clob.free();
            if (null != blob)
                blob.free();
            if (null != nclob)
                nclob.free();
            dropTables(table);
        }
    }

    private int classType(Class type) {
        if (Clob.class == type)
            return clobType;
        else if (NClob.class == type)
            return nClobType;
        else
            return blobType;
    }

    private void updateLob(Object lob, Object updater, int index) throws Exception {
        if (updater instanceof PreparedStatement)
            this.updatePreparedStatement((PreparedStatement) updater, lob, index, (int) streamLength);
        else
            this.updateResultSet((ResultSet) updater, lob, index, (int) streamLength);
    }

    private void updatePreparedStatement(PreparedStatement ps, Object lob, int index, int length) throws Exception {
        if (lob instanceof DBCharacterStream)
            ps.setCharacterStream(index, (DBCharacterStream) lob, length);
        else if (lob instanceof DBBinaryStream)
            ps.setBinaryStream(index, (InputStream) lob, length);
        else if (lob instanceof Clob)
            ps.setClob(index, (Clob) lob);
        else
            ps.setBlob(index, (Blob) lob);
        assertEquals(ps.executeUpdate(), 1, TestResource.getResource("R_incorrectUpdateCount"));
    }

    private void updateResultSet(ResultSet rs, Object lob, int index, int length) throws Exception {
        if (lob instanceof DBCharacterStream) {
            rs.updateCharacterStream(index, (DBCharacterStream) lob, length);
        } else if (lob instanceof DBBinaryStream) {
            rs.updateBinaryStream(index, (InputStream) lob, length);
        } else if (lob instanceof Clob) {
            rs.updateClob(index, (Clob) lob);
        } else {
            rs.updateBlob(index, (Blob) lob);
        }
        rs.updateRow();
    }

    private Object createLob(Class lobClass) {
        // Randomly indicate negative length
        streamLength = ThreadLocalRandom.current().nextInt(3) < 2 ? datasize
                                                                  : -1 - ThreadLocalRandom.current().nextInt(datasize);
        // For streams -1 means any length, avoid to ensure that an exception is always thrown
        if (streamLength == -1 && (lobClass == DBCharacterStream.class || lobClass == DBBinaryStream.class))
            streamLength = datasize;
        log.fine("Length passed into update : " + streamLength);

        byte[] data = new byte[datasize];
        ThreadLocalRandom.current().nextBytes(data);

        if (lobClass == DBCharacterStream.class)
            return new DBInvalidUtil().new InvalidCharacterStream(new String(data), streamLength < -1);
        else if (lobClass == DBBinaryStream.class)
            return new DBInvalidUtil().new InvalidBinaryStream(data, streamLength < -1);
        if (lobClass == Clob.class) {
            ArrayList<SqlType> types = TestUtils.types();
            SqlType type = TestUtils.find(String.class);
            Object expected = type.createdata(String.class, data);
            return new DBInvalidUtil().new InvalidClob(expected, false);
        } else {
            ArrayList<SqlType> types = TestUtils.types();
            SqlType type = TestUtils.find(byte[].class);
            Object expected = type.createdata(type.getClass(), data);
            return new DBInvalidUtil().new InvalidBlob(expected, false);
        }

    }

    private static DBTable createTable(DBTable table, String[] types, boolean populateTable) throws Exception {

        try (DBConnection connection = new DBConnection(connectionString);
                DBStatement stmt = connection.createStatement()) {
            table = new DBTable(false);

            // Add RowId
            table.addColumn(TestUtils.find("int"));

            for (String type1 : types) {
                SqlType type = TestUtils.find(type1);
                table.addColumn(type);

            }
            stmt.createTable(table);
            if (populateTable) {
                stmt.populateTable(table);
            }
        }
        return table;
    }

    private static void dropTables(DBTable table) throws SQLException {
        stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table.getEscapedTableName())
                + "','U') is not null" + " drop table " + table.getEscapedTableName());
    }

}
