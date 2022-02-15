/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.lobs;

import static org.junit.Assert.fail;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
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
import java.util.logging.Logger;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.jdbc.TestUtils.DBBinaryStream;
import com.microsoft.sqlserver.jdbc.TestUtils.DBCharacterStream;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DBColumn;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBConstants;
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
@Tag(Constants.xAzureSQLDW)
public class LobsTest extends AbstractTest {
    static String tableName;
    static String escapedTableName;
    int datasize;
    int packetSize = 1000;
    int precision = 2000;
    long streamLength = -1; // Used to verify exceptions
    public static final Logger log = Logger.getLogger("lobs");
    Class<?> lobClass = null;
    boolean isResultSet = false;
    DBTable table = null;

    @BeforeAll
    public static void init() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        tableName = RandomUtil.getIdentifier("LOBS");
        escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);
    }

    @TestFactory
    public Collection<DynamicTest> executeDynamicTests() {
        List<Class<?>> classes = new ArrayList<Class<?>>(
                Arrays.asList(Blob.class, Clob.class, NClob.class, DBBinaryStream.class, DBCharacterStream.class));
        List<Boolean> isResultSetTypes = new ArrayList<>(Arrays.asList(true, false));
        Collection<DynamicTest> dynamicTests = new ArrayList<>();

        for (Class<?> aClass : classes) {
            for (Boolean isResultSetType : isResultSetTypes) {
                final Class<?> lobClass = aClass;
                final boolean isResultSet = isResultSetType;
                Executable exec = new Executable() {
                    @Override
                    public void execute() throws Throwable {
                        testInvalidLobs(lobClass, isResultSet);
                        testFreedBlobs(lobClass, isResultSet);
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
    private void testInvalidLobs(Class<?> lobClass, boolean isResultSet) throws SQLException {
        String clobTypes[] = {"varchar(max)", "nvarchar(max)"};
        String blobTypes[] = {"varbinary(max)"};
        int choose = Constants.RANDOM.nextInt(3);
        switch (choose) {
            case 0:
                datasize = packetSize;
                break;
            case 1:
                datasize = packetSize + Constants.RANDOM.nextInt(packetSize) + 1;
                break;
            default:
                datasize = packetSize - Constants.RANDOM.nextInt(packetSize);
        }

        int coercionType = isResultSet ? DBConstants.UPDATE_COERCION : DBConstants.SET_COERCION;
        Object updater = null;
        try (Connection conn = getConnection(); Statement stmt1 = conn.createStatement()) {
            try {
                Statement stmt = null;
                if (Constants.LOB.CLOB == classType(lobClass) || Constants.LOB.NCLOB == classType(lobClass)) {
                    table = createTable(table, clobTypes, true);
                } else {
                    table = createTable(table, blobTypes, true);
                }
                for (int i = 0; i < table.getColumns().size(); i++) {
                    DBColumn col = table.getColumns().get(i);
                    try (DBConnection dbConn = new DBConnection(connectionString)) {
                        if (!col.getSqlType().canConvert(lobClass, coercionType, dbConn))
                            continue;
                    }
                    // re-create LOB since it might get closed
                    Object lob = this.createLob(lobClass);
                    if (isResultSet) {
                        stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                        updater = stmt.executeQuery("Select " + table.getEscapedTableName() + ".[" + col.getColumnName()
                                + "]" + " from " + table.getEscapedTableName());
                        ((ResultSet) updater).next();
                    } else {
                        updater = conn.prepareStatement("update " + table.getEscapedTableName() + " set " + ".["
                                + col.getColumnName() + "]" + "=?");
                    }
                    try {
                        this.updateLob(lob, updater, 1);
                        fail(TestResource.getResource("R_expectedFailPassed"));
                    } catch (SQLException e) {
                        boolean verified = false;

                        if (lobClass == Clob.class || lobClass == NClob.class)
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
                        if (lobClass == DBCharacterStream.class || ((lobClass == Clob.class || lobClass == NClob.class)
                                && ((DBInvalidUtil.InvalidClob) lob).stream != null)) {
                            try (DBInvalidUtil.InvalidCharacterStream stream = lobClass == DBCharacterStream.class ? ((DBInvalidUtil.InvalidCharacterStream) lob)
                                                                                                                   : ((DBInvalidUtil.InvalidClob) lob).stream) {
                                if (stream.threwException) {
                                    // CharacterStream threw IOException
                                    String[] args = {"java.io.IOException: "
                                            + DBInvalidUtil.InvalidCharacterStream.IOExceptionMsg};
                                    assertTrue(e.getMessage().contains(args[0]));
                                    verified = true;
                                }
                            }
                        }
                        if (!verified) {
                            // Odd CharacterStream length will throw this exception
                            if (!e.getMessage().contains(TestResource.getResource("R_badStreamLength"))) {
                                assertTrue(e.getMessage().contains(TestResource.getResource("R_streamReadError")) || (ds
                                        .getColumnEncryptionSetting().equalsIgnoreCase(Constants.ENABLED)
                                        && e.getMessage().contains(TestResource.getResource("R_aeStreamReadError"))));
                            }

                        }
                    }
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                dropTables(table, stmt1);
                if (null != updater) {
                    if (isResultSet) {
                        ((ResultSet) updater).close();
                    } else {
                        ((PreparedStatement) updater).close();
                    }
                }
            }
        }
    }

    private void testFreedBlobs(Class<?> lobClass, boolean isResultSet) throws SQLException {
        String types[] = {"varbinary(max)"};
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            try {
                table = createTable(table, types, false); // create empty table
                int size = 10000;

                byte[] data = new byte[size];
                Constants.RANDOM.nextBytes(data);

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
                        try (InputStream stream = blob.getBinaryStream()) {
                            fail(TestResource.getResource("R_expectedFailPassed"));
                        } catch (SQLException e) {
                            assertTrue(e.getMessage().contains(TestResource.getResource("R_blobFreed")));
                        }
                    }
                }
                try (InputStream stream = blob.getBinaryStream()) {
                    fail(TestResource.getResource("R_expectedFailPassed"));
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains(TestResource.getResource("R_blobFreed")));
                }
            } catch (Exception e) {
                fail(e.getMessage());
            } finally {
                dropTables(table, stmt);
            }
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
    private void testMultipleClose(Class<?> streamClass) throws Exception {
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
                                if (!col.getSqlType().canConvert(streamClass, DBConstants.GET_COERCION, con))
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
            try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
                if (null != table) {
                    dropTables(table, stmt);
                }
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

    private void testLobsInsertRetrieve(String types[], Class<?> lobClass) throws Exception {
        table = createTable(table, types, false); // create empty table
        int size = 10000;

        byte[] data = new byte[size];
        Constants.RANDOM.nextBytes(data);

        Clob clob = null;
        Blob blob = null;
        NClob nclob = null;
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
                if (Constants.LOB.CLOB == classType(lobClass)) {
                    String stringData = new String(data);
                    size = stringData.length();
                    clob = conn.createClob();
                    clob.setString(1, stringData);
                    ps.setInt(1, 1);
                    ps.setClob(2, clob);
                } else if (Constants.LOB.NCLOB == classType(lobClass)) {
                    String stringData = new String(data);
                    size = stringData.length();
                    nclob = conn.createNClob();
                    nclob.setString(1, stringData);
                    ps.setInt(1, 1);
                    ps.setNClob(2, nclob);
                } else {
                    blob = conn.createBlob();
                    blob.setBytes(1, data);
                    ps.setInt(1, 1);
                    ps.setBlob(2, blob);
                }
                ps.executeUpdate();

                byte[] chunk = new byte[size];
                try (ResultSet rs = stmt.executeQuery("select * from " + table.getEscapedTableName())) {
                    while (rs.next()) {
                        if (Constants.LOB.CLOB == classType(lobClass)) {
                            String stringData = new String(data);
                            size = stringData.length();
                            clob = conn.createClob();
                            clob.setString(1, stringData);
                            rs.getClob(2);
                            try (InputStream stream = clob.getAsciiStream()) {
                                assertEquals(clob.length(), size);
                            }

                        } else if (Constants.LOB.NCLOB == classType(lobClass)) {
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
                dropTables(table, stmt);
            }
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
        Constants.RANDOM.nextBytes(data);

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            Blob blob = null;
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
                blob = conn.createBlob();
                blob.setBytes(1, data);
                ps.setInt(1, 1);
                ps.setBlob(2, blob);
                ps.executeUpdate();

                byte[] chunk = new byte[size];
                try (ResultSet rs = stmt.executeQuery("select * from " + table.getEscapedTableName() + " ORDER BY "
                        + table.getEscapedColumnName(0))) {
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
                dropTables(table, stmt);
            }
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
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            for (int i = 0; i < 5; i++)// create 5 blobs
            {
                try (PreparedStatement ps = conn
                        .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
                    blobs[i] = conn.createBlob();
                    Constants.RANDOM.nextBytes(data);
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
                dropTables(table, stmt);
            }
        }
    }

    /*
     * Tests delayLoadingLobs with Blobs
     */
    @Test
    @DisplayName("testBlobNotStreaming")
    public void testBlobNotStreaming() throws SQLException, IOException {
        String types[] = {"varbinary(max)"};
        table = createTable(table, types, false);

        int bufferSize = 10000;
        byte[] data = new byte[bufferSize];
        try (Connection conn = getConnection(); PreparedStatement pstmt = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            Blob b = conn.createBlob();
            Constants.RANDOM.nextBytes(data);
            b.setBytes(1, data);
            pstmt.setInt(1, 1);
            pstmt.setBlob(2, b);
            pstmt.executeUpdate();
        }

        String streamString = TestUtils.addOrOverrideProperty(connectionString, "delayLoadingLobs", "true");
        String loadedString = TestUtils.addOrOverrideProperty(streamString, "delayLoadingLobs", "false");

        try (Connection streamingConnection = DriverManager.getConnection(streamString);
                Connection loadedConnection = DriverManager.getConnection(loadedString);
                Statement sStmt = streamingConnection.createStatement();
                Statement lStmt = loadedConnection.createStatement()) {
            try (ResultSet rs = sStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                Blob b = rs.getBlob(2);
                InputStream stream = b.getBinaryStream();
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    int read = 0;
                    byte[] chunk = new byte[bufferSize];
                    while ((read = stream.read(chunk)) > 0)
                        buffer.write(chunk, 0, read);
                    assertTrue(Arrays.equals(chunk, data));
                }
            }
            try (ResultSet rs = lStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                Blob b = rs.getBlob(2);
                InputStream stream = b.getBinaryStream();
                try (ByteArrayOutputStream buffer = new ByteArrayOutputStream()) {
                    int read = 0;
                    byte[] chunk = new byte[bufferSize];
                    while ((read = stream.read(chunk)) > 0)
                        buffer.write(chunk, 0, read);
                    assertTrue(Arrays.equals(chunk, data));
                }
            }
        } finally {
            try (Connection c = getConnection(); Statement stmt = c.createStatement()) {
                TestUtils.dropTableIfExists(table.getEscapedTableName(), stmt);
            }
        }
    }

    /*
     * Tests Clobs and ASCII stream
     */
    @Test
    @DisplayName("testClobNotStreamingASCII")
    public void testClobNotStreamingASCII() throws SQLException, IOException {
        String types[] = {"varchar(max)"};
        table = createTable(table, types, false);
        // Avoid issues such as UNICODE / other languages, keep the test simple
        String testStr = "MICROSOFT JDBC DRIVER CLOB TEST 123\"";

        try (Connection conn = getConnection(); PreparedStatement pstmt = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            Clob c = conn.createClob();
            c.setString(1, testStr);
            pstmt.setInt(1, 1);
            pstmt.setClob(2, c);
            pstmt.executeUpdate();
        }

        String streamString = TestUtils.addOrOverrideProperty(connectionString, "delayLoadingLobs", "true");
        String loadedString = TestUtils.addOrOverrideProperty(streamString, "delayLoadingLobs", "false");

        try (Connection streamingConnection = DriverManager.getConnection(streamString);
                Connection loadedConnection = DriverManager.getConnection(loadedString);
                Statement sStmt = streamingConnection.createStatement();
                Statement lStmt = loadedConnection.createStatement()) {
            try (ResultSet rs = sStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                Clob c = rs.getClob(2);
                BufferedReader in = new BufferedReader(new InputStreamReader(c.getAsciiStream()));
                String line = null;
                StringBuilder rslt = new StringBuilder();
                while ((line = in.readLine()) != null) {
                    rslt.append(line);
                }
                assertEquals(testStr, rslt.toString());
            }
            try (ResultSet rs = lStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                Clob c = rs.getClob(2);
                BufferedReader in = new BufferedReader(new InputStreamReader(c.getAsciiStream()));
                String line = null;
                StringBuilder rslt = new StringBuilder();
                while ((line = in.readLine()) != null) {
                    rslt.append(line);
                }
                assertEquals(testStr, rslt.toString());
            }
        } finally {
            try (Connection c = getConnection(); Statement stmt = c.createStatement()) {
                TestUtils.dropTableIfExists(table.getEscapedTableName(), stmt);
            }
        }
    }

    /*
     * Tests continuous reading on non-streaming stream after the RS closes.
     */
    @Test
    @DisplayName("testContinuousReading")
    public void testContinuousReading() throws SQLException, IOException {
        String types[] = {"varchar(max)"};
        table = createTable(table, types, false);
        // Avoid issues such as UNICODE / other languages, keep the test simple
        String testStr = "MICROSOFT JDBC DRIVER";

        try (Connection conn = getConnection(); PreparedStatement pstmt = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            Clob c = conn.createClob();
            c.setString(1, testStr);
            pstmt.setInt(1, 1);
            pstmt.setClob(2, c);
            pstmt.executeUpdate();
        }

        String loadedString = TestUtils.addOrOverrideProperty(connectionString, "delayLoadingLobs", "false");
        try (Connection loadedConnection = DriverManager.getConnection(loadedString);
                Statement lStmt = loadedConnection.createStatement()) {
            BufferedReader in;
            try (ResultSet rs = lStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                Clob c = rs.getClob(2);
                in = new BufferedReader(new InputStreamReader(c.getAsciiStream()));
                char[] msft = new char[9];
                in.read(msft);
                assertEquals("MICROSOFT", String.valueOf(msft));
            }
            assertEquals(" JDBC DRIVER", in.readLine());
        } finally {
            try (Connection c = getConnection(); Statement stmt = c.createStatement()) {
                TestUtils.dropTableIfExists(table.getEscapedTableName(), stmt);
            }
        }
    }

    /*
     * Tests NClobs and Character Stream and Data source
     */
    @Test
    @DisplayName("testNClobNotStreamingChara")
    public void testNClobNotStreamingChara() throws SQLException, IOException {
        String types[] = {"nvarchar(max)"};
        table = createTable(table, types, false);
        // Avoid issues such as UNICODE / other languages, keep the test simple
        String testStr = "MICROSOFT_JDBC_DRIVER_NCLOB_TEST_123";

        try (Connection conn = getConnection(); PreparedStatement pstmt = conn
                .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            NClob c = conn.createNClob();
            c.setString(1, testStr);
            pstmt.setInt(1, 1);
            pstmt.setClob(2, c);
            pstmt.executeUpdate();
        }

        SQLServerDataSource ds1 = new SQLServerDataSource();
        SQLServerDataSource ds2 = new SQLServerDataSource();
        updateDataSource(connectionString, ds1);
        updateDataSource(connectionString, ds2);
        ds1.setDelayLoadingLobs(true);
        ds2.setDelayLoadingLobs(false);

        try (Connection streamingConnection = ds1.getConnection(); Connection loadedConnection = ds2.getConnection();
                Statement sStmt = streamingConnection.createStatement();
                Statement lStmt = loadedConnection.createStatement()) {
            try (ResultSet rs = sStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                NClob c = rs.getNClob(2);
                BufferedReader in = new BufferedReader(c.getCharacterStream());
                String line = null;
                StringBuilder rslt = new StringBuilder();
                while ((line = in.readLine()) != null) {
                    rslt.append(line);
                }
                assertEquals(testStr, rslt.toString());
            }
            try (ResultSet rs = lStmt.executeQuery(
                    "select * from " + table.getEscapedTableName() + " ORDER BY " + table.getEscapedColumnName(0))) {
                rs.next();
                NClob c = rs.getNClob(2);
                BufferedReader in = new BufferedReader(c.getCharacterStream());
                String line = null;
                StringBuilder rslt = new StringBuilder();
                while ((line = in.readLine()) != null) {
                    rslt.append(line);
                }
                assertEquals(testStr, rslt.toString());
            }
        } finally {
            try (Connection c = getConnection(); Statement stmt = c.createStatement()) {
                TestUtils.dropTableIfExists(table.getEscapedTableName(), stmt);
            }
        }
    }

    private void testUpdateLobs(String types[], Class<?> lobClass) throws Exception {
        table = createTable(table, types, false); // create empty table
        int size = 10000;

        byte[] data = new byte[size];
        Constants.RANDOM.nextBytes(data);

        Clob clob = null;
        Blob blob = null;
        NClob nclob = null;
        try (Connection conn = getConnection();
                Statement stmt = conn.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
                PreparedStatement ps = conn
                        .prepareStatement("INSERT INTO " + table.getEscapedTableName() + "  VALUES(?,?)")) {
            if (Constants.LOB.CLOB == classType(lobClass)) {
                String stringData = new String(data);
                size = stringData.length();
                clob = conn.createClob();
                clob.setString(1, stringData);
                ps.setInt(1, 1);
                ps.setClob(2, clob);
            } else if (Constants.LOB.NCLOB == classType(lobClass)) {
                String stringData = new String(data);
                size = stringData.length();
                nclob = conn.createNClob();
                nclob.setString(1, stringData);
                ps.setInt(1, 1);
                ps.setNClob(2, nclob);
            } else {
                blob = conn.createBlob();
                blob.setBytes(1, data);
                ps.setInt(1, 1);
                ps.setBlob(2, blob);
            }
            ps.executeUpdate();

            try (ResultSet rs = stmt.executeQuery("select * from " + table.getEscapedTableName())) {
                while (rs.next()) {
                    if (Constants.LOB.CLOB == classType(lobClass)) {
                        String stringData = new String(data);
                        size = stringData.length();
                        clob = conn.createClob();
                        clob.setString(1, stringData);
                        rs.updateClob(2, clob);
                    } else if (Constants.LOB.NCLOB == classType(lobClass)) {
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
            } finally {
                if (null != clob)
                    clob.free();
                if (null != blob)
                    blob.free();
                if (null != nclob)
                    nclob.free();
                dropTables(table, stmt);
            }
        }
    }

    private Constants.LOB classType(Class<?> type) {
        if (Clob.class == type)
            return Constants.LOB.CLOB;
        else if (NClob.class == type)
            return Constants.LOB.NCLOB;
        else
            return Constants.LOB.BLOB;
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
        else if (lob instanceof NClob)
            ps.setNClob(index, (NClob) lob);
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
        } else if (lob instanceof NClob) {
            rs.updateNClob(index, (NClob) lob);
        } else {
            rs.updateBlob(index, (Blob) lob);
        }
        rs.updateRow();
    }

    private Object createLob(Class<?> lobClass) {
        // Randomly indicate negative length
        streamLength = Constants.RANDOM.nextInt(3) < 2 ? datasize : -1 - Constants.RANDOM.nextInt(datasize);
        // For streams -1 means any length, avoid to ensure that an exception is always thrown
        if (streamLength == -1 && (lobClass == DBCharacterStream.class || lobClass == DBBinaryStream.class))
            streamLength = datasize;
        log.fine("Length passed into update : " + streamLength);

        byte[] data = new byte[datasize];
        Constants.RANDOM.nextBytes(data);

        if (lobClass == DBCharacterStream.class)
            return new DBInvalidUtil().new InvalidCharacterStream(new String(data), streamLength < -1);
        else if (lobClass == DBBinaryStream.class)
            return new DBInvalidUtil().new InvalidBinaryStream(data, streamLength < -1);
        if (lobClass == Clob.class || lobClass == NClob.class) {
            SqlType type = TestUtils.find(String.class);
            Object expected = type.createdata(String.class, data);
            return new DBInvalidUtil().new InvalidClob(expected, false);
        } else {
            SqlType type = TestUtils.find(byte[].class);
            Object expected = type.createdata(type.getClass(), data);
            return new DBInvalidUtil().new InvalidBlob(expected, false);
        }

    }

    private static DBTable createTable(DBTable table, String[] types, boolean populateTable) throws SQLException {

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

    private static void dropTables(DBTable table, Statement stmt) throws SQLException {
        stmt.executeUpdate("if object_id('" + TestUtils.escapeSingleQuotes(table.getEscapedTableName())
                + "','U') is not null" + " drop table " + table.getEscapedTableName());
    }

}
