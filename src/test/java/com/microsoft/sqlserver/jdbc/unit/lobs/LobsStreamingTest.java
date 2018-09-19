package com.microsoft.sqlserver.jdbc.unit.lobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.nio.charset.Charset;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import java.util.stream.IntStream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;
import com.microsoft.sqlserver.testframework.util.RandomUtil;


@RunWith(JUnitPlatform.class)
public class LobsStreamingTest extends AbstractTest {

    private static final int LOB_ARRAY_SIZE = 250; // number of rows to insert into the table and compare
    private static final int LOB_LENGTH_MIN = 8000;
    private static final int LOB_LENGTH_MAX = 32000;

    static String tableName;
    static String escapedTableName;

    @BeforeEach
    public void init() throws SQLException {
        tableName = RandomUtil.getIdentifier("streamingTest");
        escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);
    }

    private String getRandomString(int length) {
        String validCharacters = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890!@#$%^&*()-=_+,./;'[]<>?:{}|`~\"\\";
        StringBuilder salt = new StringBuilder();
        Random rnd = new Random();
        while (salt.length() < length) {
            int index = (int) (rnd.nextFloat() * validCharacters.length());
            salt.append(validCharacters.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;

    }

    private String getStringFromInputStream(InputStream is, Charset c) {
        java.util.Scanner s = new java.util.Scanner(is, c).useDelimiter("\\A");
        return s.hasNext() ? s.next() : "";
    }

    private String getStringFromReader(Reader r, long l) throws IOException {
        // read the Reader contents into a buffer and return the complete string
        final StringBuilder stringBuilder = new StringBuilder((int) l);
        char[] buffer = new char[(int) l];
        while (true) {
            int amountRead = r.read(buffer, 0, (int) l);
            if (amountRead == -1) {
                break;
            }
            stringBuilder.append(buffer, 0, amountRead);
        }
        return stringBuilder.toString();
    }

    @Test
    @DisplayName("testLengthAfterStream")
    public void testLengthAfterStream() throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(connectionString);) {
            try (Statement stmt = conn.createStatement()) {
                Utils.dropTableIfExists(tableName, stmt);
                stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue varchar(max))");
                ArrayList<String> lobs = new ArrayList<>();
                IntStream.range(0, LOB_ARRAY_SIZE).forEach(i -> lobs
                        .add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)")) {
                    for (int i = 0; i < lobs.size(); i++) {
                        Clob c = conn.createClob();
                        c.setString(1, lobs.get(i));
                        pstmt.setInt(1, i);
                        pstmt.setClob(2, c);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();

                    ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
                    while (rs.next()) {
                        Clob c = rs.getClob(2);
                        Reader r = c.getCharacterStream();
                        long clobLength = c.length();
                        String recieved = getStringFromReader(r, clobLength);// streaming string
                        assertEquals(lobs.get(rs.getInt(1)), recieved);// compare streamed string to initial string
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    Utils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testClobsVarcharASCII")
    public void testClobsVarcharASCII() throws SQLException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                Utils.dropTableIfExists(tableName, stmt);
                stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue varchar(max))");
                ArrayList<String> lobs = new ArrayList<>();
                IntStream.range(0, LOB_ARRAY_SIZE).forEach(i -> lobs
                        .add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)")) {
                    for (int i = 0; i < lobs.size(); i++) {
                        Clob c = conn.createClob();
                        c.setString(1, lobs.get(i));
                        pstmt.setInt(1, i);
                        pstmt.setClob(2, c);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();

                    ArrayList<Clob> lobsFromServer = new ArrayList<>();
                    ArrayList<String> streamedStrings = new ArrayList<>();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        Clob c = rs.getClob(2);
                        assertEquals(c.length(), lobs.get(index).length());
                        lobsFromServer.add(c);
                        String recieved = getStringFromInputStream(c.getAsciiStream(),
                                java.nio.charset.StandardCharsets.US_ASCII);// streaming string
                        streamedStrings.add(recieved);
                        assertEquals(lobs.get(index), recieved);// compare streamed string to initial string
                    }
                    rs.close();
                    for (int i = 0; i < lobs.size(); i++) {
                        String recieved = getStringFromInputStream(lobsFromServer.get(i).getAsciiStream(),
                                java.nio.charset.StandardCharsets.US_ASCII);// non-streaming string
                        assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    Utils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testNClobsNVarcharASCII")
    public void testNClobsVarcharASCII() throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                Utils.dropTableIfExists(tableName, stmt);
                stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue nvarchar(max))");
                ArrayList<String> lobs = new ArrayList<>();
                IntStream.range(0, LOB_ARRAY_SIZE).forEach(i -> lobs
                        .add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)")) {
                    for (int i = 0; i < lobs.size(); i++) {
                        NClob c = conn.createNClob();
                        c.setString(1, lobs.get(i));
                        pstmt.setInt(1, i);
                        pstmt.setNClob(2, c);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();

                    ArrayList<NClob> lobsFromServer = new ArrayList<>();
                    ArrayList<String> streamedStrings = new ArrayList<>();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        NClob c = rs.getNClob(2);
                        assertEquals(c.length(), lobs.get(index).length());
                        lobsFromServer.add(c);
                        String recieved = getStringFromInputStream(c.getAsciiStream(),
                                java.nio.charset.StandardCharsets.UTF_16LE);// streaming string
                        streamedStrings.add(recieved);
                        assertEquals(lobs.get(index), recieved);// compare streamed string to initial string
                    }
                    rs.close();
                    for (int i = 0; i < lobs.size(); i++) {
                        String recieved = getStringFromInputStream(lobsFromServer.get(i).getAsciiStream(),
                                java.nio.charset.StandardCharsets.US_ASCII);// non-streaming string
                        assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    Utils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testClobsVarcharCHARA")
    public void testClobsVarcharCHARA() throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                Utils.dropTableIfExists(tableName, stmt);
                stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue varchar(max))");
                ArrayList<String> lobs = new ArrayList<>();
                IntStream.range(0, LOB_ARRAY_SIZE).forEach(i -> lobs
                        .add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)")) {
                    for (int i = 0; i < lobs.size(); i++) {
                        Clob c = conn.createClob();
                        c.setString(1, lobs.get(i));
                        pstmt.setInt(1, i);
                        pstmt.setClob(2, c);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();

                    ArrayList<Clob> lobsFromServer = new ArrayList<>();
                    ArrayList<String> streamedStrings = new ArrayList<>();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        Clob c = rs.getClob(2);
                        assertEquals(c.length(), lobs.get(index).length());
                        lobsFromServer.add(c);
                        String recieved = getStringFromReader(c.getCharacterStream(), c.length());// streaming string
                        streamedStrings.add(recieved);
                        assertEquals(lobs.get(index), recieved);// compare streamed string to initial string
                    }
                    rs.close();
                    for (int i = 0; i < lobs.size(); i++) {
                        String recieved = getStringFromReader(lobsFromServer.get(i).getCharacterStream(),
                                lobsFromServer.get(i).length());// non-streaming string
                        assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    Utils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testNClobsVarcharCHARA")
    public void testNClobsVarcharCHARA() throws SQLException, IOException {
        try (Connection conn = DriverManager.getConnection(connectionString)) {
            try (Statement stmt = conn.createStatement()) {
                Utils.dropTableIfExists(tableName, stmt);
                stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue nvarchar(max))");
                ArrayList<String> lobs = new ArrayList<>();
                IntStream.range(0, LOB_ARRAY_SIZE).forEach(i -> lobs
                        .add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

                try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)")) {
                    for (int i = 0; i < lobs.size(); i++) {
                        NClob c = conn.createNClob();
                        c.setString(1, lobs.get(i));
                        pstmt.setInt(1, i);
                        pstmt.setNClob(2, c);
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();

                    ArrayList<NClob> lobsFromServer = new ArrayList<>();
                    ArrayList<String> streamedStrings = new ArrayList<>();
                    ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        NClob c = rs.getNClob(2);
                        assertEquals(c.length(), lobs.get(index).length());
                        lobsFromServer.add(c);
                        String recieved = getStringFromReader(c.getCharacterStream(), c.length());// streaming string
                        streamedStrings.add(recieved);
                        assertEquals(lobs.get(index), recieved);// compare streamed string to initial string
                    }
                    rs.close();
                    for (int i = 0; i < lobs.size(); i++) {
                        String recieved = getStringFromReader(lobsFromServer.get(i).getCharacterStream(),
                                lobsFromServer.get(i).length());// non-streaming string
                        assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    Utils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }
}
