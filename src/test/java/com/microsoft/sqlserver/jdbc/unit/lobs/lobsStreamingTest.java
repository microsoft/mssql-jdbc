package com.microsoft.sqlserver.jdbc.unit.lobs;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
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

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.util.RandomUtil;


@RunWith(JUnitPlatform.class)
public class lobsStreamingTest extends AbstractTest {

    private static final int LOB_ARRAY_SIZE = 250; // number of rows to insert into the table and compare
    private static final int LOB_LENGTH_MIN = 4000;
    private static final int LOB_LENGTH_MAX = 8000;

    static Connection conn = null;
    static Statement stmt = null;
    static String tableName;
    static String escapedTableName;

    @BeforeAll
    public static void init() throws SQLException {
        conn = DriverManager.getConnection(connectionString);
        stmt = conn.createStatement();

        tableName = RandomUtil.getIdentifier("streamingTest");
        escapedTableName = AbstractSQLGenerator.escapeIdentifier(tableName);
    }

    @AfterAll
    public static void terminate() throws SQLException {
        if (null != conn)
            conn.close();
        if (null != stmt)
            stmt.close();
    }

    @AfterEach
    public void dropTable() throws SQLException, InterruptedException {
        stmt.execute("DROP TABLE [" + tableName + "]");
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

    private String getStringFromInputStream(InputStream is) {
        try (java.util.Scanner s = new java.util.Scanner(is).useDelimiter("\\A")) {
            return s.hasNext() ? s.next() : "";
        }
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
    @DisplayName("testClobsVarcharASCII")
    public void testClobsVarcharASCII() throws SQLException {
        stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue varchar(max))");
        ArrayList<String> lobs = new ArrayList<>();
        IntStream.range(0, LOB_ARRAY_SIZE).forEach(
                i -> lobs.add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)");
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
            Clob c = rs.getClob(2);
            assertEquals(c.length(),lobs.get(rs.getInt(1)).length());
            lobsFromServer.add(c);
            String recieved = getStringFromInputStream(c.getAsciiStream());// streaming string
            streamedStrings.add(recieved);
            assertEquals(lobs.get(rs.getInt(1)), recieved);// compare streamed string to initial string
        }
        rs.close();
        pstmt.close();

        for (int i = 0; i < lobs.size(); i++) {
            String recieved = getStringFromInputStream(lobsFromServer.get(i).getAsciiStream());// non-streaming string
            assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
        }
    }

    @Test
    @DisplayName("testNClobsNVarcharASCII")
    public void testNClobsVarcharASCII() throws SQLException {
        stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue nvarchar(max))");
        ArrayList<String> lobs = new ArrayList<>();
        IntStream.range(0, LOB_ARRAY_SIZE).forEach(
                i -> lobs.add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)");
        for (int i = 0; i < lobs.size(); i++) {
            NClob c = conn.createNClob();
            c.setString(1, lobs.get(i));
            pstmt.setInt(1, i);
            pstmt.setClob(2, c);
            pstmt.addBatch();
        }
        pstmt.executeBatch();

        ArrayList<NClob> lobsFromServer = new ArrayList<>();
        ArrayList<String> streamedStrings = new ArrayList<>();
        ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
        while (rs.next()) {
            NClob c = rs.getNClob(2);
            assertEquals(c.length(),lobs.get(rs.getInt(1)).length());
            lobsFromServer.add(c);
            String recieved = getStringFromInputStream(c.getAsciiStream());// streaming string
            streamedStrings.add(recieved);
            assertEquals(lobs.get(rs.getInt(1)), recieved);// compare streamed string to initial string
        }
        rs.close();
        pstmt.close();

        for (int i = 0; i < lobs.size(); i++) {
            String recieved = getStringFromInputStream(lobsFromServer.get(i).getAsciiStream());// non-streaming string
            assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
        }
    }

    @Test
    @DisplayName("testClobsVarcharCHARA")
    public void testClobsVarcharCHARA() throws SQLException, IOException {
        stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue varchar(max))");
        ArrayList<String> lobs = new ArrayList<>();
        IntStream.range(0, LOB_ARRAY_SIZE).forEach(
                i -> lobs.add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)");
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
            Clob c = rs.getClob(2);
            assertEquals(c.length(),lobs.get(rs.getInt(1)).length());
            lobsFromServer.add(c);
            String recieved = getStringFromReader(c.getCharacterStream(), c.length());// streaming string
            streamedStrings.add(recieved);
            assertEquals(lobs.get(rs.getInt(1)), recieved);// compare streamed string to initial string
        }
        rs.close();
        pstmt.close();

        for (int i = 0; i < lobs.size(); i++) {
            String recieved = getStringFromReader(lobsFromServer.get(i).getCharacterStream(),
                    lobsFromServer.get(i).length());// non-streaming string
            assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
        }
    }

    @Test
    @DisplayName("testNClobsVarcharCHARA")
    public void testNClobsVarcharCHARA() throws SQLException, IOException {
        stmt.execute("CREATE TABLE [" + tableName + "] (id int, lobValue nvarchar(max))");
        ArrayList<String> lobs = new ArrayList<>();
        IntStream.range(0, LOB_ARRAY_SIZE).forEach(
                i -> lobs.add(getRandomString(ThreadLocalRandom.current().nextInt(LOB_LENGTH_MIN, LOB_LENGTH_MAX))));

        PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + tableName + "] VALUES(?,?)");
        for (int i = 0; i < lobs.size(); i++) {
            NClob c = conn.createNClob();
            c.setString(1, lobs.get(i));
            pstmt.setInt(1, i);
            pstmt.setClob(2, c);
            pstmt.addBatch();
        }
        pstmt.executeBatch();

        ArrayList<NClob> lobsFromServer = new ArrayList<>();
        ArrayList<String> streamedStrings = new ArrayList<>();
        ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC");
        while (rs.next()) {
            NClob c = rs.getNClob(2);
            assertEquals(c.length(),lobs.get(rs.getInt(1)).length());
            lobsFromServer.add(c);
            String recieved = getStringFromReader(c.getCharacterStream(), c.length());// streaming string
            streamedStrings.add(recieved);
            assertEquals(lobs.get(rs.getInt(1)), recieved);// compare streamed string to initial string
        }
        rs.close();
        pstmt.close();

        for (int i = 0; i < lobs.size(); i++) {
            String recieved = getStringFromReader(lobsFromServer.get(i).getCharacterStream(),
                    lobsFromServer.get(i).length());// non-streaming string
            assertEquals(recieved, streamedStrings.get(i));// compare static string to streamed string
        }
    }
}
