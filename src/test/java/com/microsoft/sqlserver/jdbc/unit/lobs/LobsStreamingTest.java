package com.microsoft.sqlserver.jdbc.unit.lobs;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.NClob;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.stream.IntStream;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class LobsStreamingTest extends AbstractTest {

    private static String tableName = null;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @BeforeEach
    public void init() throws SQLException {
        tableName = RandomUtil.getIdentifier("streamingTest");
    }

    private String getRandomString(int length, String validCharacters) {
        StringBuilder salt = new StringBuilder();
        while (salt.length() < length) {
            int index = (int) (Constants.RANDOM.nextFloat() * validCharacters.length());
            salt.append(validCharacters.charAt(index));
        }
        String saltStr = salt.toString();
        return saltStr;
    }

    // closing the scanner closes the InputStream, and the driver needs the stream to fill LoBs
    private String getStringFromInputStream(InputStream is, Scanner s) {
        return s.hasNext() ? s.next() : "";
    }

    private String getStringFromReader(Reader r, long l) throws IOException {
        // read the Reader contents into a buffer and return the complete string
        final StringBuilder stringBuilder = new StringBuilder((int) l);
        char[] buffer = new char[(int) l];
        int amountRead = -1;
        while ((amountRead = r.read(buffer, 0, (int) l)) != -1) {
            stringBuilder.append(buffer, 0, amountRead);
        }
        return stringBuilder.toString();
    }

    private void createLobTable(Statement stmt, String table, Constants.LOB l) throws SQLException {
        String columnType = (l == Constants.LOB.CLOB) ? "varchar(max)" : "nvarchar(max)";
        stmt.execute("CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(table) + " (id int, lobValue " + columnType
                + ")");
    }

    private ArrayList<String> createRandomStringArray(Constants.LOB l) {
        String characterPool = (l == Constants.LOB.CLOB) ? Constants.ASCII_CHARACTERS : Constants.UNICODE_CHARACTERS;
        ArrayList<String> string_array = new ArrayList<>();
        IntStream.range(0, Constants.LOB_ARRAY_SIZE).forEach(i -> string_array.add(getRandomString(
                Constants.RANDOM.nextInt(Constants.LOB_LENGTH_MIN, Constants.LOB_LENGTH_MAX), characterPool)));
        return string_array;
    }

    private void insertData(Connection conn, String table, ArrayList<String> lobs) throws SQLException {
        try (PreparedStatement pstmt = conn.prepareStatement("INSERT INTO [" + table + "] VALUES(?,?)")) {
            for (int i = 0; i < lobs.size(); i++) {
                Clob c = conn.createClob();
                c.setString(1, lobs.get(i));
                pstmt.setInt(1, i);
                pstmt.setClob(2, c);
                pstmt.addBatch();
            }
            pstmt.executeBatch();
        }
    }

    @Test
    @DisplayName("testLengthAfterStream")
    public void testLengthAfterStream() throws SQLException, IOException {
        try (Connection conn = getConnection();) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
                ArrayList<String> lob_data = createRandomStringArray(Constants.LOB.CLOB);

                createLobTable(stmt, tableName, Constants.LOB.CLOB);
                insertData(conn, tableName, lob_data);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC")) {
                    while (rs.next()) {
                        Clob c = rs.getClob(2);
                        try (Reader r = c.getCharacterStream()) {
                            long clobLength = c.length();
                            String received = getStringFromReader(r, clobLength);// streaming string
                            c.free();
                            assertEquals(lob_data.get(rs.getInt(1)), received);// compare streamed string to initial
                                                                               // string
                        }
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testClobsVarcharASCII")
    public void testClobsVarcharASCII() throws SQLException, IOException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);

                ArrayList<String> lob_data = createRandomStringArray(Constants.LOB.CLOB);
                ArrayList<String> receivedDataFromServer = new ArrayList<>();

                createLobTable(stmt, tableName, Constants.LOB.CLOB);
                insertData(conn, tableName, lob_data);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC")) {
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        Clob c = rs.getClob(2);
                        assertEquals(c.length(), lob_data.get(index).length());
                        try (InputStream is = c.getAsciiStream(); Scanner s = new Scanner(is, "US-ASCII")) {
                            // streaming string
                            String received = getStringFromInputStream(is, s.useDelimiter("\\A"));
                            // compare streamed string to initial string
                            assertEquals(lob_data.get(index), received);
                            c.free();
                            receivedDataFromServer.add(received);
                        }
                    }
                    for (int i = 0; i < lob_data.size(); i++) {
                        // compare satic string to streamed string
                        assertEquals(receivedDataFromServer.get(i), lob_data.get(i));
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }

    }

    @Test
    @DisplayName("testNClobsNVarcharASCII")
    public void testNClobsVarcharASCII() throws SQLException, IOException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
                // Testing AsciiStream, use Clob string set or characters will be converted to '?'
                ArrayList<String> lob_data = createRandomStringArray(Constants.LOB.CLOB);

                createLobTable(stmt, tableName, Constants.LOB.NCLOB);
                insertData(conn, tableName, lob_data);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC")) {
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        NClob c = rs.getNClob(2);
                        assertEquals(c.length(), lob_data.get(index).length());
                        try (InputStream is = c.getAsciiStream(); Scanner s = new Scanner(is, "US-ASCII")) {
                            // nClob AsciiStream is never streamed
                            String received = getStringFromInputStream(is, s.useDelimiter("\\A"));
                            c.free();
                            assertEquals(lob_data.get(index), received);// compare string to initial string
                        }
                    }
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testClobsVarcharCHARA")
    public void testClobsVarcharCHARA() throws SQLException, IOException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);

                ArrayList<String> lob_data = createRandomStringArray(Constants.LOB.CLOB);
                ArrayList<String> receivedDataFromServer = new ArrayList<>();

                createLobTable(stmt, tableName, Constants.LOB.CLOB);
                insertData(conn, tableName, lob_data);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC")) {
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        Clob c = rs.getClob(2);
                        assertEquals(c.length(), lob_data.get(index).length());
                        try (Reader reader = c.getCharacterStream()) {
                            String received = getStringFromReader(reader, c.length());// streaming string
                            receivedDataFromServer.add(received);
                            assertEquals(lob_data.get(index), received);// compare streamed string to initial string
                            c.free();
                        }
                    }
                }
                for (int i = 0; i < lob_data.size(); i++) {
                    assertEquals(receivedDataFromServer.get(i), lob_data.get(i));// compare static string to streamed
                                                                                 // string
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Test
    @DisplayName("testNClobsVarcharCHARA")
    public void testNClobsVarcharCHARA() throws SQLException, IOException {
        try (Connection conn = getConnection()) {
            try (Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);

                ArrayList<String> lob_data = createRandomStringArray(Constants.LOB.NCLOB);
                ArrayList<String> receivedDataFromServer = new ArrayList<>();

                createLobTable(stmt, tableName, Constants.LOB.NCLOB);
                insertData(conn, tableName, lob_data);

                try (ResultSet rs = stmt.executeQuery("SELECT * FROM [" + tableName + "] ORDER BY id ASC")) {
                    while (rs.next()) {
                        int index = rs.getInt(1);
                        NClob c = rs.getNClob(2);
                        assertEquals(c.length(), lob_data.get(index).length());
                        try (Reader reader = c.getCharacterStream()) {
                            String received = getStringFromReader(reader, c.length());// streaming string
                            receivedDataFromServer.add(received);
                            assertEquals(lob_data.get(index), received);// compare streamed string to initial string
                            c.free();
                        }
                    }
                }
                for (int i = 0; i < lob_data.size(); i++) {
                    assertEquals(receivedDataFromServer.get(i), lob_data.get(i));// compare static string to streamed
                                                                                 // string
                }
            } finally {
                try (Statement stmt = conn.createStatement()) {
                    TestUtils.dropTableIfExists(tableName, stmt);
                }
            }
        }
    }

    @Nested
    public class TestPLP {
        private String tableName;

        @AfterEach
        public void cleanUp() {
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
                TestUtils.dropTableIfExists(tableName, stmt);
            } catch (SQLException ex) {
                fail(ex.getMessage());
            }
        }

        @Test
        public void testGetAsciiStreamOnXml() {
            tableName = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TestXmlTable")));
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 XML NULL)");
                stmt.executeUpdate("INSERT INTO " + tableName + " (col1) VALUES ('<root><child>Hello</child></root>')");
                stmt.executeUpdate("INSERT INTO " + tableName + " (col1) VALUES (NULL)");

                try (ResultSet rs = stmt.executeQuery("SELECT col1 FROM " + tableName)) {
                    while (rs.next()) {
                        try {
                            InputStream asciiStream = rs.getAsciiStream(1);
                            // If no exception is thrown, assert the value is null
                            assertNull(asciiStream, "Expected null for NULL value, but got a non-null InputStream");
                        } catch (SQLException e) {
                            // Ensure that only expected exceptions occur
                            assertTrue(e.getMessage().contains("The conversion from xml to AsciiStream is unsupported."),
                                    "Unexpected SQLException message: " + e.getMessage());
                        }
                    }
                }
            } catch (SQLException e) {
                fail("Database setup or execution failed: " + e.getMessage());
            }
        }

        @Test
        public void testGetBinaryStreamOnVarchar() {
            tableName = TestUtils.escapeSingleQuotes(AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("TestPLPTable")));
            try (Connection conn = getConnection();
                Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE " + tableName + " (col1 VARCHAR(50) NULL)");
                stmt.executeUpdate("INSERT INTO " + tableName + " (col1) VALUES ('TestValue')");
                stmt.executeUpdate("INSERT INTO " + tableName + " (col1) VALUES (NULL)");

                try (ResultSet rs = stmt.executeQuery("SELECT col1 FROM " + tableName)) {
                    while (rs.next()) {
                        try {
                            InputStream binaryStream = rs.getBinaryStream(1);
                            // If no exception is thrown, assert the value is null
                            assertNull(binaryStream, "Expected null for NULL value, but got a non-null InputStream");
                        } catch (SQLException e) {
                            // Ensure that only expected exceptions occur
                            assertTrue(e.getMessage().contains("The conversion from varchar to BinaryStream is unsupported."),
                                    "Unexpected SQLException message: " + e.getMessage());
                        }
                    }
                }
            } catch (SQLException e) {
                fail("Database setup or execution failed: " + e.getMessage());
            }
        }
    }

    /**
     * Verifies that passing a negative stream length to setCharacterStream, setBinaryStream, or
     * setAsciiStream throws an SQLException reporting that the length is not valid.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testNegativeStreamLengths() throws SQLException {
        tableName = RandomUtil.getIdentifier("negStreamLen");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (col1 char(20))");
            try (PreparedStatement ps = conn
                    .prepareStatement("INSERT INTO " + escaped + " (col1) VALUES (?)")) {

                try {
                    ps.setCharacterStream(1, new StringReader("eeep"), -4);
                    ps.executeUpdate();
                    fail("setCharacterStream with negative length should throw");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("-4") && e.getMessage().contains("not valid"),
                            "Unexpected exception for setCharacterStream: " + e.getMessage());
                }

                try {
                    ps.setBinaryStream(1, new ByteArrayInputStream(new byte[3]), -4);
                    ps.executeUpdate();
                    fail("setBinaryStream with negative length should throw");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("-4") && e.getMessage().contains("not valid"),
                            "Unexpected exception for setBinaryStream: " + e.getMessage());
                }

                try {
                    ps.setAsciiStream(1, new ByteArrayInputStream(new byte[3]), -4);
                    ps.executeUpdate();
                    fail("setAsciiStream with negative length should throw");
                } catch (SQLException e) {
                    assertTrue(e.getMessage().contains("-4") && e.getMessage().contains("not valid"),
                            "Unexpected exception for setAsciiStream: " + e.getMessage());
                }
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
        }
    }

    /**
     * Verifies that on a scrollable ResultSet a character column value is re-readable after the cursor
     * is repositioned to the same row.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testResettabilityRS() throws SQLException {
        tableName = RandomUtil.getIdentifier("resettabilityRS");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        String value = getRandomString(2000, "ABCDEFGHIJKLMNOPQRSTUVWXYZ");
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (id int primary key, col1 varchar(max))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + escaped + " VALUES (?, ?)")) {
                ps.setInt(1, 1);
                ps.setString(2, value);
                ps.executeUpdate();
            }
            try (Statement scroll = conn.createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE,
                    ResultSet.CONCUR_READ_ONLY);
                    ResultSet rs = scroll.executeQuery("SELECT col1 FROM " + escaped + " ORDER BY id")) {
                assertTrue(rs.next());
                String firstRead = rs.getString(1);
                assertEquals(value, firstRead, "First read of value");

                // Reposition to the same row and re-read: value must be intact (reset between reads).
                rs.beforeFirst();
                assertTrue(rs.next());
                String secondRead = rs.getString(1);
                assertEquals(value, secondRead, "Re-read of value after cursor reset");
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
        }
    }

    /**
     * Verifies that streaming a large result set (many rows, each with a sizable value) is fully
     * consumable without error.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testMegaSelect() throws SQLException {
        tableName = RandomUtil.getIdentifier("megaSelect");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        int rowCount = 1000;
        String value = getRandomString(500, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (id int, col1 varchar(600))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + escaped + " VALUES (?, ?)")) {
                for (int i = 0; i < rowCount; i++) {
                    ps.setInt(1, i);
                    ps.setString(2, value);
                    ps.addBatch();
                    if (i % 100 == 0) {
                        ps.executeBatch();
                    }
                }
                ps.executeBatch();
            }
            int read = 0;
            try (ResultSet rs = stmt.executeQuery("SELECT id, col1 FROM " + escaped)) {
                while (rs.next()) {
                    // Consume the value to exercise streaming.
                    assertEquals(value.length(), rs.getString(2).length());
                    read++;
                }
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
            assertEquals(rowCount, read, "All rows from a large result set should be streamed");
        }
    }

    /**
     * Verifies that if a character stream's read throws during executeUpdate, the update fails but the
     * connection remains usable.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testRepro39941() throws SQLException {
        tableName = RandomUtil.getIdentifier("repro39941");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (k1 nvarchar(4) not null, fclob varchar(max))");
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + escaped + " VALUES (?, ?)")) {
                ps.setString(1, "F");
                // A reader that throws on read; the driver should surface an error, not break the connection.
                ps.setCharacterStream(2, new Reader() {
                    @Override
                    public int read(char[] cbuf, int off, int len) throws IOException {
                        throw new IOException("simulated read failure");
                    }

                    @Override
                    public void close() {}
                }, 10);
                assertThrows(SQLException.class, () -> ps.executeUpdate(),
                        "executeUpdate should fail when the character stream throws during read");
            }

            // The connection must still be usable.
            try (Statement check = conn.createStatement(); ResultSet rs = check.executeQuery("SELECT @@TRANCOUNT")) {
                assertTrue(rs.next(), "Connection should remain usable after a stream read failure");
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
        }
    }

    /**
     * Verifies writing a Clob via the setCharacterStream(1) Writer, inserting it, and reading the
     * value back unchanged. Exercises the Clob output-writer path.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testClobSetCharacterStreamWrite() throws Exception {
        tableName = RandomUtil.getIdentifier("clobWriter");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        String value = getRandomString(3000, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (col1 varchar(max))");
            try {
                Clob clob = conn.createClob();
                try (Writer w = clob.setCharacterStream(1)) {
                    w.write(value);
                }
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + escaped + " VALUES (?)")) {
                    ps.setClob(1, clob);
                    ps.executeUpdate();
                }
                try (ResultSet rs = stmt.executeQuery("SELECT col1 FROM " + escaped)) {
                    assertTrue(rs.next());
                    assertEquals(value, rs.getString(1), "Clob written via character-stream should round-trip");
                }
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
        }
    }

    /**
     * Verifies writing a Clob via the setAsciiStream(1) OutputStream, inserting it, and reading the
     * value back unchanged. Exercises the Clob ASCII output-stream path.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testClobSetAsciiStreamWrite() throws Exception {
        tableName = RandomUtil.getIdentifier("clobAscii");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        String value = getRandomString(2000, "ABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789");
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (col1 varchar(max))");
            try {
                Clob clob = conn.createClob();
                try (OutputStream os = clob.setAsciiStream(1)) {
                    os.write(value.getBytes(java.nio.charset.StandardCharsets.US_ASCII));
                }
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + escaped + " VALUES (?)")) {
                    ps.setClob(1, clob);
                    ps.executeUpdate();
                }
                try (ResultSet rs = stmt.executeQuery("SELECT col1 FROM " + escaped)) {
                    assertTrue(rs.next());
                    assertEquals(value, rs.getString(1), "Clob written via ASCII-stream should round-trip");
                }
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
        }
    }

    /**
     * Verifies writing a Blob via the setBinaryStream(1) OutputStream, inserting it, and reading the
     * value back unchanged. Exercises the Blob output-stream path.
     */
    @Test
    @Tag(Constants.legacyFx)
    @Tag(Constants.legacyFxDataTypes)
    public void testBlobSetBinaryStreamWrite() throws Exception {
        tableName = RandomUtil.getIdentifier("blobStream");
        String escaped = AbstractSQLGenerator.escapeIdentifier(tableName);
        byte[] value = new byte[4096];
        Constants.RANDOM.nextBytes(value);
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escaped, stmt);
            stmt.executeUpdate("CREATE TABLE " + escaped + " (col1 varbinary(max))");
            try {
                Blob blob = conn.createBlob();
                try (OutputStream os = blob.setBinaryStream(1)) {
                    os.write(value);
                }
                try (PreparedStatement ps = conn.prepareStatement("INSERT INTO " + escaped + " VALUES (?)")) {
                    ps.setBlob(1, blob);
                    ps.executeUpdate();
                }
                try (ResultSet rs = stmt.executeQuery("SELECT col1 FROM " + escaped)) {
                    assertTrue(rs.next());
                    assertTrue(java.util.Arrays.equals(value, rs.getBytes(1)),
                            "Blob written via binary-stream should round-trip");
                }
            } finally {
                TestUtils.dropTableIfExists(escaped, stmt);
            }
        }
    }
}