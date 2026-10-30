/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLTimeoutException;
import java.sql.Statement;
import java.text.MessageFormat;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.PrepUtil;


/** Integration coverage for stream-send cancellation and direct-to-unprepared-RPC statement reuse. */
@RunWith(JUnitPlatform.class)
@Timeout(90)
public class PrepareMethodNoneStreamRecoveryTest extends AbstractTest {
    private static final String IO_FAILURE = "prepare-none-stream-failure";
    private static final String TEXT = "a\u540d\u00e9Z!";
    private static final byte[] BYTES = {0, 1, (byte) 0xFF, 127, 42};

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @ParameterizedTest(name = "mode={0}, reader={1}, batch={2}")
    @MethodSource("executionCases")
    public void testIOExceptionRecovery(String mode, boolean reader, boolean batch) throws SQLException {
        try (Connection con = openConnection(mode); Statement stmt = con.createStatement()) {
            createTable(stmt, reader);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #stream_recovery VALUES (?, ?)")) {
                seed(ps, reader);
                ps.setInt(1, 1);
                if (reader) {
                    ps.setCharacterStream(2, new FailingReader(), 4);
                } else {
                    ps.setBinaryStream(2, new FailingInputStream(), 4);
                }
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, batch));
                assertEquals("22000", error.getSQLState());
                assertEquals(0, error.getErrorCode());
                assertEquals(MessageFormat.format(SQLServerException.getErrString("R_errorReadingStream"),
                        new IOException(IO_FAILURE).toString()), error.getMessage());
                recoverAndCheck(con, ps, reader, batch);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, reader={1}, batch={2}, actualLength={3}")
    @MethodSource("lengthCases")
    public void testDeclaredLengthMismatchRecovery(String mode, boolean reader, boolean batch,
            int actualLength) throws SQLException {
        try (Connection con = openConnection(mode); Statement stmt = con.createStatement()) {
            createTable(stmt, reader);
            try (PreparedStatement ps = con.prepareStatement("INSERT INTO #stream_recovery VALUES (?, ?)")) {
                seed(ps, reader);
                ps.setInt(1, 1);
                bindStream(ps, 2, reader, actualLength, 4);
                SQLException error = assertThrows(SQLException.class, () -> execute(ps, batch));
                assertEquals("22026", error.getSQLState());
                assertEquals(0, error.getErrorCode());
                assertEquals(MessageFormat.format(SQLServerException.getErrString("R_mismatchedStreamLength"),
                        4, actualLength), error.getMessage());
                recoverAndCheck(con, ps, reader, batch);
            }
        }
    }

    @ParameterizedTest(name = "mode={0}, reader={1}")
    @MethodSource("streamCases")
    public void testQueryTimeoutAfterRpcTransition(String mode, boolean reader) throws SQLException {
        try (Connection con = openConnection(mode);
                PreparedStatement ps = con.prepareStatement("WAITFOR DELAY ?; SELECT DATALENGTH(?)")) {
            ps.setString(1, "00:00:00");
            bindLiteral(ps, reader);
            assertLength(ps, reader);
            // Same statement: literal execution, successful RPC, timeout with a fresh stream, successful RPC.
            bindStream(ps, 2, reader, 5, 5);
            assertLength(ps, reader);
            ps.setString(1, "00:00:10");
            bindStream(ps, 2, reader, 5, 5);
            ps.setQueryTimeout(1);
            SQLTimeoutException error = assertThrows(SQLTimeoutException.class, () -> {
                try (ResultSet rs = ps.executeQuery()) {
                    rs.next();
                }
            });
            assertEquals(SQLServerException.getErrString("R_queryTimedOut"), error.getMessage());
            assertEquals("HY008", error.getSQLState());
            assertEquals(0, error.getErrorCode());
            assertFalse(con.isClosed());
            ps.setQueryTimeout(10);
            ps.setString(1, "00:00:00");
            bindStream(ps, 2, reader, 5, 5);
            assertLength(ps, reader);
            try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery("SELECT 42")) {
                assertTrue(rs.next());
                assertEquals(42, rs.getInt(1));
                assertFalse(rs.next());
            }
        }
    }

    private static Stream<Arguments> streamCases() {
        return Stream.of("prepexec", "none").flatMap(mode -> Stream.of(false, true)
                .map(reader -> Arguments.of(mode, reader)));
    }

    private static Stream<Arguments> executionCases() {
        return streamCases().flatMap(args -> Stream.of(false, true)
                .map(batch -> Arguments.of(args.get()[0], args.get()[1], batch)));
    }

    private static Stream<Arguments> lengthCases() {
        return executionCases().flatMap(args -> Stream.of(3, 5)
                .map(length -> Arguments.of(args.get()[0], args.get()[1], args.get()[2], length)));
    }

    private static Connection openConnection(String mode) throws SQLException {
        String url = TestUtils.addOrOverrideProperty(connectionString, "prepareMethod", mode);
        String[][] properties = {{"columnEncryptionSetting", "Disabled"}, {"disableStatementPooling", "true"},
                {"statementPoolingCacheSize", "0"}, {"enablePrepareOnFirstPreparedStatementCall", "true"},
                {"useBulkCopyForBatchInsert", "false"}, {"sendStringParametersAsUnicode", "true"},
                {"packetSize", "4096"}, {"queryTimeout", "10"}, {"cancelQueryTimeout", "5"},
                {"socketTimeout", "20000"}, {"retryExec", ""}};
        for (String[] property : properties) {
            url = TestUtils.addOrOverrideProperty(url, property[0], property[1]);
        }
        return PrepUtil.getConnection(url);
    }

    private static void createTable(Statement stmt, boolean reader) throws SQLException {
        stmt.execute("CREATE TABLE #stream_recovery (id int, value "
                + (reader ? "nvarchar(max)" : "varbinary(max)") + ")");
    }

    private static void bindLiteral(PreparedStatement ps, boolean reader) throws SQLException {
        if (reader) {
            ps.setString(2, TEXT);
        } else {
            ps.setBytes(2, BYTES);
        }
    }

    private static void seed(PreparedStatement ps, boolean reader) throws SQLException {
        ps.setInt(1, 0);
        bindLiteral(ps, reader);
        assertEquals(1, ps.executeUpdate());
    }

    private static void bindStream(PreparedStatement ps, int index, boolean reader, int actualLength,
            int declaredLength) throws SQLException {
        if (reader) {
            ps.setCharacterStream(index, new StringReader(TEXT.substring(0, actualLength)), declaredLength);
        } else {
            ps.setBinaryStream(index, new ByteArrayInputStream(BYTES, 0, actualLength), declaredLength);
        }
    }

    private static void execute(PreparedStatement ps, boolean batch) throws SQLException {
        if (batch) {
            ps.addBatch();
            assertArrayEquals(new int[] {1}, ps.executeBatch());
        } else {
            assertEquals(1, ps.executeUpdate());
        }
    }

    private static void recoverAndCheck(Connection con, PreparedStatement ps, boolean reader,
            boolean batch) throws SQLException {
        // These tiny, known-length values fail before any packet is sent. TDSWriter.error interrupts the
        // incomplete request; TDSCommand.onRequestComplete discards it without expecting an attention ack.
        // This is the recoverable prepexec contract, not a claim about arbitrary partially sent LOBs.
        assertFalse(con.isClosed());
        assertFalse(ps.isClosed());
        ps.clearBatch();
        assertRows(con, reader, 0);
        for (int id = 2; id <= 3; id++) {
            ps.setInt(1, id);
            bindStream(ps, 2, reader, 5, 5);
            execute(ps, batch);
        }
        assertRows(con, reader, 0, 2, 3);
    }

    private static void assertRows(Connection con, boolean reader, int... ids) throws SQLException {
        try (Statement stmt = con.createStatement(); ResultSet rs = stmt.executeQuery(
                "SELECT id, CONVERT(varbinary(max), value) FROM #stream_recovery ORDER BY id")) {
            for (int id : ids) {
                assertTrue(rs.next());
                assertEquals(id, rs.getInt(1));
                assertArrayEquals(reader ? TEXT.getBytes(StandardCharsets.UTF_16LE) : BYTES, rs.getBytes(2));
            }
            assertFalse(rs.next(), "No failed, duplicate, or ghost rows may survive");
        }
    }

    private static void assertLength(PreparedStatement ps, boolean reader) throws SQLException {
        try (ResultSet rs = ps.executeQuery()) {
            assertTrue(rs.next());
            assertEquals(reader ? 10 : 5, rs.getInt(1));
            assertFalse(rs.next());
        }
    }

    private static final class FailingInputStream extends InputStream {
        private int position;

        @Override
        public int read() throws IOException {
            if (position == 3) {
                throw new IOException(IO_FAILURE);
            }
            return BYTES[position++] & 0xFF;
        }

        @Override
        public int read(byte[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (position == 3) {
                throw new IOException(IO_FAILURE);
            }
            int count = Math.min(length, 3 - position);
            System.arraycopy(BYTES, position, buffer, offset, count);
            position += count;
            return count;
        }
    }

    private static final class FailingReader extends Reader {
        private int position;

        @Override
        public int read(char[] buffer, int offset, int length) throws IOException {
            if (length == 0) {
                return 0;
            }
            if (position == 3) {
                throw new IOException(IO_FAILURE);
            }
            int count = Math.min(length, 3 - position);
            TEXT.getChars(position, position + count, buffer, offset);
            position += count;
            return count;
        }

        @Override
        public void close() {
            // No resources; stream ownership and reuse after failure are deliberately not asserted.
        }
    }
}