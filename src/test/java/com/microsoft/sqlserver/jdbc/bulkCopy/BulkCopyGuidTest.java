/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.bulkCopy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCSVFileRecord;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Tests bulk copy into a uniqueidentifier destination column, which the driver sends in the native 16 byte
 * representation instead of as a character string the server has to convert for every row.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.bulkCopy)
public class BulkCopyGuidTest extends AbstractTest {

    private static final String GUID = "6f9619ff-8b86-d011-b42d-00c04fc964ff";
    private static final String STORED_GUID = "6F9619FF-8B86-D011-B42D-00C04FC964FF";
    private static final int GUID_TEXT_LENGTH = 36;

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testBulkCopyGuidRoundTripsValues() throws Exception {
        String guidDestTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("guidRoundTripDest"));
        UUID asObject = UUID.randomUUID();
        UUID asString = UUID.randomUUID();
        List<Object> values = Arrays.asList(asObject, asString.toString().toLowerCase(), null);

        try {
            createGuidTable(guidDestTable);

            bulkCopyGuidValues(guidDestTable, values);

            List<String> stored = readGuids(guidDestTable);
            assertEquals(3, stored.size());
            assertEquals(asObject, UUID.fromString(stored.get(0)));
            assertEquals(asString, UUID.fromString(stored.get(1)));
            assertNull(stored.get(2));
        } finally {
            dropTable(guidDestTable);
        }
    }

    /**
     * Sending a uniqueidentifier column natively may not change which values the driver accepts. A source column of
     * type CHAR carries the character wire format the driver used for GUID source columns before, so running a value
     * through both source types shows whether a rendering is still handled the same way. Only precisions that can hold
     * the rendering are paired with it, since the character format truncates to the declared precision before the
     * server converts.
     */
    @ParameterizedTest
    @MethodSource("guidRenderings")
    public void testBulkCopyGuidNativeFormatKeepsCharacterFormatBehavior(String rendering,
            int declaredPrecision) throws Exception {
        String viaCharacterFormat = bulkCopyGuidOutcome(rendering, java.sql.Types.CHAR, declaredPrecision);
        String viaNativeFormat = bulkCopyGuidOutcome(rendering, microsoft.sql.Types.GUID, declaredPrecision);

        assertEquals(viaCharacterFormat, viaNativeFormat);
    }

    private static Stream<Arguments> guidRenderings() {
        List<String> renderings = Arrays.asList(GUID, GUID.toUpperCase(), "{" + GUID + "}", " " + GUID + " ",
                GUID.replace("-", ""), "(" + GUID + ")", "urn:uuid:" + GUID, "not-a-guid", "");
        return renderings.stream().flatMap(rendering -> IntStream.of(GUID_TEXT_LENGTH, GUID_TEXT_LENGTH + 2)
                .filter(precision -> rendering.length() <= precision)
                .mapToObj(precision -> Arguments.of(rendering, precision)));
    }

    /**
     * The one difference the native wire format brings: the character format truncated a value longer than the declared
     * precision before the server converted it, so the registry format did not fit into the 36 characters a GUID column
     * is usually declared with. Parsing on the client no longer depends on the declared precision.
     */
    @Test
    public void testBulkCopyGuidNativeFormatIgnoresDeclaredPrecision() throws Exception {
        String braced = "{" + GUID + "}";

        assertEquals("rejected", bulkCopyGuidOutcome(braced, java.sql.Types.CHAR, GUID_TEXT_LENGTH));
        assertEquals("stored " + STORED_GUID, bulkCopyGuidOutcome(braced, microsoft.sql.Types.GUID, GUID_TEXT_LENGTH));
    }

    /**
     * A value that cannot be parsed as a GUID has to be reported as a {@link SQLServerException}, never as an unchecked
     * exception escaping writeToServer in the middle of a batch. The failure happening on the client also shows that
     * the native wire format is in use without needing Extended Events, because the character wire format would have
     * sent the value to the server and failed there instead.
     */
    @ParameterizedTest
    @ValueSource(strings = {"not-a-guid", "6F9619FF-8B86-D011-B42D", "6f9619ff8b86d011b42d00c04fc964ff", "",
            " 6f9619ff-8b86-d011-b42d-00c04fc964ff ", "(6f9619ff-8b86-d011-b42d-00c04fc964ff)",
            "urn:uuid:6f9619ff-8b86-d011-b42d-00c04fc964ff"})
    public void testBulkCopyGuidUnparsableValueFailsOnClient(String value) throws Exception {
        String guidDestTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("guidUnparsableDest"));

        try {
            createGuidTable(guidDestTable);

            SQLServerException e = assertThrows(SQLServerException.class,
                    () -> bulkCopyGuidValues(guidDestTable, Arrays.asList((Object) value)));

            assertEquals("An error occurred while converting the '" + value + "' value to JDBC data type GUID.",
                    e.getMessage());
        } finally {
            dropTable(guidDestTable);
        }
    }

    @Test
    public void testBulkCopyGuidByteArrayValueFailsOnClient() throws Exception {
        String guidDestTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("guidByteArrayDest"));

        try {
            createGuidTable(guidDestTable);

            SQLServerException e = assertThrows(SQLServerException.class,
                    () -> bulkCopyGuidValues(guidDestTable, Arrays.asList((Object) new byte[16])));

            assertTrue(e.getMessage().startsWith("An error occurred while converting the '[B@"),
                    "Unexpected message: " + e.getMessage());
        } finally {
            dropTable(guidDestTable);
        }
    }

    /**
     * A GUID source column keeps being sent as a character string when the destination is not a uniqueidentifier, since
     * only a uniqueidentifier destination can read the native 16 byte representation.
     */
    @ParameterizedTest
    @ValueSource(strings = {"char(36)", "varchar(36)", "nchar(36)", "nvarchar(36)"})
    public void testBulkCopyGuidIntoCharacterDestination(String destColumnType) throws Exception {
        String destTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("guidCharDest"));
        UUID guid = UUID.randomUUID();

        try {
            createTable(destTable, destColumnType);

            bulkCopyGuidValues(destTable, Arrays.asList((Object) guid));

            assertEquals(guid, UUID.fromString(readStrings(destTable).get(0).trim()));
        } finally {
            dropTable(destTable);
        }
    }

    /**
     * Shows on the server side that a GUID source column reaches a uniqueidentifier destination without a conversion,
     * while a CHAR source column still gets converted by the server. The CHAR case is the control that proves the
     * capture works, so that the GUID case cannot pass by capturing nothing at all.
     */
    @ParameterizedTest
    @MethodSource("guidSourceTypes")
    public void testBulkCopyGuidConversionOnServer(int srcJdbcType, boolean expectConversion) throws Exception {
        String guidDestTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("guidConversionDest"));

        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            assumeTrue(canCaptureImplicitConversions(stmt),
                    "Requires a server scoped Extended Events session, so it needs ALTER ANY EVENT SESSION and VIEW"
                            + " SERVER STATE on a server that is not Azure SQL Database.");

            try (ImplicitConversionCapture capture = new ImplicitConversionCapture(conn, stmt)) {
                stmt.execute("CREATE TABLE " + guidDestTable + " (id uniqueidentifier)");
                try {
                    SQLServerBulkCSVFileRecord fileRecord = guidFileRecord(UUID.randomUUID().toString());
                    fileRecord.addColumnMetadata(1, "id", srcJdbcType, GUID_TEXT_LENGTH, 0);

                    try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                        bulkCopy.setDestinationTableName(guidDestTable);
                        bulkCopy.writeToServer(fileRecord);
                    }

                    String conversions = capture.conversionsFor(guidDestTable);

                    assertEquals(expectConversion, !conversions.isEmpty(),
                            "Conversions the server reported for the bulk insert: " + conversions);
                } finally {
                    TestUtils.dropTableIfExists(guidDestTable, stmt);
                }
            }
        }
    }

    private static Stream<Arguments> guidSourceTypes() {
        return Stream.of(Arguments.of(microsoft.sql.Types.GUID, false), Arguments.of(java.sql.Types.CHAR, true));
    }

    /**
     * Returns what a single row bulk copy of the value into a uniqueidentifier column did, so that the character and
     * the native wire format can be compared. The messages of a rejection are not comparable, since the character
     * format is rejected by the server and the native one by the driver.
     */
    private static String bulkCopyGuidOutcome(String rendering, int srcJdbcType,
            int declaredPrecision) throws Exception {
        String guidDestTable = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("guidParityDest"));

        try {
            createGuidTable(guidDestTable);
            try {
                bulkCopyGuidValues(guidDestTable, srcJdbcType, declaredPrecision, Arrays.asList((Object) rendering));
            } catch (SQLServerException e) {
                return "rejected";
            }
            return "stored " + readGuids(guidDestTable).get(0);
        } finally {
            dropTable(guidDestTable);
        }
    }

    private static void createGuidTable(String escapedTableName) throws Exception {
        createTable(escapedTableName, "uniqueidentifier");
    }

    private static void createTable(String escapedTableName, String columnType) throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            stmt.execute("CREATE TABLE " + escapedTableName + " (guidCol " + columnType + ")");
        }
    }

    /**
     * Cleans up on a connection of its own, since a bulk copy that failed mid batch leaves its own connection unusable.
     */
    private static void dropTable(String escapedTableName) throws Exception {
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement()) {
            TestUtils.dropTableIfExists(escapedTableName, stmt);
        }
    }

    private static List<String> readGuids(String escapedTableName) throws Exception {
        List<String> guids = new ArrayList<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                SQLServerResultSet rs = (SQLServerResultSet) stmt
                        .executeQuery("SELECT guidCol FROM " + escapedTableName)) {
            while (rs.next()) {
                guids.add(rs.getUniqueIdentifier(1));
            }
        }
        return guids;
    }

    private static List<String> readStrings(String escapedTableName) throws Exception {
        List<String> values = new ArrayList<>();
        try (Connection conn = getConnection(); Statement stmt = conn.createStatement();
                ResultSet rs = stmt.executeQuery("SELECT guidCol FROM " + escapedTableName)) {
            while (rs.next()) {
                values.add(rs.getString(1));
            }
        }
        return values;
    }

    private static void bulkCopyGuidValues(String escapedTableName, List<Object> values) throws Exception {
        bulkCopyGuidValues(escapedTableName, microsoft.sql.Types.GUID, GUID_TEXT_LENGTH, values);
    }

    private static void bulkCopyGuidValues(String escapedTableName, int srcJdbcType, int declaredPrecision,
            List<Object> values) throws Exception {
        try (Connection conn = getConnection(); SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
            bulkCopy.setDestinationTableName(escapedTableName);
            bulkCopy.writeToServer(new GuidBulkRecord(srcJdbcType, declaredPrecision, values));
        }
    }

    private static SQLServerBulkCSVFileRecord guidFileRecord(String guid) throws Exception {
        byte[] bytes = ("guidcol\n" + guid + "\n").getBytes(StandardCharsets.UTF_8);
        try (InputStream inputStream = new ByteArrayInputStream(bytes)) {
            return new SQLServerBulkCSVFileRecord(inputStream, Constants.UTF8, Constants.COMMA, true);
        }
    }

    /**
     * Server scoped Extended Events sessions do not exist on Azure SQL Database and need permissions a test principal
     * does not necessarily have, so the capture based assertions can only run where both are given.
     */
    private static boolean canCaptureImplicitConversions(Statement stmt) throws SQLException {
        try (ResultSet rs = stmt.executeQuery("SELECT HAS_PERMS_BY_NAME(null, null, 'ALTER ANY EVENT SESSION'),"
                + " HAS_PERMS_BY_NAME(null, null, 'VIEW SERVER STATE'),"
                + " CAST(SERVERPROPERTY('EngineEdition') AS int)")) {
            rs.next();
            return 1 == rs.getInt(1) && 1 == rs.getInt(2) && Constants.ENGINE_EDITION_FOR_SQL_AZURE != rs.getInt(3);
        }
    }

    /**
     * A running Extended Events session capturing plan_affecting_convert, which the server raises once per compiled
     * plan when a bulk insert has to convert an incoming column to the destination type.
     */
    private static final class ImplicitConversionCapture implements AutoCloseable {

        private static final long DISPATCH_TIMEOUT_MILLIS = 30000;

        private final Connection conn;
        private final Statement stmt;
        private final String sessionName;

        ImplicitConversionCapture(Connection conn, Statement stmt) throws SQLException {
            this.conn = conn;
            this.stmt = stmt;
            this.sessionName = "mssqljdbc_guid_" + UUID.randomUUID().toString().replace("-", "");

            stmt.execute("CREATE EVENT SESSION [" + sessionName + "] ON SERVER ADD EVENT"
                    + " sqlserver.plan_affecting_convert (ACTION (sqlserver.sql_text)) ADD TARGET package0.ring_buffer"
                    + " WITH (MAX_DISPATCH_LATENCY = 1 SECONDS)");
            try {
                stmt.execute("ALTER EVENT SESSION [" + sessionName + "] ON SERVER STATE = START");
            } catch (SQLException e) {
                close();
                throw e;
            }
        }

        /**
         * Returns the conversions the server had to run for the bulk inserted column, which it reports against the
         * [!BulkInsert] pseudo table. A conversion of its own is issued first and waited for, so that the events of the
         * bulk copy are known to have reached the ring buffer.
         */
        String conversionsFor(String escapedTableName) throws Exception {
            String barrier = "xeBarrier" + UUID.randomUUID().toString().replace("-", "");
            stmt.execute("/*" + barrier + "*/ SELECT COUNT(*) FROM " + escapedTableName
                    + " WHERE CAST(id AS varchar(36)) = N'x'");

            long giveUpAt = System.currentTimeMillis() + DISPATCH_TIMEOUT_MILLIS;
            String events = readRingBuffer();
            while (!events.contains(barrier) && System.currentTimeMillis() < giveUpAt) {
                Thread.sleep(500);
                events = readRingBuffer();
            }
            assertTrue(events.contains(barrier), "Extended Events session did not report the expected conversion.");

            // The session sees conversions from the whole server, so keep only the ones the bulk insert into this
            // table caused. Other tests running in parallel forks bulk copy into uniqueidentifier columns too.
            return Arrays.stream(events.split("</event>"))
                    .filter(event -> event.contains("!BulkInsert") && event.contains(escapedTableName))
                    .collect(Collectors.joining("\n"));
        }

        private String readRingBuffer() throws SQLException {
            try (PreparedStatement pstmt = conn.prepareStatement("SELECT CAST(t.target_data AS NVARCHAR(MAX)) FROM"
                    + " sys.dm_xe_session_targets AS t JOIN sys.dm_xe_sessions AS s"
                    + " ON s.address = t.event_session_address"
                    + " WHERE s.name = ? AND t.target_name = 'ring_buffer'")) {
                pstmt.setString(1, sessionName);
                try (ResultSet rs = pstmt.executeQuery()) {
                    String targetData = rs.next() ? rs.getString(1) : null;
                    return (null == targetData) ? "" : targetData;
                }
            }
        }

        @Override
        public void close() throws SQLException {
            stmt.execute("IF EXISTS (SELECT 1 FROM sys.server_event_sessions WHERE name = '" + sessionName + "')"
                    + " DROP EVENT SESSION [" + sessionName + "] ON SERVER");
        }
    }

    private static final class GuidBulkRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;

        private final int columnType;
        private final int precision;
        private final List<Object> values;
        private int row = -1;

        GuidBulkRecord(int columnType, int precision, List<Object> values) {
            this.columnType = columnType;
            this.precision = precision;
            this.values = values;
        }

        @Override
        public Set<Integer> getColumnOrdinals() {
            return new HashSet<>(Arrays.asList(1));
        }

        @Override
        public String getColumnName(int column) {
            return "guidCol";
        }

        @Override
        public int getColumnType(int column) {
            return columnType;
        }

        @Override
        public int getPrecision(int column) {
            return precision;
        }

        @Override
        public int getScale(int column) {
            return 0;
        }

        @Override
        public Object[] getRowData() {
            return new Object[] {values.get(row)};
        }

        @Override
        public boolean next() {
            return ++row < values.size();
        }
    }
}
