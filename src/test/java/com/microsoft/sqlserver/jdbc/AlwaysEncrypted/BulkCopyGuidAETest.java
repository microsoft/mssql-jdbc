/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerBulkData;
import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerBulkCopy;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.PrepUtil;


/**
 * Tests bulk copy into an encrypted uniqueidentifier column, which is sent as the binary ciphertext of the value and
 * never in the native 16 byte representation a plaintext uniqueidentifier column is sent in.
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.reqExternalSetup)
@Tag(Constants.alwaysEncrypted)
public class BulkCopyGuidAETest extends AESetup {

    private static final int GUID_TEXT_LENGTH = 36;
    private static final int GUID_BYTE_LENGTH = 16;

    private static final String destTableNameAE = AbstractSQLGenerator
            .escapeIdentifier(RandomUtil.getIdentifier("bulkCopyGuidDestTableAE"));

    /**
     * An encrypted uniqueidentifier column is fed with the ciphertext of the value, so it must not be affected by a
     * plaintext uniqueidentifier column being sent in the native 16 byte representation.
     */
    @Test
    public void testBulkCopyGuidIntoEncryptedColumn() throws SQLException {
        UUID guid = UUID.randomUUID();

        try (Connection conn = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement stmt = conn.createStatement()) {
            createEncryptedGuidTable(stmt);
            try {
                try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                    bulkCopy.setDestinationTableName(destTableNameAE);
                    bulkCopy.writeToServer(
                            new GuidBulkRecord(java.sql.Types.CHAR, Arrays.asList((Object) guid.toString())));
                }

                try (SQLServerResultSet rs = (SQLServerResultSet) stmt
                        .executeQuery("SELECT guidCol FROM " + destTableNameAE)) {
                    assertTrue(rs.next());
                    assertEquals(guid, UUID.fromString(rs.getUniqueIdentifier(1)));
                }

                assertStoredValueIsCiphertext();
            } finally {
                TestUtils.dropTableIfExists(destTableNameAE, stmt);
            }
        }
    }

    /**
     * A GUID source column has never been accepted for an encrypted uniqueidentifier destination, because the driver
     * validates the conversion against the base type of the encrypted column. Pinned here so that a plaintext value can
     * never start reaching an encrypted column unnoticed.
     */
    @Test
    public void testBulkCopyGuidSourceIntoEncryptedColumnIsRejected() throws SQLException {
        try (Connection conn = PrepUtil.getConnection(AETestConnectionString, AEInfo);
                Statement stmt = conn.createStatement()) {
            createEncryptedGuidTable(stmt);
            try (SQLServerBulkCopy bulkCopy = new SQLServerBulkCopy(conn)) {
                bulkCopy.setDestinationTableName(destTableNameAE);

                SQLServerException e = assertThrows(SQLServerException.class, () -> bulkCopy
                        .writeToServer(new GuidBulkRecord(microsoft.sql.Types.GUID,
                                Arrays.asList((Object) UUID.randomUUID()))));

                assertEquals("The given value of type GUID from the data source cannot be converted to type GUID of"
                        + " the specified target column guidCol.", e.getMessage());
            } finally {
                TestUtils.dropTableIfExists(destTableNameAE, stmt);
            }
        }
    }

    /**
     * Reads the column over a connection without column encryption, where the driver cannot decrypt, to show that what
     * reached the server is the ciphertext of the value and not the 16 bytes of the GUID itself.
     */
    private void assertStoredValueIsCiphertext() throws SQLException {
        try (Connection plainConn = PrepUtil.getConnection(connectionString);
                Statement plainStmt = plainConn.createStatement();
                ResultSet rs = plainStmt.executeQuery("SELECT guidCol FROM " + destTableNameAE)) {
            assertTrue(rs.next());
            byte[] stored = rs.getBytes(1);
            assertTrue(stored.length > GUID_BYTE_LENGTH,
                    "Expected the encrypted column to hold ciphertext, but it held " + stored.length + " bytes.");
        }
    }

    private void createEncryptedGuidTable(Statement stmt) throws SQLException {
        TestUtils.dropTableIfExists(destTableNameAE, stmt);
        stmt.execute("create table " + destTableNameAE + " (guidCol uniqueidentifier ENCRYPTED WITH"
                + " (ENCRYPTION_TYPE = RANDOMIZED, ALGORITHM = 'AEAD_AES_256_CBC_HMAC_SHA_256',"
                + " COLUMN_ENCRYPTION_KEY = " + cekJks + ") NULL)");
    }

    private static final class GuidBulkRecord implements ISQLServerBulkData {
        private static final long serialVersionUID = 1L;

        private final int columnType;
        private final List<Object> values;
        private int row = -1;

        GuidBulkRecord(int columnType, List<Object> values) {
            this.columnType = columnType;
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
            return GUID_TEXT_LENGTH;
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
