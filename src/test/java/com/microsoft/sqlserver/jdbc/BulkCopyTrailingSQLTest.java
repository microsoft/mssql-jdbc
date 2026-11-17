/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.BatchUpdateException;
import java.sql.ResultSet;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


/**
 * Database-independent checks of Bulk Copy routing for comments and unsupported trailing SQL on statement reuse.
 */
class BulkCopyTrailingSQLTest {
    @ParameterizedTest
    @MethodSource("batchModes")
    void testRejectedSQLStaysRejectedAfterExecutionFailure(boolean firstLargeBatch, boolean alternate,
            String trailer) throws Exception {
        SQLServerConnection connection = mockConnection();
        // Stop at normal command execution, before any network I/O. Each reuse must reach this same path.
        SQLServerException executionFailure = new SQLServerException("Simulated batch execution failure", null);
        when(connection.executeCommand(any(TDSCommand.class))).thenThrow(executionFailure);

        try (SQLServerPreparedStatement pstmt = new SQLServerPreparedStatement(connection,
                "INSERT INTO t (Id, Data) VALUES (?, ?)" + trailer, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING)) {
            for (int batch = 0; batch < 3; batch++) {
                pstmt.addBatch();
                boolean largeBatch = alternate && batch % 2 == 1 ? !firstLargeBatch : firstLargeBatch;
                SQLServerException actual = largeBatch ? assertThrows(SQLServerException.class,
                        pstmt::executeLargeBatch) : assertThrows(SQLServerException.class, pstmt::executeBatch);
                assertSame(executionFailure, actual);
                pstmt.clearBatch();
                pstmt.clearParameters();
            }
            // The Bulk Copy path requests destination metadata through this overload before sending rows.
            verify(connection, never()).createStatement(anyInt(), anyInt(), anyInt(),
                    any(SQLServerStatementColumnEncryptionSetting.class));
        }
    }

    @ParameterizedTest
    @MethodSource("supportedBatchModes")
    void testCommentOnlyTrailersKeepBulkCopyAfterExecutionFailure(boolean firstLargeBatch, boolean alternate,
            String trailer) throws Exception {
        SQLServerConnection connection = mockConnection();
        // Stop at Bulk Copy metadata lookup, before network I/O. Reuse must still reach this path.
        SQLServerException metadataFailure = new SQLServerException("Simulated metadata lookup failure", null);
        when(connection.createStatement(anyInt(), anyInt(), anyInt(),
                any(SQLServerStatementColumnEncryptionSetting.class))).thenThrow(metadataFailure);
        when(connection.executeCommand(any(TDSCommand.class)))
                .thenThrow(new SQLServerException("Unexpected normal batch execution", null));

        try (SQLServerPreparedStatement pstmt = new SQLServerPreparedStatement(connection,
                "INSERT INTO t (Id, Data) VALUES (?, ?)" + trailer, ResultSet.TYPE_FORWARD_ONLY,
                ResultSet.CONCUR_READ_ONLY, SQLServerStatementColumnEncryptionSetting.USE_CONNECTION_SETTING)) {
            for (int batch = 0; batch < 3; batch++) {
                pstmt.addBatch();
                boolean largeBatch = alternate && batch % 2 == 1 ? !firstLargeBatch : firstLargeBatch;
                BatchUpdateException actual = largeBatch ? assertThrows(BatchUpdateException.class,
                        pstmt::executeLargeBatch) : assertThrows(BatchUpdateException.class, pstmt::executeBatch);
                assertEquals(metadataFailure.getMessage(), actual.getMessage());
                pstmt.clearBatch();
                pstmt.clearParameters();
            }
            verify(connection, times(3)).createStatement(anyInt(), anyInt(), anyInt(),
                    any(SQLServerStatementColumnEncryptionSetting.class));
            verify(connection, never()).executeCommand(any(TDSCommand.class));
        }
    }

    private static SQLServerConnection mockConnection() {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getPrepareMethod()).thenReturn("prepexec");
        when(connection.getResponseBuffering()).thenReturn("adaptive");
        when(connection.getUseBulkCopyForBatchInsert()).thenReturn(true);
        return connection;
    }

    private static Stream<Arguments> batchModes() {
        return Stream
                .of(" OPTION (RECOMPILE)", ", (?, ?)", " /* outer /* inner */ outer */ OPTION (RECOMPILE)",
                        " /* outer /* inner */ outer */ , (?, ?)", " /* outer /* inner */ outer */; DELETE FROM t",
                        " /* unterminated", " /* outer /* inner */", " /* outer /* inner",
                        " /* outer /* inner */ outer */ /* unterminated")
                .flatMap(BulkCopyTrailingSQLTest::executionModes);
    }

    private static Stream<Arguments> supportedBatchModes() {
        return Stream.of("", " ; ; ", " /* ordinary */", " -- trailing comment", " /* outer /* inner */ outer */",
                " /* level 1 /* level 2 /* level 3 */ */ */", " /*/**/*/",
                " ; /* outer /* inner */ outer */ ; /* next */ -- end\n ;", " /* outer '--' '/*' inner */ outer */")
                .flatMap(BulkCopyTrailingSQLTest::executionModes);
    }

    private static Stream<Arguments> executionModes(String trailer) {
        return Stream.of(Arguments.of(false, false, trailer), Arguments.of(true, false, trailer),
                Arguments.of(false, true, trailer), Arguments.of(true, true, trailer));
    }
}
