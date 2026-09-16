/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.sql.ResultSet;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;


/**
 * Database-independent checks that rejected trailing SQL stays on the normal batch path after execution fails.
 */
class BulkCopyTrailingSQLTest {
    @ParameterizedTest
    @MethodSource("batchModes")
    void testRejectedSQLStaysRejectedAfterExecutionFailure(boolean firstLargeBatch, boolean alternate,
            String trailer) throws Exception {
        SQLServerConnection connection = mock(SQLServerConnection.class);
        when(connection.getPrepareMethod()).thenReturn("prepexec");
        when(connection.getResponseBuffering()).thenReturn("adaptive");
        when(connection.getUseBulkCopyForBatchInsert()).thenReturn(true);
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
            }
            // The Bulk Copy path requests destination metadata through this overload before sending rows.
            verify(connection, never()).createStatement(anyInt(), anyInt(), anyInt(),
                    any(SQLServerStatementColumnEncryptionSetting.class));
        }
    }

    private static Stream<Arguments> batchModes() {
        return Stream.of(" OPTION (RECOMPILE)", ", (?, ?)")
                .flatMap(trailer -> Stream.of(Arguments.of(false, false, trailer), Arguments.of(true, false, trailer),
                        Arguments.of(false, true, trailer), Arguments.of(true, true, trailer)));
    }
}
