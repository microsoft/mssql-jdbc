/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;


@Tag("Unit")
public class ParameterLengthHistoryTest {
    @Test
    public void testDisabledParametersDoNotAllocateHistory() {
        Parameter parameter = new Parameter(false);
        assertNull(parameter.getLengthHistory());
        assertNull(parameter.cloneForBatch().getLengthHistory());
        assertNull(new Parameter(true, false).getLengthHistory());
    }

    @ParameterizedTest
    @ValueSource(booleans = {false, true})
    public void testBatchClonesShareHistoryBeforeFirstObservation(boolean honorAE) {
        Parameter parameter = new Parameter(honorAE, true);
        Parameter first = parameter.cloneForBatch();
        Parameter second = parameter.cloneForBatch();
        Parameter.LengthHistory history = parameter.getLengthHistory();
        assertSame(history, first.getLengthHistory());
        assertSame(history, second.getLengthHistory());
        assertSame(history, first.cloneForBatch().getLengthHistory());
        assertNotSame(history, new Parameter(honorAE, true).getLengthHistory());

        assertEquals(11, first.getLengthHistory().resolveLength(JDBCType.VARCHAR, 10, 11));
        assertEquals(13, second.getLengthHistory().resolveLength(JDBCType.VARCHAR, 10, 13));
        assertEquals(13, parameter.getLengthHistory().resolveLength(JDBCType.VARCHAR, 10, 1));
        parameter.setValueLength(77);
        parameter.setDefineParameterTypeLengthHint(89);
        parameter.clearInputValue();
        parameter.resetOutputValue();
        assertSame(history, parameter.getLengthHistory());
        assertEquals(13, history.resolveLength(JDBCType.VARCHAR, 10, 0));
        assertEquals(77, parameter.getValueLength());
    }

    @ParameterizedTest
    @EnumSource(value = JDBCType.class, names = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"})
    public void testExactMonotonicGrowth(JDBCType type) {
        Parameter.LengthHistory history = new Parameter.LengthHistory();
        assertEquals(10, history.resolveLength(type, 10, 0));
        assertEquals(10, history.resolveLength(type, 10, 4));
        assertEquals(11, history.resolveLength(type, 10, 11));
        assertEquals(11, history.resolveLength(type, 10, 1));
        assertEquals(13, history.resolveLength(type, 10, 13));
        assertEquals(13, history.resolveLength(type, 10, 0));
        assertEquals(9001, history.resolveLength(type, 10, 9001));
        assertEquals(9001, history.resolveLength(type, 10, 1));
        assertEquals(Integer.MAX_VALUE, history.resolveLength(type, 10, Integer.MAX_VALUE));
    }

    @Test
    public void testFamiliesAreIndependentAndAliasesShareHistory() {
        Parameter.LengthHistory history = new Parameter.LengthHistory();
        assertEquals(101, history.resolveLength(JDBCType.CHAR, 10, 101));
        assertEquals(101, history.resolveLength(JDBCType.VARCHAR, 10, 0));
        assertEquals(10, history.resolveLength(JDBCType.NVARCHAR, 10, 0));
        assertEquals(203, history.resolveLength(JDBCType.NCHAR, 10, 203));
        assertEquals(203, history.resolveLength(JDBCType.NVARCHAR, 10, 1));
        assertEquals(10, history.resolveLength(JDBCType.VARBINARY, 10, 0));
        assertEquals(307, history.resolveLength(JDBCType.BINARY, 10, 307));
        assertEquals(307, history.resolveLength(JDBCType.VARBINARY, 10, 1));
        assertEquals(101, history.resolveLength(JDBCType.VARCHAR, 10, 0));
        assertEquals(203, history.resolveLength(JDBCType.NVARCHAR, 10, 0));
    }

    @Test
    public void testHistoryStoresObservationsNotConfiguration() {
        Parameter.LengthHistory history = new Parameter.LengthHistory();
        assertEquals(100, history.resolveLength(JDBCType.VARCHAR, 100, 13));
        assertEquals(13, history.resolveLength(JDBCType.VARCHAR, 10, 0));
    }

    @Test
    public void testInstancesDoNotShareObservations() {
        Parameter.LengthHistory first = new Parameter.LengthHistory();
        Parameter.LengthHistory second = new Parameter.LengthHistory();
        assertEquals(103, first.resolveLength(JDBCType.VARCHAR, 10, 103));
        assertEquals(10, second.resolveLength(JDBCType.VARCHAR, 10, 0));
    }

    @ParameterizedTest
    @EnumSource(value = JDBCType.class, names = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"})
    public void testDisabledFloorDoesNotRecordObservations(JDBCType type) {
        Parameter.LengthHistory history = new Parameter.LengthHistory();
        assertEquals(0, history.resolveLength(type, 0, 100));
        assertEquals(10, history.resolveLength(type, 10, 0));
        assertEquals(13, history.resolveLength(type, 10, 13));
        assertEquals(0, history.resolveLength(type, 0, 100));
        assertEquals(13, history.resolveLength(type, 10, 0));
    }

    @ParameterizedTest
    @EnumSource(value = JDBCType.class, mode = EnumSource.Mode.EXCLUDE,
            names = {"CHAR", "VARCHAR", "NCHAR", "NVARCHAR", "BINARY", "VARBINARY"})
    public void testOtherTypesHaveNoFloor(JDBCType type) {
        Parameter.LengthHistory history = new Parameter.LengthHistory();
        assertEquals(0, history.resolveLength(type, 10, 9001));
        assertEquals(10, history.resolveLength(JDBCType.VARCHAR, 10, 0));
        assertEquals(10, history.resolveLength(JDBCType.NVARCHAR, 10, 0));
        assertEquals(10, history.resolveLength(JDBCType.VARBINARY, 10, 0));
    }
}
