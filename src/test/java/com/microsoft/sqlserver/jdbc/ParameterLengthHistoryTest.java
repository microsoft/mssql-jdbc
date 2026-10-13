/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;


@Tag("Unit")
public class ParameterLengthHistoryTest {
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
