/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;

import java.util.Arrays;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;


/**
 * Verifies which parameter markers direct execution treats as EXEC arguments, whose literals cannot be CAST
 * expressions. T-SQL statements need not be separated by semicolons, so an EXEC argument list ends at the first token
 * that cannot continue it.
 */
@RunWith(JUnitPlatform.class)
public class SQLServerPreparedStatementProcedureArgumentTest {

    private static Stream<Arguments> statements() {
        return Stream.of(
                // Statement boundaries without semicolons.
                Arguments.of("EXEC p ?", new boolean[] {true}),
                Arguments.of("SET NOCOUNT ON\nEXEC p ?", new boolean[] {true}),
                Arguments.of("SET NOCOUNT ON EXEC p ?", new boolean[] {true}),
                Arguments.of("BEGIN EXEC p ?; END", new boolean[] {true}),
                Arguments.of("BEGIN EXEC p ? END", new boolean[] {true}),
                Arguments.of("IF ? = 1 EXEC p ? ELSE SELECT ?", new boolean[] {false, true, false}),
                Arguments.of("EXEC p ?\nSELECT ? + 1", new boolean[] {true, false}),
                Arguments.of("EXEC p ? SELECT ? + 1", new boolean[] {true, false}),
                Arguments.of("EXEC p\nSELECT ? + 1", new boolean[] {false}),
                Arguments.of("EXEC p 1\nSELECT ? + 1", new boolean[] {false}),
                Arguments.of("EXEC p ? OUTPUT\nSELECT ?", new boolean[] {true, false}),
                Arguments.of("EXEC p ?\nEXEC q ?", new boolean[] {true, true}),
                Arguments.of("EXEC p\nEXEC q ?", new boolean[] {true}),
                Arguments.of("SELECT ?; EXEC p ?; SELECT ?", new boolean[] {false, true, false}),
                Arguments.of("INSERT INTO t EXEC p ?\nSELECT ?", new boolean[] {true, false}),

                // Argument lists.
                Arguments.of("EXECUTE dbo.p ?, ?, ?", new boolean[] {true, true, true}),
                Arguments.of("exec [db]..[p] ?, -?", new boolean[] {true, true}),
                Arguments.of("EXEC p @a = ?, @b = ? OUT, @c = @v, @d = DEFAULT, ?", new boolean[] {true, true, true}),
                Arguments.of("EXEC p N'x', 'y''z', 1.5e-3, 0x0A, $5, @v OUTPUT, ?", new boolean[] {true}),
                Arguments.of("EXEC @rc = p ?\nSELECT @rc + ?", new boolean[] {true, false}),
                Arguments.of("EXEC @proc ?, ?", new boolean[] {true, true}),
                Arguments.of("EXEC ? = p ?, ?", new boolean[] {false, true, true}),
                Arguments.of("EXEC p ?, @v ?", new boolean[] {true, false}),
                Arguments.of("EXEC p ? WITH RECOMPILE\nSELECT ?", new boolean[] {true, false}),
                Arguments.of("EXEC (?)", new boolean[] {true}),
                Arguments.of("EXEC (N'SELECT ' + ?)\nSELECT ?", new boolean[] {true, false}),

                // Quoted text, identifiers and comments.
                Arguments.of("SELECT 'EXEC p', ?", new boolean[] {false}),
                Arguments.of("SELECT [EXEC] FROM t WHERE c = ?", new boolean[] {false}),
                Arguments.of("SELECT exec_count FROM t WHERE c = ?", new boolean[] {false}),
                Arguments.of("/* EXEC p */ SELECT ?", new boolean[] {false}),
                Arguments.of("-- EXEC p\nSELECT ?", new boolean[] {false}),
                Arguments.of("EXEC p /* , */ ? -- , \n, ?", new boolean[] {true, true}),
                Arguments.of("EXEC p ? /* ; */ , ?\nSELECT ?", new boolean[] {true, true, false}),
                Arguments.of("EXEC p 'a;b', ?", new boolean[] {true}),
                Arguments.of("EXEC [p ; q] ?", new boolean[] {true}),
                Arguments.of("EXECUTE AS USER = 'u'; SELECT ?", new boolean[] {false}),
                Arguments.of("SELECT ?", new boolean[] {false}));
    }

    @ParameterizedTest(name = "{0}")
    @MethodSource("statements")
    public void testFindProcedureArguments(String sql, boolean[] expected) {
        int[] positions = markerPositions(sql);
        assertArrayEquals(expected, SQLServerPreparedStatement.findProcedureArguments(sql, positions),
                () -> sql + " -> " + Arrays.toString(expected));
    }

    /** Finds the parameter markers with the scanner the driver uses to parse the SQL text. */
    private static int[] markerPositions(String sql) {
        int[] positions = new int[sql.length()];
        int count = 0;
        int offset = 0;
        while ((offset = ParameterUtils.scanSQLForChar('?', sql, offset)) < sql.length()) {
            positions[count++] = offset++;
        }
        return Arrays.copyOf(positions, count);
    }
}
