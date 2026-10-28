/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.CALLS_REAL_METHODS;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullAndEmptySource;
import org.junit.jupiter.params.provider.ValueSource;


class TemporaryTableDetectionTest {
    @ParameterizedTest
    @ValueSource(strings = {"CREATE TABLE #t (v int)", "CREATE/* comment */TABLE #t (v int)",
            "CREATE TABLE/* outer /* inner */ end */#t (v int)", "CREATE -- comment\n TABLE #t (v int)",
            "CREATE TABLE tempdb..#t (v int)", "CREATE TABLE tempdb.dbo.#t (v int)",
            "CREATE TABLE [tempdb] . /* schema */ . [#t] (v int)", "CREATE TABLE \"tempdb\"..\"#t\" (v int)",
            "CrEaTe TaBlE tempdb..#t (v int)", "CREATE TABLE [#t]]escaped] (v int)", "SELECT ? AS v INTO #t",
            "SELECT ? AS v INTO /* comment */ tempdb..#t", "SELECT ';' AS v INTO [tempdb]..[#t]",
            "SELECT [column;name] INTO \"#t\" FROM source", "SELECT 'not INTO #ignored' AS v INTO #t",
            "/* CREATE TABLE regular */ CREATE TABLE #t (v int)"})
    void detectsLocalTempTableCreation(String sql) {
        SQLServerPreparedStatement stmt = mock(SQLServerPreparedStatement.class, CALLS_REAL_METHODS);
        assertTrue(stmt.containsTemporaryTableOperations(sql), sql);
    }

    @ParameterizedTest
    @NullAndEmptySource
    @ValueSource(strings = {"CREATE TABLE regular (v int)", "CREATE TABLE ##t (v int)",
            "CREATE TABLE tempdb..##t (v int)", "CREATE TABLE [tempdb]..[##t] (v int)",
            "CREATE TABLE [#database].dbo.regular (v int)", "SELECT ? INTO \"##t\"", "INSERT INTO #t VALUES (?)",
            "SELECT * FROM #t", "-- CREATE TABLE #t\nSELECT ?", "/* outer /* CREATE TABLE #t */ comment */ SELECT ?",
            "SELECT 'CREATE TABLE #t (v int)'", "SELECT 'INTO #t'", "SELECT [INTO #t] FROM source",
            "SELECT \"CREATE TABLE #t\" FROM source", "CREATE TABLE [name CREATE TABLE #t] (v int)",
            "SELECT [a]]; INTO #t] FROM source", "SELECT 'escaped''; INTO #t'", "SELECT 1; INSERT INTO #t VALUES (?)",
            "SELECT 1 INSERT INTO #t VALUES (?)", "WITH cte AS (SELECT 1 AS v) INSERT INTO #t SELECT v FROM cte",
            "SELECT @into #t", "SELECT 1 /* unterminated CREATE TABLE #t"})
    void ignoresNonCreationText(String sql) {
        SQLServerPreparedStatement stmt = mock(SQLServerPreparedStatement.class, CALLS_REAL_METHODS);
        assertFalse(stmt.containsTemporaryTableOperations(sql), sql);
    }
}
