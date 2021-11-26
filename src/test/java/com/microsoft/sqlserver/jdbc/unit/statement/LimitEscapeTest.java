/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Vector;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;


/**
 * Testing with LimitEscape queries
 *
 */
@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class LimitEscapeTest extends AbstractTest {
    public static final Logger log = Logger.getLogger("LimitEscape");
    private static Vector<String> offsetQuery = new Vector<>();

    // TODO: remove quote for now to avoid bug in driver
    static String table1 = RandomUtil.getIdentifier("UnitStatement_LimitEscape_t1").replaceAll("\'", "");
    static String table2 = RandomUtil.getIdentifier("UnitStatement_LimitEscape_t2").replaceAll("\'", "");
    static String table3 = RandomUtil.getIdentifier("UnitStatement_LimitEscape_t3").replaceAll("\'", "");
    static String table4 = RandomUtil.getIdentifier("UnitStatement_LimitEscape_t4").replaceAll("\'", "");
    static String procName = RandomUtil.getIdentifier("UnitStatement_LimitEscape_p1").replaceAll("\'", "");

    static class Query {
        String inputSql, outputSql;
        int[] idCols = null;
        String[][] stringResultCols = null;
        int[][] intResultCols = null;
        int rows, columns;
        boolean prepared = false;
        boolean callable = false;
        int preparedCount = 0;
        boolean verifyResult = true;
        ResultSet resultSet = null;
        PreparedStatement pstmt = null;
        Statement stmt = null;
        int queryID;
        int queryId = 0;
        static int queryCount = 0;
        String exceptionMsg = null;

        /*
         * This is used to test different SQL queries. Each SQL query to test is an instance of this class, and is
         * initiated using this constructor. This constructor sets the expected results from the query and also verifies
         * the translation by comparing it with manual translation.
         * @param input The SQL query to test
         * @param output The manual translation of the query to verify with
         * @param rows The expected number of rows in ResultSet
         * @param columns The expected number of columns in ResultSet
         * @param ids The array of the expected id columns in the ResultSet
         * @param intCols The array of the expected int columns of each row in the ResultSet
         * @param stringCols The array of the expected String columns of each row in the ResultSet
         */
        Query(String input, String output, int rows, int columns, int[] ids, int[][] intCols,
                String[][] stringCols) throws Exception {
            queryCount++;

            queryID = queryCount;
            this.inputSql = input;
            this.outputSql = output;
            this.rows = rows;
            this.columns = columns;

            if (null != ids) {
                idCols = ids.clone();
            }
            if (null != intCols) {
                intResultCols = intCols.clone();
            }
            if (null != stringCols) {
                stringResultCols = stringCols.clone();
            }

            verifyTranslation();
        }

        public void setExceptionMsg(String errorMessage) {
            exceptionMsg = errorMessage;
        }

        public void verifyTranslation() throws Exception {
            Class<?>[] cArg = new Class<?>[1];
            cArg[0] = String.class;
            Class<?> innerClass = Class.forName("com.microsoft.sqlserver.jdbc.JDBCSyntaxTranslator");
            Constructor<?> ctor = innerClass.getDeclaredConstructor();

            ctor.setAccessible(true);
            Object innerInstance = ctor.newInstance();
            Method method = innerClass.getDeclaredMethod("translate", cArg);

            method.setAccessible(true);
            Object str = method.invoke(innerInstance, inputSql);
            assertEquals(str, outputSql, TestResource.getResource("R_syntaxMatchError") + ": " + queryID);
        }

        public void setverifyResult(boolean val) {
            this.verifyResult = val;
        }

        void executeSpecific(Connection conn) throws Exception {
            stmt = conn.createStatement();
            resultSet = stmt.executeQuery(inputSql);
        }

        void execute(Connection conn) throws Exception {
            try {
                executeSpecific(conn);
            } catch (Exception e) {
                if (null != exceptionMsg) {
                    // This query is to verify right exception is thrown for errors in syntax.
                    assertTrue(e.getMessage().equalsIgnoreCase(exceptionMsg),
                            TestResource.getResource("R_unexpectedExceptionContent") + e.getMessage());
                    // Exception message matched. Return as there is no result to verify.
                    return;
                } else
                    throw e;
            }

            if (!verifyResult) {
                return;
            }

            if (null == resultSet) {
                assertEquals(false, true, TestResource.getResource("R_resultsetNull"));
            }

            int rowCount = 0;
            while (resultSet.next()) {
                // The int and string columns should be retrieved in order, for example cannot run a query that
                // retrieves col2 but not col1
                assertEquals(resultSet.getInt(1), idCols[rowCount],
                        TestResource.getResource("R_valueNotMatch") + queryID + ", row: " + rowCount);
                for (int j = 0, colNumber = 1; null != intResultCols && j < intResultCols[rowCount].length; ++j) {
                    String colName = "col" + colNumber;
                    assertEquals(resultSet.getInt(colName), intResultCols[rowCount][j],
                            TestResource.getResource("R_valueNotMatch") + queryID + ", row: " + rowCount + ", column: "
                                    + colName);
                    colNumber++;
                }
                for (int j = 0, colNumber = 3; null != stringResultCols && j < stringResultCols[rowCount].length; ++j) {
                    String colName = "col" + colNumber;
                    assertEquals(resultSet.getString(colName), stringResultCols[rowCount][j],
                            TestResource.getResource("R_valueNotMatch") + queryID + ", row: " + rowCount + ", column: "
                                    + colName);
                    colNumber++;
                }
                rowCount++;
            }
            assertEquals(rowCount, rows,
                    TestResource.getResource("R_valueNotMatch") + "rowCount: " + rowCount + ", rows: " + rows);
            assertEquals(resultSet.getMetaData().getColumnCount(), columns, "Column Count does not match");
        }
    }

    static class PreparedQuery extends Query {
        int placeholderCount = 0;

        PreparedQuery(String input, String output, int rows, int columns, int[] ids, int[][] intCols,
                String[][] stringCols, int placeholderCount) throws Exception {
            super(input, output, rows, columns, ids, intCols, stringCols);
            this.placeholderCount = placeholderCount;
        }

        void executeSpecific(Connection conn) throws Exception {
            pstmt = conn.prepareStatement(inputSql);
            for (int i = 1; i <= placeholderCount; ++i) {
                pstmt.setObject(i, i);
            }
            resultSet = pstmt.executeQuery();
        }
    }

    static class CallableQuery extends PreparedQuery {
        CallableQuery(String input, String output, int rows, int columns, int[] ids, int[][] intCols,
                String[][] stringCols, int placeholderCount) throws Exception {
            super(input, output, rows, columns, ids, intCols, stringCols, placeholderCount);
        }

        void execute(Connection conn) throws Exception {
            try (CallableStatement cstmt = conn.prepareCall(inputSql)) {
                for (int i = 1; i <= placeholderCount; ++i) {
                    cstmt.setObject(i, i);
                }
                resultSet = cstmt.executeQuery();
            }
        }
    }

    public static void createAndPopulateTables(Connection conn) throws Exception {
        try (Statement stmt = conn.createStatement()) {
            // Instead of table identifiers use some simple table names for this test only, as a lot of string
            // manipulation
            // is done
            // around table names.
            try {
                stmt.executeUpdate("drop table " + AbstractSQLGenerator.escapeIdentifier(table1));
            } catch (Exception ex) {} ;
            try {
                stmt.executeUpdate("drop table " + AbstractSQLGenerator.escapeIdentifier(table2));
            } catch (Exception ex) {} ;
            try {
                stmt.executeUpdate("drop table " + AbstractSQLGenerator.escapeIdentifier(table3));
            } catch (Exception ex) {} ;
            try {
                stmt.executeUpdate("drop table " + AbstractSQLGenerator.escapeIdentifier(table4));
            } catch (Exception ex) {} ;
            try {
                stmt.executeUpdate("drop procedure " + AbstractSQLGenerator.escapeIdentifier(procName));
            } catch (Exception ex) {} ;
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(table1)
                    + " (col1 int, col2 int, col3 varchar(100), col4 varchar(100), id int identity(1,1) primary key)");
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(table2)
                    + " (col1 int, col2 int, col3 varchar(100), col4 varchar(100), id int identity(1,1) primary key)");
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " (col1 int, col2 int, col3 varchar(100), col4 varchar(100), id int identity(1,1) primary key)");
            stmt.executeUpdate("create table " + AbstractSQLGenerator.escapeIdentifier(table4)
                    + " (col1 int, col2 int, col3 varchar(100), col4 varchar(100), id int identity(1,1) primary key)");

            stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(table1) + " values "
                    + "(1, 1, 'col3', 'col4'), "
                    + "(2, 2, 'row2 '' with '' quote', 'row2 with limit  {limit 22} {limit ?}'),"
                    + "(3, 3, 'row3 with subquery (select * from t1)', 'row3 with subquery (select * from (select * from t1) {limit 4})'),"
                    + "(4, 4, 'select * from t1 {limit 4} ''quotes'' (braces)', 'ucase(scalar function)'),"
                    + "(5, 5, 'openquery(''server'', ''query'')', 'openrowset(''server'',''connection string'',''query'')')");
            stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(table2)
                    + " values (11, 11, 'col33', 'col44')");
            stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(table3)
                    + " values (111, 111, 'col333', 'col444')");
            stmt.executeUpdate("Insert into " + AbstractSQLGenerator.escapeIdentifier(table4)
                    + " values (1111, 1111, 'col4444', 'col4444')");
            String query = "create procedure " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " @col3Value varchar(512), @col4Value varchar(512) AS BEGIN SELECT TOP 1 * from "
                    + AbstractSQLGenerator.escapeIdentifier(table1)
                    + " where col3 = @col3Value and col4 = @col4Value END";
            stmt.execute(query);
        }
    }

    /**
     * Initialize and verify queries
     * 
     * @throws Exception
     */
    @Test
    public void initAndVerifyQueries() throws Exception {
        Query qry;
        try (Connection conn = getConnection()) {
            // 1
            // Test whether queries without limit syntax works
            qry = new Query("select TOP 1 * from " + AbstractSQLGenerator.escapeIdentifier(table1),
                    "select TOP 1 * from " + AbstractSQLGenerator.escapeIdentifier(table1), 1, // # of rows
                    5, // # of columns
                    new int[] {1}, // id column values
                    new int[][] {{1, 1}}, // int column values
                    new String[][] {{"col3", "col4"}}); // string column values
            qry.execute(conn);

            // 2
            // Test parentheses in limit syntax
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit ( (  (2)))}",
                    "select TOP ( (  (2))) * from " + AbstractSQLGenerator.escapeIdentifier(table1), 2, // # of rows
                    5, // # of columns
                    new int[] {1, 2}, // id column values
                    new int[][] {{1, 1}, {2, 2}}, // int column values
                    new String[][] {{"col3", "col4"},
                            {"row2 ' with ' quote", "row2 with limit  {limit 22} {limit ?}"}}); // string
                                                                                                // column
                                                                                                // values
            qry.execute(conn);

            // 3
            // Test limit syntax in string literal as well as in query, also test subquery syntax in string literal
            qry = new Query("select ( (col1)), ( ((col2) ) ) from " + AbstractSQLGenerator.escapeIdentifier(table1)
                    + " where col3 = 'row3 with subquery (select * from t1)' and col4 = 'row3 with subquery (select * from (select * from t1) {limit 4})' {limit (35)}",
                    "select TOP (35) ( (col1)), ( ((col2) ) ) from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " where col3 = 'row3 with subquery (select * from t1)' and col4 = 'row3 with subquery (select * from (select * from t1) {limit 4})'",
                    1, // # of rows
                    2, // # of columns
                    new int[] {3}, // id column values
                    new int[][] {{3, 3}}, // int column values
                    null); // string column values
            qry.execute(conn);

            // 4
            // Test quotes/limit syntax/scalar function in string literal. Also test real limit syntax in query.
            qry = new Query("select (col1), (col2) from " + AbstractSQLGenerator.escapeIdentifier(table1)
                    + " where col3 = 'select * from t1 {limit 4} ''quotes'' (braces)' and col4 = 'ucase(scalar function)' {limit 3543}",
                    "select TOP 3543 (col1), (col2) from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " where col3 = 'select * from t1 {limit 4} ''quotes'' (braces)' and col4 = 'ucase(scalar function)'",
                    1, // # of rows
                    2, // # of columns
                    new int[] {4}, // id column values
                    new int[][] {{4, 4}}, // int column values
                    null); // string column values
            qry.execute(conn);

            // 5
            // Test openquery/openrowset in string literals
            qry = new Query("select col1 from " + AbstractSQLGenerator.escapeIdentifier(table1)
                    + " where col3 = 'openquery(''server'', ''query'')' and col4 = 'openrowset(''server'',''connection string'',''query'')' {limit (((2)))}",
                    "select TOP (((2))) col1 from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " where col3 = 'openquery(''server'', ''query'')' and col4 = 'openrowset(''server'',''connection string'',''query'')'",
                    1, // # of rows
                    1, // # of columns
                    new int[] {5}, // id column values
                    new int[][] {{5}}, // int column values
                    null); // string column values
            qry.execute(conn);

            // 6
            // Test limit syntax in subquery as well as in outer query
            qry = new Query(
                    "select id from (select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 10}) t1 {limit ((1) )}",
                    "select TOP ((1) ) id from (select TOP 10 * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + ") t1",
                    1, // # of rows
                    1, // # of columns
                    new int[] {1}, // id column values
                    null, // int column values
                    null); // string column values
            qry.execute(conn);

            // 7
            // Test multiple parentheses in limit syntax and in subquery
            qry = new Query(
                    "select id from (( (select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 10})) ) t1 {limit ((1) )}",
                    "select TOP ((1) ) id from (( (select TOP 10 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1) + ")) ) t1",
                    1, // # of
                       // rows
                    1, // # of columns
                    new int[] {1}, // id column values
                    null, // int column values
                    null); // string column values
            qry.execute(conn);

            // 8
            // Test limit syntax in multiple subqueries, also test arbitrary spaces in limit syntax
            qry = new Query(
                    "select j1.id from (( (select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 10})) ) j1 join (select * from " + AbstractSQLGenerator.escapeIdentifier(table2)
                            + " {limit 4}) j2 on j1.id = j2.id {limit  	(1)}",
                    "select TOP (1) j1.id from (( (select TOP 10 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1) + ")) ) j1 join (select TOP 4 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table2) + ") j2 on j1.id = j2.id",
                    1, // # of rows
                    1, // # of columns
                    new int[] {1}, // id column values
                    null, // int column values
                    null); // string column values
            qry.execute(conn);

            // 9
            // Test limit syntax in multiple levels of nested subqueries
            qry = new Query(
                    "select j1.id from (select * from (select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 3}) j3 {limit 2}) j1 join (select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table2)
                            + " {limit 4}) j2 on j1.id = j2.id {limit 1}",
                    "select TOP 1 j1.id from (select TOP 2 * from (select TOP 3 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1) + ") j3) j1 join (select TOP 4 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table2) + ") j2 on j1.id = j2.id",
                    1, // # of rows
                    1, // # of columns
                    new int[] {1}, // id column values
                    null, // int column values
                    null); // string column values
            qry.execute(conn);

            // 10
            // Test limit syntax in multiple levels of nested subqueries as well as in outer query
            qry = new Query(
                    "select j1.id from (select * from (select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 3}) j3 {limit 2}) j1 join (select j4.id from (select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table3) + " {limit 5}) j4 join (select * from  "
                            + AbstractSQLGenerator.escapeIdentifier(table4)
                            + " {limit 6}) j5 on j4.id = j5.id ) j2 on j1.id = j2.id {limit 1}",
                    "select TOP 1 j1.id from (select TOP 2 * from (select TOP 3 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1)
                            + ") j3) j1 join (select j4.id from (select TOP 5 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table3) + ") j4 join (select TOP 6 * from  "
                            + AbstractSQLGenerator.escapeIdentifier(table4)
                            + ") j5 on j4.id = j5.id ) j2 on j1.id = j2.id",
                    1, // # of rows
                    1, // # of columns
                    new int[] {1}, // id column values
                    null, // int column values
                    null); // string column values
            qry.execute(conn);

            // 11
            // Test multiple parentheses/spaces in limit syntax, also test '[]' in columns
            qry = new Query(
                    "select [col1], col2, [col3], col4 from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit ( (  (2)))}",
                    "select TOP ( (  (2))) [col1], col2, [col3], col4 from "
                            + AbstractSQLGenerator.escapeIdentifier(table1),
                    2, // # of rows
                    4, // # of columns
                    new int[] {1, 2}, // id column values
                    new int[][] {{1, 1}, {2, 2}}, // int column values
                    new String[][] {{"col3", "col4"},
                            {"row2 ' with ' quote", "row2 with limit  {limit 22} {limit ?}"}}); // string
                                                                                                // column
                                                                                                // values
            qry.execute(conn);

            // 12
            // Test complicated query with nested subquery having limit syntax
            qry = new Query(
                    "select j1.id from ( ((select * from (select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 3}) j3 {limit 2}))) j1 join (select j4.id from ((((select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table3) + " {limit 5})))) j4 join (select * from  "
                            + AbstractSQLGenerator.escapeIdentifier(table4)
                            + " {limit 6}) j5 on j4.id = j5.id ) j2 on j1.id = j2.id {limit 1}",
                    "select TOP 1 j1.id from ( ((select TOP 2 * from (select TOP 3 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1)
                            + ") j3))) j1 join (select j4.id from ((((select TOP 5 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table3) + ")))) j4 join (select TOP 6 * from  "
                            + AbstractSQLGenerator.escapeIdentifier(table4)
                            + ") j5 on j4.id = j5.id ) j2 on j1.id = j2.id",
                    1, // # of rows
                    1, // # of columns
                    new int[] {1}, // id column values
                    null, // int column values
                    null); // string column values
            qry.execute(conn);

            // 13
            // Test prepared statements with limit syntax with multiple parentheses/spaces
            qry = new PreparedQuery(
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit ( (  (?)))}",
                    "select TOP ( (  (?))) * from " + AbstractSQLGenerator.escapeIdentifier(table1), 1, // # of rows
                    5, // # of columns
                    new int[] {1}, // id column values
                    new int[][] {{1, 1}}, // int column values
                    new String[][] {{"col3", "col4"}}, 1);
            qry.execute(conn);

            // 14
            // Test prepared statements with limit syntax
            qry = new PreparedQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit (?)}",
                    "select TOP (?) * from " + AbstractSQLGenerator.escapeIdentifier(table1), 1, // #
                    // of
                    // rows
                    5, // # of columns
                    new int[] {1}, // id column values
                    new int[][] {{1, 1}}, // int column values
                    new String[][] {{"col3", "col4"}}, 1);
            qry.execute(conn);

            // 15
            // Test prepared statements with limit syntax with multiple parentheses/spaces
            qry = new PreparedQuery("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit ?}",
                    "select TOP (?) * from " + AbstractSQLGenerator.escapeIdentifier(table1), 1, // #
                    // of
                    // rows
                    5, // # of columns
                    new int[] {1}, // id column values
                    new int[][] {{1, 1}}, // int column values
                    new String[][] {{"col3", "col4"}}, 1);
            qry.execute(conn);

            // 16
            // Test prepared statements with limit syntax with subqueries
            qry = new PreparedQuery(
                    "select * from (select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit ?}) t1 {limit (?)}",
                    "select TOP (?) * from (select TOP (?) * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + ") t1",
                    1, // # of rows
                    5, // # of columns
                    new int[] {1}, // id column values
                    new int[][] {{1, 1}}, // int column values
                    new String[][] {{"col3", "col4"}}, 2);
            qry.execute(conn);

            // 17
            // Test callable statements as they are also translated by the driver
            qry = new CallableQuery(
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " @col3Value = 'col3', @col4Value = 'col4'",
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " @col3Value = 'col3', @col4Value = 'col4'",
                    1, // # of rows
                    5, // # of columns
                    new int[] {1}, // id column values
                    new int[][] {{1, 1}}, // int column values
                    new String[][] {{"col3", "col4"}}, 0);
            qry.execute(conn);

            // 18
            // Test callable statements with limit syntax in string literals
            qry = new CallableQuery("EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " @col3Value = 'row2 '' with '' quote', @col4Value = 'row2 with limit  {limit 22} {limit ?}'",
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " @col3Value = 'row2 '' with '' quote', @col4Value = 'row2 with limit  {limit 22} {limit ?}'",
                    1, // #
                       // of
                       // rows
                    5, // # of columns
                    new int[] {2}, // id column values
                    new int[][] {{2, 2}}, // int column values
                    new String[][] {{"row2 ' with ' quote", "row2 with limit  {limit 22} {limit ?}"}}, 0);
            qry.execute(conn);

            // 19
            // Test callable statements with subquery/limit syntax in string literals
            qry = new CallableQuery("EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " @col3Value = 'row3 with subquery (select * from t1)', @col4Value = 'row3 with subquery (select * from (select * from t1) {limit 4})'",
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " @col3Value = 'row3 with subquery (select * from t1)', @col4Value = 'row3 with subquery (select * from (select * from t1) {limit 4})'",
                    1, // # of rows
                    5, // # of columns
                    new int[] {3}, // id column values
                    new int[][] {{3, 3}}, // int column values
                    new String[][] {{"row3 with subquery (select * from t1)",
                            "row3 with subquery (select * from (select * from t1) {limit 4})"}},
                    0);
            qry.execute(conn);

            // 20
            // Test callable statements with quotes/scalar functions/limit syntax in string literals
            qry = new CallableQuery("EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " @col3Value = 'select * from t1 {limit 4} ''quotes'' (braces)', @col4Value = 'ucase(scalar function)'",
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " @col3Value = 'select * from t1 {limit 4} ''quotes'' (braces)', @col4Value = 'ucase(scalar function)'",
                    1, // # of rows
                    5, // # of columns
                    new int[] {4}, // id column values
                    new int[][] {{4, 4}}, // int column value
                    new String[][] {{"select * from t1 {limit 4} 'quotes' (braces)", "ucase(scalar function)"}}, 0);
            qry.execute(conn);

            // 21
            // Test callable statement escape syntax with quotes/scalar functions/limit syntax in string literals
            qry = new CallableQuery(
                    "{call " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " ('select * from t1 {limit 4} ''quotes'' (braces)', 'ucase(scalar function)')}",
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " 'select * from t1 {limit 4} ''quotes'' (braces)', 'ucase(scalar function)'",
                    1, // #
                       // of
                       // rows
                    5, // # of columns
                    new int[] {4}, // id column values
                    new int[][] {{4, 4}}, // int column value
                    new String[][] {{"select * from t1 {limit 4} 'quotes' (braces)", "ucase(scalar function)"}}, 0);
            qry.execute(conn);

            // 22
            // Test callable statement escape syntax with openrowquery/openrowset/quotes in string literals
            qry = new CallableQuery("{call " + AbstractSQLGenerator.escapeIdentifier(procName)
                    + " ('openquery(''server'', ''query'')', 'openrowset(''server'',''connection string'',''query'')')}",
                    "EXEC " + AbstractSQLGenerator.escapeIdentifier(procName)
                            + " 'openquery(''server'', ''query'')', 'openrowset(''server'',''connection string'',''query'')'",
                    1, // #
                       // of
                       // rows
                    5, // # of columns
                    new int[] {5}, // id column values
                    new int[][] {{5, 5}}, // int column value
                    new String[][] {
                            {"openquery('server', 'query')", "openrowset('server','connection string','query')"}},
                    0);
            qry.execute(conn);

            // Do not execute this query as no lnked_server is setup. Only verify the translation for it.
            // 23
            // Test openquery syntax translation with limit syntax
            qry = new Query(
                    "select * from openquery('linked_server', 'select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 2}') {limit 1}",
                    "select TOP 1 * from openquery('linked_server', 'select TOP 2 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1) + "')",
                    1, // #
                       // of
                       // rows
                    5, // # of columns
                    new int[] {5}, // id column values
                    new int[][] {{5, 5}}, // int column value
                    new String[][] {
                            {"openquery('server', 'query')", "openrowset('server','connection string','query')"}});

            // Do not execute this query as no lnked_server is setup. Only verify the translation for it.
            // 24
            // Test openrowset syntax translation with a complicated query with subqueries and limit syntax
            qry = new Query(
                    "select * from openrowset('provider_name', 'provider_string', 'select j1.id from (select * from (select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " {limit 3}) j3 {limit 2}) j1 join (select j4.id from (select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table3) + " {limit 5}) j4 join (select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table4)
                            + " {limit 6}) j5 on j4.id = j5.id ) j2 on j1.id = j2.id {limit 1}') {limit 1}",
                    "select TOP 1 * from openrowset('provider_name', 'provider_string', 'select TOP 1 j1.id from (select TOP 2 * from (select TOP 3 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1)
                            + ") j3) j1 join (select j4.id from (select TOP 5 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table3) + ") j4 join (select TOP 6 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table4)
                            + ") j5 on j4.id = j5.id ) j2 on j1.id = j2.id')",
                    1, // # of rows
                    5, // # of columns
                    new int[] {5}, // id column values
                    new int[][] {{5, 5}}, // int column value
                    new String[][] {
                            {"openquery('server', 'query')", "openrowset('server','connection string','query')"}});

            // 25
            // Test offset syntax in string literals
            qry = new Query(
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " where col3 = '{limit 1 offset 2}'",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1)
                            + " where col3 = '{limit 1 offset 2}'",
                    0, // # of rows
                    5, // # of columns
                    null, // id column values
                    null, // int column values
                    null);
            qry.execute(conn);

            // 26
            // Do not execute this query as it is a batch query, needs to be handled differently.
            // Only test the syntax translation.
            // Test batch query.
            qry = new Query(
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1}; select * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 4}",
                    "select TOP 1 * from " + AbstractSQLGenerator.escapeIdentifier(table1) + "; select TOP 4 * from "
                            + AbstractSQLGenerator.escapeIdentifier(table1),
                    0, // #
                       // of
                       // rows
                    5, // # of columns
                    null, // id column values
                    null, // int column values
                    null);

            // 27
            // Execute query, and verify exception for unclosed quotation marks.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " where col3 = 'abcd",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " where col3 = 'abcd", 0, // # of
                                                                                                                 // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Unclosed quotation mark after the character string 'abcd'.");
            qry.execute(conn);

            // 28
            // Execute query, and verify exception for unclosed subquery.
            qry = new Query(
                    "select * from (select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1}",
                    "select * from (select TOP 1 * from " + AbstractSQLGenerator.escapeIdentifier(table1), 0, // # of
                                                                                                              // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '" + table1 + "'.");
            qry.execute(conn);

            // 29
            // Execute query, and verify exception for syntax error in select.
            qry = new Query("selectsel * from from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1}",
                    "selectsel * from from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1}", 0, // # of
                                                                                                                // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '*'.");
            qry.execute(conn);

            // 29
            // Execute query, and verify exception for limit syntax error. The translator should leave the query
            // unchanged
            // as limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit1}",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit1}", 0, // # of rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            // 30
            // Execute query, and verify exception for limit syntax error. The translator should leave the query
            // unchanged
            // as limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit(1}",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit(1}", 0, // # of
                    // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            // 31
            // Execute query, and verify exception for limit syntax error. The translator should leave the query
            // unchanged
            // as limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1 offset10}",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1 offset10}", 0, // # of
                                                                                                                 // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            // 32
            // Execute query, and verify exception for limit syntax error. The translator should leave the query
            // unchanged
            // as limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit1 offset 10}",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit1 offset 10}", 0, // # of
                                                                                                                 // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            // 33
            // Execute query, and verify exception for limit syntax error. The translator should leave the query
            // unchanged
            // as limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit1 offset10}",
                    "select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit1 offset10}", 0, // # of
                                                                                                                // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            // 34
            // Execute query, and verify exception for syntax error. The translator should leave the query unchanged as
            // limit syntax is not correct.
            qry = new Query("insert into " + AbstractSQLGenerator.escapeIdentifier(table1) + "(col3) values({limit 1})",
                    "insert into " + AbstractSQLGenerator.escapeIdentifier(table1) + "(col3) values({limit 1})", 0, // #
                                                                                                                    // of
                                                                                                                    // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '1'.");
            qry.execute(conn);

            // 35
            // Execute query, and verify exception for syntax error. The translator should leave the query unchanged as
            // limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit {limit 5}}",
                    "select TOP 5 * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit}", 0, // #
                    // of
                    // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            // 36
            // Execute query, and verify exception for syntax error. The translator should leave the query unchanged as
            // limit syntax is not correct.
            qry = new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 1} {limit 2}",
                    "select TOP 1 * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit 2}", 0, // # of
                                                                                                              // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
            // Verified that SQL Server throws an exception with this message for similar errors.
            qry.setExceptionMsg("Incorrect syntax near '{'.");
            qry.execute(conn);

            log.fine("Tranlsation verified for " + Query.queryCount + " queries");
        }
    }

    /**
     * Verify offset Exception
     * 
     * @throws Exception
     */
    @Test
    public void verifyOffsetException() throws Exception {
        offsetQuery.addElement("select * from "
                + AbstractSQLGenerator.escapeIdentifier(TestUtils.escapeSingleQuotes(table1)) + " {limit 2 offset 1}");
        offsetQuery.addElement(
                "select * from " + AbstractSQLGenerator.escapeIdentifier(TestUtils.escapeSingleQuotes(table1))
                        + " {limit 2232 offset 1232}");
        offsetQuery.addElement(
                "select * from " + AbstractSQLGenerator.escapeIdentifier(TestUtils.escapeSingleQuotes(table1))
                        + " {limit (2) offset (1)}");
        offsetQuery.addElement(
                "select * from " + AbstractSQLGenerator.escapeIdentifier(TestUtils.escapeSingleQuotes(table1))
                        + " {limit (265) offset (1972)}");
        offsetQuery.addElement("select * from "
                + AbstractSQLGenerator.escapeIdentifier(TestUtils.escapeSingleQuotes(table1)) + " {limit ? offset ?}");
        offsetQuery.addElement(
                "select * from " + AbstractSQLGenerator.escapeIdentifier(TestUtils.escapeSingleQuotes(table1))
                        + " {limit (?) offset (?)}");

        int i;
        for (i = 0; i < offsetQuery.size(); ++i) {
            try {
                // Do not execute query. Exception will be thrown when verifying translation.
                new Query(offsetQuery.elementAt(i), "", 0, // # of rows
                        0, // # of columns
                        null, // id column values
                        null, // int column values
                        null); // string column values
            }
            // Exception was thrown from Java reflection method invocation
            catch (InvocationTargetException e) {
                assertEquals(e.toString(), "java.lang.reflect.InvocationTargetException");
            }
        }
        log.fine("Offset exception verified for " + i + " queries");
        // Test the parsing error with unmatched braces in limit clause
        try {
            // Do not execute query. Exception will be thrown when verifying translation.
            new Query("select * from " + AbstractSQLGenerator.escapeIdentifier(table1) + " {limit (2))}", "", 0, // # of
                                                                                                                 // rows
                    0, // # of columns
                    null, // id column values
                    null, // int column values
                    null); // string column values
        }
        // Exception was thrown from Java reflection method invocation
        catch (InvocationTargetException e) {
            assertEquals(e.toString(), "java.lang.reflect.InvocationTargetException");
        }
    }

    /**
     * clean up
     */
    @BeforeAll
    public static void beforeAll() throws Exception {
        connectionString = TestUtils.addOrOverrideProperty(connectionString,"trustServerCertificate", "true");
        setConnection();

        try {
            createAndPopulateTables(connection);
        } catch (Exception e) {
            fail(e.getMessage());
        }
    }

    /**
     * Clean up
     * 
     * @throws Exception
     */
    @AfterAll
    public static void afterAll() throws SQLException {
        try (Statement stmt = connection.createStatement()) {
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table1), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table2), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table3), stmt);
            TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(table4), stmt);
            TestUtils.dropProcedureIfExists(AbstractSQLGenerator.escapeIdentifier(procName), stmt);
        }
    }
}
