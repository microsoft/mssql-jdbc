/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Constants;
import com.microsoft.sqlserver.testframework.DBTable;


@RunWith(JUnitPlatform.class)
@Tag(Constants.xAzureSQLDW)
public class DBMetadataTest extends AbstractTest {

    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }

    @Test
    public void testDatabaseMetaData() throws SQLException {
        String functionName = RandomUtil.getIdentifier("proc");
        String escapedFunctionName = DBTable.escapeIdentifier(functionName);
        SQLServerDataSource ds = new SQLServerDataSource();
        ds.setURL(connectionString);

        String sqlDropFunction = "if exists (select * from dbo.sysobjects where id = object_id(N'[dbo]."
                + TestUtils.escapeSingleQuotes(escapedFunctionName) + "')" + "and xtype in (N'FN', N'IF', N'TF'))"
                + "drop function " + escapedFunctionName;
        String sqlCreateFunction = "CREATE  FUNCTION " + escapedFunctionName
                + " (@text varchar(8000), @delimiter varchar(20) = ' ') RETURNS @Strings TABLE "
                + "(position int IDENTITY PRIMARY KEY, value varchar(8000)) AS BEGIN INSERT INTO @Strings VALUES ('DDD') RETURN END ";

        try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {
            // drop function
            stmt.execute(sqlDropFunction);
            // create function
            stmt.execute(sqlCreateFunction);

            DatabaseMetaData md = con.getMetaData();
            try (ResultSet arguments = md.getProcedureColumns(null, null, functionName, "@TABLE_RETURN_VALUE")) {
                if (arguments.next()) {
                    assertTrue(arguments.getString("PROCEDURE_NAME").startsWith(functionName),
                            "Expected Stored Procedure was not retrieved.");
                } else {
                    assertTrue(false, "Expected Stored Procedure was not found.");
                }
                arguments.getStatement().close();
            }
            stmt.execute(sqlDropFunction);
        }
    }
}
