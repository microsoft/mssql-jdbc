/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.jdbc.TestResource;
import com.microsoft.sqlserver.jdbc.TestUtils;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.DBConnection;
import com.microsoft.sqlserver.testframework.DBStatement;


/**
 * Testing merge queries
 */
@RunWith(JUnitPlatform.class)
public class MergeTest extends AbstractTest {
    static String cricketTeams = RandomUtil.getIdentifier("CricketTeams");
    static String cricketTeamsUpdated = RandomUtil.getIdentifier("cricketTeamsUpdated");

    private static final String setupTables = "IF OBJECT_ID (N'" + TestUtils.escapeSingleQuotes(cricketTeams)
            + "', N'U') IS NOT NULL DROP TABLE " + AbstractSQLGenerator.escapeIdentifier(cricketTeams) + ";"
            + "   CREATE TABLE " + AbstractSQLGenerator.escapeIdentifier(cricketTeams)
            + "   (       CricketTeamID tinyint NOT NULL PRIMARY KEY,     CricketTeamCountry nvarchar(30),        CricketTeamContinent nvarchar(50))"
            + "   INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(cricketTeams)
            + " VALUES      (1, 'Australia', 'Australia'),      (2, 'India', 'Asia'),       (3, 'Pakistan', 'Asia'),        (4, 'Srilanka', 'Asia'),        (5, 'Bangaladesh', 'Asia'),     (6, 'HongKong', 'Asia'),"
            + "     (7, 'U.A.E', 'Asia'),      (8, 'England', 'Europe'),       (9, 'South Africa', 'Africa'),      (10, 'West Indies', 'North America');"
            + "   SELECT * FROM " + AbstractSQLGenerator.escapeIdentifier(cricketTeams) + "  IF OBJECT_ID (N'"
            + TestUtils.escapeSingleQuotes(cricketTeams) + "_UpdatedList', N'U') IS NOT NULL        DROP TABLE "
            + AbstractSQLGenerator.escapeIdentifier(cricketTeamsUpdated) + ";" + "   CREATE TABLE "
            + AbstractSQLGenerator.escapeIdentifier(cricketTeamsUpdated)
            + "   (       CricketTeamID tinyint NOT NULL PRIMARY KEY,     CricketTeamCountry nvarchar(30),        CricketTeamContinent nvarchar(50))"
            + "INSERT INTO " + AbstractSQLGenerator.escapeIdentifier(cricketTeamsUpdated)
            + " VALUES  (1, 'Australia', 'Australia'),     (2, 'India', 'Asia'),       (3, 'Pakistan', 'Asia'),     (4, 'Srilanka', 'Asia'),   (5, 'Bangaladesh', 'Asia'),"
            + " (6, 'Thailand', 'Asia'),      (8, 'England', 'Europe'),       (9, 'South Africa', 'Africa'),      (10, 'West Indies', 'North America'),       (11, 'Zimbabwe', 'Africa');";

    private static final String mergeCmd2 = "MERGE " + AbstractSQLGenerator.escapeIdentifier(cricketTeams)
            + " AS TARGET " + "USING " + AbstractSQLGenerator.escapeIdentifier(cricketTeamsUpdated) + " AS SOURCE "
            + "ON (TARGET.CricketTeamID = SOURCE.CricketTeamID) "
            + "WHEN MATCHED AND TARGET.CricketTeamContinent <> SOURCE.CricketTeamContinent OR "
            + "TARGET.CricketTeamCountry <> SOURCE.CricketTeamCountry "
            + "THEN UPDATE SET TARGET.CricketTeamContinent = SOURCE.CricketTeamContinent ,"
            + "TARGET.CricketTeamCountry = SOURCE.CricketTeamCountry " + "WHEN NOT MATCHED THEN "
            + "INSERT (CricketTeamID, CricketTeamCountry, CricketTeamContinent) "
            + "VALUES (SOURCE.CricketTeamID, SOURCE.CricketTeamCountry, SOURCE.CricketTeamContinent) "
            + "WHEN NOT MATCHED BY SOURCE THEN                                                    DELETE;";

    /**
     * Merge test
     * 
     * @throws Exception
     */
    @Test
    @DisplayName("Merge Test")
    public void runTest() throws Exception {
        try (DBConnection conn = new DBConnection(connectionString)) {
            if (conn.getServerVersion() >= 10) {
                try (DBStatement stmt = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY,
                        ResultSet.CONCUR_UPDATABLE);) {
                    stmt.executeUpdate(setupTables);
                    stmt.executeUpdate(mergeCmd2);
                    int updateCount = stmt.getUpdateCount();
                    assertEquals(updateCount, 3, TestResource.getResource("R_incorrectUpdateCount"));

                }
            }
        }
    }

    /**
     * Clean up
     * 
     * @throws Exception
     */
    @AfterAll
    public static void afterAll() throws Exception {

        try (Connection con = DriverManager.getConnection(connectionString); Statement stmt = con.createStatement()) {
            try {
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(cricketTeams), stmt);
                TestUtils.dropTableIfExists(AbstractSQLGenerator.escapeIdentifier(cricketTeamsUpdated), stmt);
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }
}
