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
    static String cricketTeams = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("CricketTeams"));
    static String cricketTeamsUpdated = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("cricketTeamsUpdated"));

    private static final String setupTables = "IF OBJECT_ID (N'" + cricketTeams + "', N'U') IS NOT NULL DROP TABLE " + cricketTeams + ";"
            + "   CREATE TABLE " + cricketTeams + "   (       CricketTeamID tinyint NOT NULL PRIMARY KEY,     CricketTeamCountry nvarchar(30),        CricketTeamContinent nvarchar(50))"
            + "   INSERT INTO " + cricketTeams + " VALUES      (1, 'Australia', 'Australia'),      (2, 'India', 'Asia'),       (3, 'Pakistan', 'Asia'),        (4, 'Srilanka', 'Asia'),        (5, 'Bangaladesh', 'Asia'),     (6, 'HongKong', 'Asia'),"
            + "     (7, 'U.A.E', 'Asia'),      (8, 'England', 'Europe'),       (9, 'South Africa', 'Africa'),      (10, 'West Indies', 'North America');"
            + "   SELECT * FROM " + cricketTeams + "  IF OBJECT_ID (N'" + cricketTeams + "_UpdatedList', N'U') IS NOT NULL        DROP TABLE " + cricketTeamsUpdated + ";"
            + "   CREATE TABLE " + cricketTeamsUpdated + "   (       CricketTeamID tinyint NOT NULL PRIMARY KEY,     CricketTeamCountry nvarchar(30),        CricketTeamContinent nvarchar(50))"
            + "INSERT INTO " + cricketTeamsUpdated + " VALUES  (1, 'Australia', 'Australia'),     (2, 'India', 'Asia'),       (3, 'Pakistan', 'Asia'),     (4, 'Srilanka', 'Asia'),   (5, 'Bangaladesh', 'Asia'),"
            + " (6, 'Thailand', 'Asia'),      (8, 'England', 'Europe'),       (9, 'South Africa', 'Africa'),      (10, 'West Indies', 'North America'),       (11, 'Zimbabwe', 'Africa');";

    private static final String mergeCmd2 = "MERGE " + cricketTeams + " AS TARGET "
            + "USING " + cricketTeamsUpdated + " AS SOURCE " + "ON (TARGET.CricketTeamID = SOURCE.CricketTeamID) "
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
                TestUtils.dropTableIfExists(cricketTeams, stmt);
            } catch (Exception ex) {
                fail(ex.toString());
            }
        }
    }
}
