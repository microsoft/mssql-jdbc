/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.preparedStatement;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.Utils;

import static org.junit.jupiter.api.Assertions.fail;

/**
 * Tests with sql queries using preparedStatement without parameters
 * 
 *
 */
@RunWith(JUnitPlatform.class)
public class RegressionTest extends AbstractTest {
    static Connection con = null;
    static PreparedStatement pstmt1 = null;
    static PreparedStatement pstmt2 = null;
    static PreparedStatement pstmt3 = null;
    static PreparedStatement pstmt4 = null;

    @BeforeAll
    public static void setupTest() throws SQLException {
        con = DriverManager.getConnection(connectionString);
        Statement stmt = con.createStatement();
        Utils.dropTableIfExists("x", stmt);
        Utils.dropViewIfExists("x", stmt);
        if (null != stmt){
            stmt.close();
        }
    }

    /**
     * Tests creating view using preparedStatement
     * @throws SQLException
     */
    @Test
    public void createViewTest() throws SQLException {
        try {
            pstmt1 = con.prepareStatement("create view x as select 1 a");
            pstmt2 = con.prepareStatement("drop view x");
            pstmt1.execute();
            pstmt2.execute();
        }
        catch (SQLException e) {
            fail("Create/drop view with preparedStatement failed! Error message: " + e.getMessage());
        }

        finally {
            if (null != pstmt1) {
                pstmt1.close();
            }
            if (null != pstmt2) {
                pstmt2.close();
            }
        }
    }
    
    /**
     * Tests creating schema using preparedStatement
     * @throws SQLException
     */
    @Test
    public void createSchemaTest() throws SQLException {
        try {
            pstmt1 = con.prepareStatement("create schema x");
            pstmt2 = con.prepareStatement("drop schema x");
            pstmt1.execute();
            pstmt2.execute();
        }
        catch (SQLException e) {
            fail("Create/drop schema with preparedStatement failed! Error message:" + e.getMessage());
        }

        finally {
            if (null != pstmt1) {
                pstmt1.close();
            }
            if (null != pstmt2) {
                pstmt2.close();
            }
        }
    }
    
    @Test
    public void createTableTest() throws SQLException {
        try {
            pstmt1 = con.prepareStatement("create table x (col1 int)");
            pstmt2 = con.prepareStatement("drop table x");
            pstmt1.execute();
            pstmt2.execute();
        }
        catch (SQLException e) {
            fail("Create/drop table with preparedStatement failed! Error message:" + e.getMessage());
        }

        finally {
            if (null != pstmt1) {
                pstmt1.close();
            }
            if (null != pstmt2) {
                pstmt2.close();
            }
        }
    }
    
    @Test
    public void alterTableTest() throws SQLException {
        try {
            pstmt1 = con.prepareStatement("create table x (col1 int)"); 
            pstmt2 = con.prepareStatement("ALTER TABLE x ADD column_name char;");
            pstmt3 = con.prepareStatement("drop table x");
            pstmt1.execute();
            pstmt2.execute();
            pstmt3.execute();
        }
        catch (SQLException e) {
            fail("Create/drop/alter table with preparedStatement failed! Error message:" + e.getMessage());
        }

        finally {
            if (null != pstmt1) {
                pstmt1.close();
            }
            if (null != pstmt2) {
                pstmt2.close();
            }
            if (null != pstmt3) {
                pstmt3.close();
            }
        }
    }
    
    @Test
    public void grantTest() throws SQLException {
        try {
            pstmt1 = con.prepareStatement("create table x (col1 int)");
            pstmt2 = con.prepareStatement("grant select on x to public");
            pstmt3 = con.prepareStatement("revoke select on x from public");
            pstmt4 = con.prepareStatement("drop table x");
            pstmt1.execute();
            pstmt2.execute();
            pstmt3.execute();
            pstmt4.execute();
        }
        catch (SQLException e) {
            fail("Create/drop/alter table with preparedStatement failed! Error message:" + e.getMessage());
        }

        finally {
            if (null != pstmt1) {
                pstmt1.close();
            }
            if (null != pstmt2) {
                pstmt2.close();
            }
            if (null != pstmt3) {
                pstmt3.close();
            }
            if (null != pstmt4) {
                pstmt4.close();
            }
        }
    }
    
    @AfterAll
    public static void cleanup() throws SQLException{
        Statement stmt = con.createStatement();
        Utils.dropTableIfExists("x", stmt);
        Utils.dropViewIfExists("x", stmt);
        if (null != stmt){
            stmt.close();
        }
        if (null != con) {
            con.close();
        }
        if (null != pstmt1) {
            pstmt1.close();
        }
        if (null != pstmt2) {
            pstmt2.close();
        }
        
    }

}
