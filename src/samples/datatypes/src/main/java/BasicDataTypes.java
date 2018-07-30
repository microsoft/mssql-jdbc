/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package datatypes.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;

import microsoft.sql.DateTimeOffset;

/**
 * Sample to demonstrate usage of Basic Datatypes in SQL Server with JDBC Driver
 */
public class BasicDataTypes {

    private static String tableName = "DataTypesTable_JDBC_Sample";

    public static void main(String[] args) {

        String serverName = null;
        String portNumber = null;
        String databaseName = null;
        String username = null;
        String password = null;

        try (InputStreamReader in = new InputStreamReader(System.in); BufferedReader br = new BufferedReader(in)) {
            System.out.print("Enter server name: ");
            serverName = br.readLine();
            System.out.print("Enter port number: ");
            portNumber = br.readLine();
            System.out.print("Enter database name: ");
            databaseName = br.readLine();
            System.out.print("Enter username: ");
            username = br.readLine();
            System.out.print("Enter password: ");
            password = br.readLine();

            // Establish the connection.
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(serverName);
            ds.setPortNumber(Integer.parseInt(portNumber));
            ds.setDatabaseName(databaseName);
            ds.setUser(username);
            ds.setPassword(password);

            // Establish the connection.
            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement();) {
                dropAndCreateTable(stmt);
                insertOriginalData(con);

                System.out.println();

                // Create and execute an SQL statement that returns some data
                // and display it.
                String SQL = "SELECT * FROM " + tableName;
                try (Statement stmt1 = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);
                        ResultSet rs = stmt1.executeQuery(SQL)) {
                    rs.next();
                    displayRow("ORIGINAL DATA", rs);

                    // Update the data in the result set.
                    rs.updateInt(1, 200);
                    rs.updateString(2, "B");
                    rs.updateString(3, "Some updated text.");
                    rs.updateBoolean(4, true);
                    rs.updateDouble(5, 77.89);
                    rs.updateDouble(6, 1000.01);
                    long timeInMillis = System.currentTimeMillis();
                    Timestamp ts = new Timestamp(timeInMillis);
                    rs.updateTimestamp(7, ts);
                    rs.updateDate(8, new Date(timeInMillis));
                    rs.updateTime(9, new Time(timeInMillis));
                    rs.updateTimestamp(10, ts);

                    // -480 indicates GMT - 8:00 hrs
                    ((SQLServerResultSet) rs).updateDateTimeOffset(11, DateTimeOffset.valueOf(ts, -480));

                    rs.updateRow();
                }
                // Get the updated data from the database and display it.
                try (ResultSet rs = stmt.executeQuery(SQL)) {
                    rs.next();
                    displayRow("UPDATED DATA", rs);
                }
            }
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void displayRow(String title, ResultSet rs) throws SQLException {
        System.out.println(title);
        System.out.println(rs.getInt(1) + " , " + // SQL integer type.
                rs.getString(2) + " , " + // SQL char type.
                rs.getString(3) + " , " + // SQL varchar type.
                rs.getBoolean(4) + " , " + // SQL bit type.
                rs.getDouble(5) + " , " + // SQL decimal type.
                rs.getDouble(6) + " , " + // SQL money type.
                rs.getTimestamp(7) + " , " + // SQL datetime type.
                rs.getDate(8) + " , " + // SQL date type.
                rs.getTime(9) + " , " + // SQL time type.
                rs.getTimestamp(10) + " , " + // SQL datetime2 type.
                ((SQLServerResultSet) rs).getDateTimeOffset(11)); // SQL datetimeoffset type.
        System.out.println();
    }

    private static void dropAndCreateTable(Statement stmt) throws SQLException {
        stmt.executeUpdate("if object_id('" + tableName + "','U') is not null" + " drop table " + tableName);

        String sql = "create table " + tableName + " (" + "c1 int, " + "c2 char(20), " + "c3 varchar(20), " + "c4 bit, "
                + "c5 decimal(10,5), " + "c6 money, " + "c7 datetime, " + "c8 date, " + "c9 time(7), "
                + "c10 datetime2(7), " + "c11 datetimeoffset(7), " + ");";

        stmt.execute(sql);
    }

    private static void insertOriginalData(Connection con) throws SQLException {
        String sql = "insert into " + tableName + " values( " + "?,?,?,?,?,?,?,?,?,?,?" + ")";
        try (PreparedStatement pstmt = con.prepareStatement(sql)) {
            pstmt.setObject(1, 100);
            pstmt.setObject(2, "original text");
            pstmt.setObject(3, "original text");
            pstmt.setObject(4, false);
            pstmt.setObject(5, 12.34);
            pstmt.setObject(6, 56.78);
            pstmt.setObject(7, new Date(1453500034839L));
            pstmt.setObject(8, new Date(1453500034839L));
            pstmt.setObject(9, new java.util.Date(1453500034839L));
            pstmt.setObject(10, new Date(1453500034839L));
            pstmt.setObject(11, new Date(1453500034839L));
            pstmt.execute();
        }
    }
}
