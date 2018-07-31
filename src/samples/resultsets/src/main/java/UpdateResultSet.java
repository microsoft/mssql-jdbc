/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package resultsets.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Sample application that demonstrates how to use an updateable result set to insert,
 * update, and delete a row of data in a SQL Server database.
 */
public class UpdateResultSet {

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
            System.out.println();

            // Create a variable for the connection string.
            String connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" + "databaseName="
                    + databaseName + ";username=" + username + ";password=" + password + ";";

            // Establish the connection.
            try (Connection con = DriverManager.getConnection(connectionUrl); Statement stmt = con.createStatement();
                    Statement stmt1 = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE,
                            ResultSet.CONCUR_UPDATABLE)) {

                createTable(stmt);

                // Create and execute an SQL statement, retrieving an updateable result set.
                String SQL = "SELECT * FROM Department_JDBC_Sample;";

                try (ResultSet rs = stmt.executeQuery(SQL)) {

                    // Insert a row of data.
                    rs.moveToInsertRow();
                    rs.updateString("Name", "Accounting");
                    rs.updateString("GroupName", "Executive General and Administration");
                    rs.updateString("ModifiedDate", "08/01/2006");
                    rs.insertRow();
                }

                // Retrieve the inserted row of data and display it.
                SQL = "SELECT * FROM Department_JDBC_Sample WHERE Name = 'Accounting';";
                try (ResultSet rs = stmt.executeQuery(SQL)) {
                    displayRow("ADDED ROW", rs);

                    // Update the row of data.
                    rs.first();
                    rs.updateString("GroupName", "Finance");
                    rs.updateRow();
                }
                // Retrieve the updated row of data and display it.
                try (ResultSet rs = stmt.executeQuery(SQL)) {
                    displayRow("UPDATED ROW", rs);

                    // Delete the row of data.
                    rs.first();
                    rs.deleteRow();
                    System.out.println("ROW DELETED");
                }
            }
        }

        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Statement stmt) throws SQLException {
        stmt.execute("if exists (select * from sys.objects where name = 'Department_JDBC_Sample')"
                + "drop table Department_JDBC_Sample");

        String sql = "CREATE TABLE [Department_JDBC_Sample](" + "[DepartmentID] [smallint] IDENTITY(1,1) NOT NULL,"
                + "[Name] [varchar](50) NOT NULL," + "[GroupName] [varchar](50) NOT NULL,"
                + "[ModifiedDate] [datetime] NOT NULL,)";

        stmt.execute(sql);
    }

    private static void displayRow(String title, ResultSet rs) {
        try {
            System.out.println(title);
            while (rs.next()) {
                System.out.println(rs.getString("Name") + " : " + rs.getString("GroupName"));
                System.out.println();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
