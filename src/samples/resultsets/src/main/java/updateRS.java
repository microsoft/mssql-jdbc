/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class updateRS {

    public static void main(String[] args) {

        // Declare the JDBC objects.
        Connection con = null;
        Statement stmt = null;
        ResultSet rs = null;

        String serverName = null;
        String portNumber = null;
        String databaseName = null;
        String username = null;
        String password = null;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

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

            // Create a variable for the connection string.
            String connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" + "databaseName=" + databaseName + ";username="
                    + username + ";password=" + password + ";";

            // Establish the connection.
            Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            con = DriverManager.getConnection(connectionUrl);

            createTable(con);

            // Create and execute an SQL statement, retrieving an updateable result set.
            String SQL = "SELECT * FROM Department_JDBC_Sample;";
            stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
            rs = stmt.executeQuery(SQL);

            // Insert a row of data.
            rs.moveToInsertRow();
            rs.updateString("Name", "Accounting");
            rs.updateString("GroupName", "Executive General and Administration");
            rs.updateString("ModifiedDate", "08/01/2006");
            rs.insertRow();

            if (rs != null) {
                rs.close();
            }

            // Retrieve the inserted row of data and display it.
            SQL = "SELECT * FROM Department_JDBC_Sample WHERE Name = 'Accounting';";
            rs = stmt.executeQuery(SQL);
            displayRow("ADDED ROW", rs);

            // Update the row of data.
            rs.first();
            rs.updateString("GroupName", "Finance");
            rs.updateRow();

            // Retrieve the updated row of data and display it.
            rs = stmt.executeQuery(SQL);
            displayRow("UPDATED ROW", rs);

            // Delete the row of data.
            rs.first();
            rs.deleteRow();
            System.out.println("ROW DELETED");
        }

        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }

        finally {
            if (rs != null)
                try {
                    rs.close();
                }
                catch (Exception e) {
                }
            if (stmt != null)
                try {
                    stmt.close();
                }
                catch (Exception e) {
                }
            if (con != null)
                try {
                    con.close();
                }
                catch (Exception e) {
                }
        }
    }

    private static void createTable(Connection con) throws SQLException {
        Statement stmt = con.createStatement();

        stmt.execute("if exists (select * from sys.objects where name = 'Department_JDBC_Sample')" + "drop table Department_JDBC_Sample");

        String sql = "CREATE TABLE [Department_JDBC_Sample](" + "[DepartmentID] [smallint] IDENTITY(1,1) NOT NULL," + "[Name] [varchar](50) NOT NULL,"
                + "[GroupName] [varchar](50) NOT NULL," + "[ModifiedDate] [datetime] NOT NULL,)";

        stmt.execute(sql);
    }

    private static void displayRow(String title,
            ResultSet rs) {
        try {
            System.out.println(title);
            while (rs.next()) {
                System.out.println(rs.getString("Name") + " : " + rs.getString("GroupName"));
                System.out.println();
            }
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}