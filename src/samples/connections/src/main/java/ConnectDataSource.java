/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package connections.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Sample application that demonstrates how to connect to a SQL Server database by 
 * using a data source object.
 */
public class ConnectDataSource {

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

            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement();) {

                System.out.println();
                System.out.println("Connection established successfully.");

                // Create and execute an SQL statement that returns user name.
                String SQL = "SELECT SUSER_SNAME()";
                
                try (ResultSet rs = stmt.executeQuery(SQL)) {

                    // Iterate through the data in the result set and display it.
                    while (rs.next()) {
                        System.out.println("user name: " + rs.getString(1));
                    }
                }
            }
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}
