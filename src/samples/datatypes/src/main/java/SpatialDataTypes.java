/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package datatypes.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

/**
 * Sample to demonstrate how to use Spatial Datatypes 'Geography' and 'Geometry' in SQL Server with JDBC Driver
 */
public class SpatialDataTypes {

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

            // Create a variable for the connection string.
            String connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" + "databaseName="
                    + databaseName + ";username=" + username + ";password=" + password + ";";

            // Establish the connection.
            try (Connection con = DriverManager.getConnection(connectionUrl); Statement stmt = con.createStatement()) {
                // TODO: Implement Sample code
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
}
