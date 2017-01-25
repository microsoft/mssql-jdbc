/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

public class AzureActiveDirectoryAuthentication {

    public static void main(String[] args) {

        // Declare the JDBC objects.
        Connection con = null;
        Statement stmt = null;
        CallableStatement cstmt = null;
        ResultSet rs = null;

        String serverName = null;
        String portNumber = null;
        String databaseName = null;
        String username = null;
        String password = null;
        String authentication = null;
        String hostNameInCertificate = null;

        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

            System.out.println("Remember to put sqljdbc_auth.dll in the same directory as the pom.xml file.");

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
            System.out.print("Enter authentication: ");		// e.g. ActiveDirectoryPassword
            authentication = br.readLine();
            System.out.print("Enter host name in certificate: ");	// e.g. *.database.windows.net
            hostNameInCertificate = br.readLine();

            // Establish the connection.
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(serverName);
            ds.setPortNumber(Integer.parseInt(portNumber));
            ds.setDatabaseName(databaseName);
            ds.setUser(username);
            ds.setPassword(password);
            ds.setAuthentication(authentication);
            ds.setHostNameInCertificate(hostNameInCertificate);

            con = ds.getConnection();

            System.out.println();
            System.out.println("Connection established successfully.");

            // Create and execute an SQL statement that returns user name.
            String SQL = "SELECT SUSER_SNAME()";
            stmt = con.createStatement();
            rs = stmt.executeQuery(SQL);

            // Iterate through the data in the result set and display it.
            while (rs.next()) {
                System.out.println("user name: " + rs.getString(1));
            }
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
            if (cstmt != null)
                try {
                    cstmt.close();
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
}
