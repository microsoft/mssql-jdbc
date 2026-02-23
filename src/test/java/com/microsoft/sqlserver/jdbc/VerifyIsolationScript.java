package com.microsoft.sqlserver.jdbc;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.Properties;

public class VerifyIsolationScript {
    public static void main(String[] args) {
        // Connection string provided by the user
        String url = "jdbc:sqlserver://;serverName=mibsql.pub.minilab.hi.inet;databaseName=gvp;encrypt=true;trustServerCertificate=true;transactionIsolation=1;";

        // IMPORTANT: Update these with your actual credentials
        String user = "DataHub_Pods_Reporting";
        String password = "1a2b3c4d";

        Properties props = new Properties();
        props.setProperty("user", user);
        props.setProperty("password", password);

        System.out.println("Connecting to: " + url);

        try (Connection con = DriverManager.getConnection(url, props)) {
            System.out.println("Connected successfully!");

            // 1. Verify Transaction Isolation Level on Server
            try (Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(
                            "SELECT transaction_isolation_level FROM sys.dm_exec_sessions WHERE session_id = @@SPID")) {
                if (rs.next()) {
                    int isolationLevel = rs.getInt(1);
                    System.out.println("Active Transaction Isolation Level on SQL Server: " + isolationLevel);
                    if (isolationLevel == 1) {
                        System.out.println("SUCCESS: READ_UNCOMMITTED (1) is active.");
                    } else {
                        System.out.println("WARNING: Isolation level is " + isolationLevel + " (expected 1).");
                    }
                }
            }

            // 2. Execute the user's query
            String query = "select count(ID) from GVP_USERS_TAGS;";
            System.out.println("Executing query: " + query);
            try (Statement stmt = con.createStatement();
                    ResultSet rs = stmt.executeQuery(query)) {
                if (rs.next()) {
                    long count = rs.getLong(1);
                    System.out.println("Query Result (count): " + count);
                }
            }

        } catch (Exception e) {
            System.err.println("Error occurred:");
            e.printStackTrace();
        }
    }
}
