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
import java.util.ArrayList;

import com.microsoft.sqlserver.jdbc.SQLServerResultSet;

/**
 * Sample application that demonstrates how to use a result set to retrieve a large set
 * of data from a SQL Server database.
 * 
 * In addition, it demonstrates how to control the amount of data that is fetched
 * from the database and cached on the client.
 */
public class CacheResultSet {

    private static final int ROW_COUNT = 10000;

    @SuppressWarnings("serial")
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
                    Statement stmt1 = con.createStatement(SQLServerResultSet.TYPE_SS_SERVER_CURSOR_FORWARD_ONLY,
                            +SQLServerResultSet.CONCUR_READ_ONLY);) {

                createTable(stmt);

                // Create and execute an SQL statement that returns a large
                // set of data and then display it.
                String SQL = "SELECT * FROM SalesOrderDetail_JDBC_Sample;";

                for (int n : new ArrayList<Integer>() {
                    {
                        add(1);
                        add(10);
                        add(100);
                        add(1000);
                        add(0);
                    }
                }) {
                    // Perform a fetch for every nth row in the result set.
                    try (ResultSet rs = stmt.executeQuery(SQL)) {
                        timerTest(n, rs);
                    }
                }
            }
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void timerTest(int fetchSize, ResultSet rs) {
        try {

            // Declare the variables for tracking the row count and elapsed time.
            int rowCount = 0;
            long startTime = 0;
            long stopTime = 0;
            long runTime = 0;

            // Set the fetch size and then iterate through the result set to
            // cache the data locally.
            rs.setFetchSize(fetchSize);
            startTime = System.currentTimeMillis();
            while (rs.next()) {
                rowCount++;
            }
            stopTime = System.currentTimeMillis();
            runTime = stopTime - startTime;

            // Display the results of the timer test.
            System.out.println("FETCH SIZE: " + rs.getFetchSize());
            System.out.println("ROWS PROCESSED: " + rowCount);
            System.out.println("TIME TO EXECUTE: " + runTime);
            System.out.println();

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(Statement stmt) throws SQLException {
        stmt.execute("if exists (select * from sys.objects where name = 'SalesOrderDetail_JDBC_Sample')"
                + "drop table SalesOrderDetail_JDBC_Sample");

        String sql = "CREATE TABLE [SalesOrderDetail_JDBC_Sample](" + "[SalesOrderID] [int] NOT NULL,"
                + "[SalesOrderDetailID] [int] IDENTITY(1,1) NOT NULL," + "[CarrierTrackingNumber] [nvarchar](25) NULL,"
                + "[OrderQty] [smallint] NOT NULL," + "[ProductID] [int] NOT NULL," + "[SpecialOfferID] [int] NOT NULL,"
                + "[UnitPrice] [money] NOT NULL," + "[UnitPriceDiscount] [money] NOT NULL,"
                + "[LineTotal]  AS (isnull(([UnitPrice]*((1.0)-[UnitPriceDiscount]))*[OrderQty],(0.0))),"
                + "[rowguid] [uniqueidentifier] ROWGUIDCOL  NOT NULL," + "[ModifiedDate] [datetime] NOT NULL)";

        stmt.execute(sql);

        for (int i = 0; i < ROW_COUNT; i++) {
            sql = "INSERT SalesOrderDetail_JDBC_Sample VALUES ('1','4911-403C-98','5','1','0','10.5555','0.00','5A74C7D2-E641-438E-A7AC-37BF23280301','2011-05-31 00:00:00.000') ";
            stmt.execute(sql);
        }
    }
}
