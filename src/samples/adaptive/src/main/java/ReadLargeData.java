/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package adaptive.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

/**
 * Sample application to demonstrate how to read the large data from a database and 
 * how to get the adaptive buffering mode. 
 * 
 * It also demonstrates how to retrieve a large single-column value from a SQL Server 
 * database by using the getCharacterStream method.
 */
public class ReadLargeData {

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
            try (Connection con = DriverManager.getConnection(connectionUrl);
                    SQLServerStatement stmt = (SQLServerStatement) con.createStatement()) {

                createTable(stmt);
                // Create test data as an example.
                StringBuffer buffer = new StringBuffer(4000);
                for (int i = 0; i < 4000; i++)
                    buffer.append((char) ('A'));

                try (PreparedStatement pstmt = con.prepareStatement(
                        "UPDATE Document_JDBC_Sample " + "SET DocumentSummary = ? WHERE (DocumentID = 1)")) {

                    pstmt.setString(1, buffer.toString());
                    pstmt.executeUpdate();
                }

                // In adaptive mode, the application does not have to use a server cursor
                // to avoid OutOfMemoryError when the SELECT statement produces very large
                // results.

                // Create and execute an SQL statement that returns some data.
                String SQL = "SELECT Title, DocumentSummary " + "FROM Document_JDBC_Sample";

                // Display the response buffering mode.
                System.out.println("Response buffering mode is: " + stmt.getResponseBuffering());

                // Get the updated data from the database and display it.
                try (ResultSet rs = stmt.executeQuery(SQL)) {

                    while (rs.next()) {
                        try (Reader reader = rs.getCharacterStream(2)) {
                            if (reader != null) {
                                char output[] = new char[40];
                                while (reader.read(output) != -1) {
                                    // Print the chunk of the data that was read.
                                    String stringOutput = new String(output);
                                    System.out.println("Document_Summary Data Chunk: " + stringOutput);
                                }

                                System.out.println(rs.getString(1) + " has been accessed for the summary column.");
                            }
                        }
                    }
                }
            }
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createTable(SQLServerStatement stmt) throws SQLException {
        stmt.execute("if exists (select * from sys.objects where name = 'Document_JDBC_Sample')"
                + "drop table Document_JDBC_Sample");

        String sql = "CREATE TABLE Document_JDBC_Sample (" + "[DocumentID] [int] NOT NULL identity,"
                + "[Title] [char](50) NOT NULL," + "[DocumentSummary] [varchar](max) NULL)";

        stmt.execute(sql);

        sql = "INSERT Document_JDBC_Sample VALUES ('title1','summary1') ";
        stmt.execute(sql);

        sql = "INSERT Document_JDBC_Sample VALUES ('title2','summary2') ";
        stmt.execute(sql);

        sql = "INSERT Document_JDBC_Sample VALUES ('title3','summary3') ";
        stmt.execute(sql);
    }
}
