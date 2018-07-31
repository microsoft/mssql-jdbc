/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package adaptive.src.main.java;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

/**
 * Sample application to demonstrate how to retrieve a large OUT parameter from 
 * a stored procedure and how to get the adaptive buffering mode.
 */
public class ExecuteStoredProcedures {

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

            try (Connection con = ds.getConnection(); Statement stmt = con.createStatement()) {

                createTable(stmt);
                createStoredProcedure(stmt);

                // Create test data as an example.
                StringBuffer buffer = new StringBuffer(4000);
                for (int i = 0; i < 4000; i++)
                    buffer.append((char) ('A'));

                try (PreparedStatement pstmt = con.prepareStatement(
                        "UPDATE Document_JDBC_Sample " + "SET DocumentSummary = ? WHERE (DocumentID = 1)")) {

                    pstmt.setString(1, buffer.toString());
                    pstmt.executeUpdate();
                }

                // Query test data by using a stored procedure.
                try (SQLServerCallableStatement cstmt = (SQLServerCallableStatement) con
                        .prepareCall("{call GetLargeDataValue(?, ?, ?, ?)}")) {

                    cstmt.setInt(1, 1);
                    cstmt.registerOutParameter(2, java.sql.Types.INTEGER);
                    cstmt.registerOutParameter(3, java.sql.Types.CHAR);
                    cstmt.registerOutParameter(4, java.sql.Types.LONGVARCHAR);

                    // Display the response buffering mode.
                    System.out.println("Response buffering mode is: " + cstmt.getResponseBuffering());

                    cstmt.execute();
                    System.out.println("DocumentID: " + cstmt.getInt(2));
                    System.out.println("Document_Title: " + cstmt.getString(3));

                    try (Reader reader = cstmt.getCharacterStream(4)) {

                        // If your application needs to re-read any portion of the value,
                        // it must call the mark method on the InputStream or Reader to
                        // start buffering data that is to be re-read after a subsequent
                        // call to the reset method.
                        reader.mark(4000);

                        // Read the first half of data.
                        char output1[] = new char[2000];
                        reader.read(output1);
                        String stringOutput1 = new String(output1);

                        // Reset the stream.
                        reader.reset();

                        // Read all the data.
                        char output2[] = new char[4000];
                        reader.read(output2);
                        String stringOutput2 = new String(output2);

                        System.out.println("Document_Summary in half: " + stringOutput1);
                        System.out.println("Document_Summary: " + stringOutput2);
                    }
                }
            }
        }
        // Handle any errors that may have occurred.
        catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static void createStoredProcedure(Statement stmt) throws SQLException {
        String outputProcedure = "GetLargeDataValue";

        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + outputProcedure
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE " + outputProcedure;
        stmt.execute(sql);

        sql = "CREATE PROCEDURE " + outputProcedure + " @p0 int, @p1 int OUTPUT, @p2 char(50) OUTPUT, "
                + "@p3 varchar(max) OUTPUT " + " AS" + " SELECT top 1 @p1=DocumentID, @p2=Title,"
                + " @p3=DocumentSummary FROM Document_JDBC_Sample where DocumentID = @p0";

        stmt.execute(sql);
    }

    private static void createTable(Statement stmt) throws SQLException {
        stmt.execute("if exists (select * from sys.objects where name = 'Document_JDBC_Sample')"
                + "drop table Document_JDBC_Sample");

        String sql = "CREATE TABLE Document_JDBC_Sample(" + "[DocumentID] [int] NOT NULL identity,"
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
