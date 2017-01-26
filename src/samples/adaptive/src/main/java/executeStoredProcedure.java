/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import com.microsoft.sqlserver.jdbc.SQLServerCallableStatement;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;

public class executeStoredProcedure {

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

            // Establish the connection.
            SQLServerDataSource ds = new SQLServerDataSource();
            ds.setServerName(serverName);
            ds.setPortNumber(Integer.parseInt(portNumber));
            ds.setDatabaseName(databaseName);
            ds.setUser(username);
            ds.setPassword(password);

            con = ds.getConnection();

            createTable(con);
            createStoredProcedure(con);

            // Create test data as an example.
            StringBuffer buffer = new StringBuffer(4000);
            for (int i = 0; i < 4000; i++)
                buffer.append((char) ('A'));

            PreparedStatement pstmt = con.prepareStatement("UPDATE Document_JDBC_Sample " + "SET DocumentSummary = ? WHERE (DocumentID = 1)");

            pstmt.setString(1, buffer.toString());
            pstmt.executeUpdate();
            pstmt.close();

            // Query test data by using a stored procedure.
            CallableStatement cstmt = con.prepareCall("{call GetLargeDataValue(?, ?, ?, ?)}");

            cstmt.setInt(1, 1);
            cstmt.registerOutParameter(2, java.sql.Types.INTEGER);
            cstmt.registerOutParameter(3, java.sql.Types.CHAR);
            cstmt.registerOutParameter(4, java.sql.Types.LONGVARCHAR);

            // Display the response buffering mode.
            SQLServerCallableStatement SQLcstmt = (SQLServerCallableStatement) cstmt;
            System.out.println("Response buffering mode is: " + SQLcstmt.getResponseBuffering());

            SQLcstmt.execute();
            System.out.println("DocumentID: " + cstmt.getInt(2));
            System.out.println("Document_Title: " + cstmt.getString(3));

            Reader reader = SQLcstmt.getCharacterStream(4);

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

            // Close the stream.
            reader.close();
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

    private static void createStoredProcedure(Connection con) throws SQLException {
        Statement stmt = con.createStatement();

        String outputProcedure = "GetLargeDataValue";

        String sql = " IF EXISTS (select * from sysobjects where id = object_id(N'" + outputProcedure
                + "') and OBJECTPROPERTY(id, N'IsProcedure') = 1)" + " DROP PROCEDURE " + outputProcedure;
        stmt.execute(sql);

        sql = "CREATE PROCEDURE " + outputProcedure + " @p0 int, @p1 int OUTPUT, @p2 char(50) OUTPUT, " + "@p3 varchar(max) OUTPUT " + " AS"
                + " SELECT top 1 @p1=DocumentID, @p2=Title," + " @p3=DocumentSummary FROM Document_JDBC_Sample where DocumentID = @p0";

        stmt.execute(sql);
    }

    private static void createTable(Connection con) throws SQLException {
        Statement stmt = con.createStatement();

        stmt.execute("if exists (select * from sys.objects where name = 'Document_JDBC_Sample')" + "drop table Document_JDBC_Sample");

        String sql = "CREATE TABLE Document_JDBC_Sample(" + "[DocumentID] [int] NOT NULL identity," + "[Title] [char](50) NOT NULL,"
                + "[DocumentSummary] [varchar](max) NULL)";

        stmt.execute(sql);

        sql = "INSERT Document_JDBC_Sample VALUES ('title1','summary1') ";
        stmt.execute(sql);

        sql = "INSERT Document_JDBC_Sample VALUES ('title2','summary2') ";
        stmt.execute(sql);

        sql = "INSERT Document_JDBC_Sample VALUES ('title3','summary3') ";
        stmt.execute(sql);
    }
}
