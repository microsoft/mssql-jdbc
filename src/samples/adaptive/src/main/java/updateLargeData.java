/*=====================================================================
File: 	 updateLargeData.java
Summary: This Microsoft JDBC Driver for SQL Server sample application
         demonstrates how to update the large data in a database. 
         It also demonstrates how to set the adaptive buffering mode 
         explicitly for updatable result sets.
---------------------------------------------------------------------
This file is part of the Microsoft JDBC Driver for SQL Server Code Samples.
Copyright (C) Microsoft Corporation.  All rights reserved.

This source code is intended only as a supplement to Microsoft
Development Tools and/or on-line documentation.  See these other
materials for detailed information regarding Microsoft code samples.

THIS CODE AND INFORMATION ARE PROVIDED "AS IS" WITHOUT WARRANTY OF
ANY KIND, EITHER EXPRESSED OR IMPLIED, INCLUDING BUT NOT LIMITED TO
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND/OR FITNESS FOR A
PARTICULAR PURPOSE.
=====================================================================*/
import java.sql.*;
import java.io.*;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

public class updateLargeData {

	public static void main(String[] args) {

		// Declare the JDBC objects.
		Connection con = null;
		Statement stmt = null;
		ResultSet rs = null;

		String serverName = null;
		String portNumber = null;
		String databaseName = null;
		String username = null;
		String password= null;
		
		Reader reader = null;

		try(BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

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
			String connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" +
					"databaseName="+ databaseName + ";username=" + username + ";password=" + password + ";";

			// Establish the connection.
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
			con = DriverManager.getConnection(connectionUrl);

			createTable(con);

			stmt = con.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

			// Since the summaries could be large, we should make sure that
			// the driver reads them incrementally from a database, 
			// even though a server cursor is used for the updatable result sets.

			// The recommended way to access the Microsoft JDBC Driver for SQL Server
			// specific methods is to use the JDBC 4.0 Wrapper functionality. 
			// The following code statement demonstrates how to use the 
			// Statement.isWrapperFor and Statement.unwrap methods
			// to access the driver specific response buffering methods.

			if (stmt.isWrapperFor(com.microsoft.sqlserver.jdbc.SQLServerStatement.class))
			{
				SQLServerStatement SQLstmt = 
						stmt.unwrap(com.microsoft.sqlserver.jdbc.SQLServerStatement.class);

				SQLstmt.setResponseBuffering("adaptive");
				System.out.println("Response buffering mode has been set to " +
						SQLstmt.getResponseBuffering());
			}

			// Select all of the document summaries.
			rs = stmt.executeQuery("SELECT Title, DocumentSummary FROM Document_JDBC_Sample");

			// Update each document summary.
			while (rs.next()) {

				// Retrieve the original document summary.
				reader = rs.getCharacterStream("DocumentSummary");

				if (reader == null)
				{
					// Update the document summary.
					System.out.println("Updating " + rs.getString("Title"));
					rs.updateString("DocumentSummary", "Work in progress");
					rs.updateRow();
				}
				else
				{
					System.out.println("reading " + rs.getString("Title"));
					reader.close();
					reader = null;
				}
			}
		}
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
			if (reader != null) try { reader.close(); } catch(Exception e) {}
			if (rs != null) try { rs.close(); } catch(Exception e) {}
			if (stmt != null) try { stmt.close(); } catch(Exception e) {}
			if (con != null) try { con.close(); } catch(Exception e) {}
		}
	}

	private static void createTable(Connection con) throws SQLException {
		Statement stmt = con.createStatement();

		stmt.execute("if exists (select * from sys.objects where name = 'Document_JDBC_Sample')" +
				"drop table Document_JDBC_Sample" );

		String sql = "CREATE TABLE Document_JDBC_Sample ("
				+ "[DocumentID] [int] NOT NULL identity,"
				+ "[Title] [char](50) NOT NULL,"
				+ "[DocumentSummary] [varchar](max) NULL)";

		stmt.execute(sql);

		sql = "INSERT Document_JDBC_Sample VALUES ('title1','summary1') ";
		stmt.execute(sql);

		sql = "INSERT Document_JDBC_Sample (title) VALUES ('title2') ";
		stmt.execute(sql);

		sql = "INSERT Document_JDBC_Sample (title) VALUES ('title3') ";
		stmt.execute(sql);

		sql = "INSERT Document_JDBC_Sample VALUES ('title4','summary3') ";
		stmt.execute(sql);
	}
}

