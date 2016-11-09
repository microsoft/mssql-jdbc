/*=====================================================================
File: 	 readLargeData.java
Summary: This Microsoft JDBC Driver for SQL Server sample application
         demonstrates how to read the large data from a database and 
         how to get the adaptive buffering mode. It also demonstrates
         how to retrieve a large single-column value from a SQL Server 
         database by using the getCharacterStream method.
---------------------------------------------------------------------
Microsoft JDBC Driver for SQL Server
Copyright(c) Microsoft Corporation
All rights reserved.
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
=====================================================================*/
import java.sql.*;
import java.io.*;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

public class readLargeData {

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
			
			// Create test data as an example.
			StringBuffer buffer = new StringBuffer(4000);
			for (int i = 0; i < 4000; i++) 
				buffer.append( (char) ('A'));

			PreparedStatement pstmt = con.prepareStatement(
					"UPDATE Document_JDBC_Sample " +
					"SET DocumentSummary = ? WHERE (DocumentID = 1)");

			pstmt.setString(1, buffer.toString());
			pstmt.executeUpdate();
			pstmt.close();

			// In adaptive mode, the application does not have to use a server cursor 
			// to avoid OutOfMemoryError when the SELECT statement produces very large
			// results. 

			// Create and execute an SQL statement that returns some data.
			String SQL = "SELECT Title, DocumentSummary " +
					"FROM Document_JDBC_Sample";
			stmt = con.createStatement();

			// Display the response buffering mode.
			SQLServerStatement SQLstmt = (SQLServerStatement) stmt;          
			System.out.println("Response buffering mode is: " +
					SQLstmt.getResponseBuffering());              

			// Get the updated data from the database and display it.
			rs = stmt.executeQuery(SQL);

			while (rs.next()) {
				Reader reader = rs.getCharacterStream(2);
				if (reader != null)
				{
					char output[] = new char[40];
					while (reader.read(output) != -1)
					{
						// Print the chunk of the data that was read.
						String stringOutput = new String(output);
						System.out.println("Document_Summary Data Chunk: " + stringOutput);
					}		 		 

					System.out.println(rs.getString(1) + 
							" has been accessed for the summary column.");
					// Close the stream.
					reader.close();
				}
			}
		}
		// Handle any errors that may have occurred.
		catch (Exception e) {
			e.printStackTrace();
		}
		finally {
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

		sql = "INSERT Document_JDBC_Sample VALUES ('title2','summary2') ";
		stmt.execute(sql);

		sql = "INSERT Document_JDBC_Sample VALUES ('title3','summary3') ";
		stmt.execute(sql);
	}
}
