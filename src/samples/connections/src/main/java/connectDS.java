/*=====================================================================
  File:    connectDS.java      
  Summary: This Microsoft JDBC Driver for SQL Server sample application
	     demonstrates how to connect to a SQL Server database by 
	     using a data source object. It also demonstrates how to 
	     retrieve data from a SQL Server database by using a stored 
	     procedure.
---------------------------------------------------------------------
Microsoft JDBC Driver for SQL Server
Copyright(c) Microsoft Corporation
All rights reserved.
MIT License

Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :

The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
===================================================================== */

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.sql.*;
import com.microsoft.sqlserver.jdbc.*;

public class connectDS {

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

			// Establish the connection. 
			SQLServerDataSource ds = new SQLServerDataSource();
			ds.setServerName(serverName);
			ds.setPortNumber(Integer.parseInt(portNumber)); 
			ds.setDatabaseName(databaseName);
			ds.setUser(username);
			ds.setPassword(password);
			
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
			if (rs != null) try { rs.close(); } catch(Exception e) {}
			if (cstmt != null) try { cstmt.close(); } catch(Exception e) {}
			if (con != null) try { con.close(); } catch(Exception e) {}
		}
	}
}
