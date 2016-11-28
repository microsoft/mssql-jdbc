/*
File: bvtTest.java
Contents: Basic verification tests.

Microsoft JDBC Driver for SQL Server
Copyright(c) Microsoft Corporation
All rights reserved.
MIT License
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
IN THE SOFTWARE.
 */

import java.io.BufferedReader;
import java.io.FileReader;
import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import static org.junit.Assert.*;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class bvtTest {

	private static boolean cursor = false;
	private static boolean querytimeout = false;
	private static String connectionUrl = "";
	private static Connection con;
	private static String serverName = null;
	private static String portNumber = null;
	private static String databaseName = null;
	private static String username = null;
	private static String password = null;
	private static String line = null;
	private static String driverNamePattern = "Microsoft JDBC Driver \\d.\\d for SQL Server";
	private static String table1 = "stmt_test_bvt";
	private static String table2 = "rs_test_bvt";
	private static Statement stmt = null;
	private static ResultSet rs = null;
	private static SQLServerPreparedStatement pstmt = null;
	private static bvt_ResultSet bvt_rs = null;

	@BeforeClass
	public static void init() throws SQLException {
		try {
			Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		}

		readFromConfig();

		Statement stmt = null;
		try {
			stmt = conn().createStatement();

			// CREATE the table
			stmt.executeUpdate(Tables.dropTable(table1));
			stmt.executeUpdate(Tables.createTable(table1));
			// CREATE the data to populate the table with
			Values.createData();
			Tables.populate(table1, stmt);

			stmt.executeUpdate(Tables.dropTable(table2));
			stmt.executeUpdate(Tables.createTable(table2));
			Tables.populate(table2, stmt);
		} finally {
			if (null != stmt) {
				stmt.close();
			}
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	//// Connect to specified server and close the connection
	/////////////////////////////////////////////////////////////////////
	@Test
	public void testConnection() throws SQLException {
		try {
			conn().close();
		} finally {
			terminateVariation();
		}
	}

	/////////////////////////////////////////////////////////////////////
	//// Verify isClosed()
	/////////////////////////////////////////////////////////////////////
	@Test
	public void testConnectionIsClosed() throws SQLException {
		try {
			Connection conn = conn();
			assertTrue("BVT connection should not be closed", !conn.isClosed());
			conn.close();
			assertTrue("BVT connection should not be open", conn.isClosed());
		} finally {
			terminateVariation();
		}
	}

	/////////////////////////////////////////////////////////////////////
	//// Verify Driver Name and Version from MetaData
	/////////////////////////////////////////////////////////////////////
	@Test
	public void testDriverNameAndDriverVersion() throws SQLException {
		try {
			DatabaseMetaData metaData = conn().getMetaData();
			Pattern p = Pattern.compile(driverNamePattern);
			Matcher m = p.matcher(metaData.getDriverName());
			assertTrue("Driver name is not a correct format! ", m.find());
			String[] parts = metaData.getDriverVersion().split("\\.");
			if (parts.length != 4)
				assertTrue("Driver version number should be four parts! ", true);
		} finally {
			terminateVariation();
		}
	}

	@Test
	public void testCreateStatement() throws SQLException {

		try {
			stmt = conn().createStatement();
			// SELECT * FROM <table1>
			String query = Tables.select(table1);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// close and verify
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

    ///////////////////////////////////////////////////////////////////
    // Create a statement with a query timeout
    // ResultSet.Type_forward_only,
    // ResultSet.CONCUR_READ_ONLY, executeQuery
    // verify cursor by using next and previous and verify data
    ///////////////////////////////////////////////////////////////////
	@Test
	public void testCreateStatementWithQueryTimeout() throws SQLException {

		querytimeout = true;
		
		try {
			stmt = conn().createStatement();
			assertEquals(10, stmt.getQueryTimeout());
		} finally {
			terminateVariation();
			querytimeout = false;
		}
	}
	
	///////////////////////////////////////////////////////////////////
	// Create a statement
	// ResultSet.Type_forward_only,
	// ResultSet.CONCUR_READ_ONLY, executeQuery
	// verify cursor by using next and previous and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtForwardOnlyReadOnly() throws SQLException, ClassNotFoundException {

		try {
			stmt = conn().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);

			// SELECT * FROM <table1> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table1, pk);
			rs = stmt.executeQuery(query);

			bvt_rs = new bvt_ResultSet(rs);
			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			try {
				bvt_rs.previous();
				assertTrue("Previous should have thrown an exception", false);
			} catch (SQLException ex) {
				// expected exception
			}
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Create a statement
	// ResultSet.SCROLL_INSENSITIVE,
	// ResultSet.CONCUR_READ_ONLY, executeQuery
	// verify cursor by using next, afterlast and previous and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtScrollInsensitiveReadOnly() throws SQLException, ClassNotFoundException {
		try {
			stmt = conn().createStatement(ResultSet.TYPE_SCROLL_INSENSITIVE, ResultSet.CONCUR_READ_ONLY);

			// SELECT * FROM <table1> ORDER BY <c1>, <c2>
			String c1 = Tables.primaryKey();
			String c2 = "[c1_char(512)]";

			Tables.select(table1);
			Tables.orderby(c1);
			Tables.orderby(c2);

			rs = stmt.executeQuery(Tables.query);
			bvt_rs = new bvt_ResultSet(rs);

			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			bvt_rs.afterLast();
			bvt_rs.previous();
			bvt_rs.verifyCurrentRow();
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	/////////////////////////////////////////////////////////////////
	// Create a statement
	// ResultSet.SCROLL_SENSITIVE,
	// ResultSet.CONCUR_READ_ONLY, executeQuery
	// verify cursor by using next and absolute and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtScrollSensitiveReadOnly() throws SQLException {

		try {
			stmt = conn().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_READ_ONLY);

			// SELECT * FROM <table1> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table1, pk);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			bvt_rs.absolute(3);
			bvt_rs.verifyCurrentRow();
			bvt_rs.absolute(1);
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Create a statement
	// ResultSet.Type_forward_only,
	// ResultSet.CONCUR_UPDATABLE, executeQuery
	// verify cursor by using next and previous and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtForwardOnlyUpdateable() throws SQLException {

		try {
			stmt = conn().createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_UPDATABLE);

			// SELECT * FROM <table1> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table1, pk);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			try {
				bvt_rs.previous();
				assertTrue("Previous should have thrown an exception", false);
			} catch (SQLException ex) {
				// expected exception
			}
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Create a statement
	// ResultSet.SCROLL_SENSITIVE,
	// ResultSet.CONCUR_UPDATABLE, executeQuery
	// verify cursor by using next and previous and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtScrollSensitiveUpdatable() throws SQLException {

		try {
			stmt = conn().createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

			// SELECT * FROM <table1> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table1, pk);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.next();
			bvt_rs.verifyCurrentRow();
			bvt_rs.absolute(3);
			bvt_rs.verifyCurrentRow();
			bvt_rs.absolute(1);
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Create a statement
	// TYPE_SS_SCROLL_DYNAMIC,
	// CONCUR_SS_OPTIMISTIC_CC, executeQuery
	// verify cursor by using next and previous and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtSS_ScrollDynamicOptimistic_CC() throws SQLException {

		try {
			int TYPE_SS_SCROLL_DYNAMIC = 1006;
			int CONCUR_SS_OPTIMISTIC_CC = 1008;
			stmt = conn().createStatement(TYPE_SS_SCROLL_DYNAMIC, CONCUR_SS_OPTIMISTIC_CC);

			// SELECT * FROM <table> ORDER BY <c1>, <c2> ASC|DESC
			String c1 = Tables.primaryKey();
			String c2 = "[c1_char(512)]";

			Tables.select(table1);
			Tables.orderby(c1);
			Tables.orderby(c2);

			rs = stmt.executeQuery(Tables.query);
			bvt_rs = new bvt_ResultSet(rs);

			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.afterLast();
			bvt_rs.previous();
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Create a statement
	// TYPE_SS_SEVER_CURSOR_FORWARD_ONLY,
	// CONCUR_READ_ONLY, executeQuery
	// verify cursor by using next and verify data
	///////////////////////////////////////////////////////////////////
	@Test
	public void testStmtSS_SEVER_CURSOR_FORWARD_ONLY() throws SQLException {

		try {
			int TYPE_SS_SEVER_CURSOR_FORWARD_ONLY = 2004;
			int CONCUR_READ_ONLY = 1008;
			stmt = conn().createStatement(TYPE_SS_SEVER_CURSOR_FORWARD_ONLY, CONCUR_READ_ONLY);

			String c1 = Tables.primaryKey();
			String c2 = "[c1_char(512)]";

			Tables.select(table1);
			Tables.orderby(c1);
			Tables.orderby(c2);

			rs = stmt.executeQuery(Tables.query);
			bvt_rs = new bvt_ResultSet(rs);

			// Verify resultset behavior
			bvt_rs.next();
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}

	}

	///////////////////////////////////////////////////////////////////
	// Create a preparedstatement, call close
	///////////////////////////////////////////////////////////////////
	@Test
	public void testCreatepreparedStatement() throws SQLException {

		try {
			String pk = Tables.primaryKey();
			String query = "SELECT * from " + table1 + " where c30_smallmoney = ? order by " + pk;

			pstmt = (SQLServerPreparedStatement) conn().prepareStatement(query);
			pstmt.setSmallMoney(1, new BigDecimal("214748.3647"));

			rs = pstmt.executeQuery();
			bvt_rs = new bvt_ResultSet(rs);
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Verify resultset using ResultSetMetaData
	///////////////////////////////////////////////////////////////////
	@Test
	public void testResultSet() throws SQLException {

		try {
			stmt = conn().createStatement();
			// SELECT * FROM <rs_test_bvt> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table2, pk);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// verify resultSet
			bvt_rs.verify();
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Verify resultset and close resultSet
	///////////////////////////////////////////////////////////////////
	@Test
	public void testResultSetAndClose() throws SQLException {

		try {
			stmt = conn().createStatement();

			// SELECT * FROM <table2> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table2, pk);
			rs = stmt.executeQuery(query);
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Verify two concurrent resultsets from same connection,
	// separate statements
	///////////////////////////////////////////////////////////////////
	@Test
	public void testTwoResultsetsDifferentStmt() throws SQLException {

		Statement stmt1 = null;
		Statement stmt2 = null;
		ResultSet rs1 = null;
		ResultSet rs2 = null;
		bvt_ResultSet bvt_rs1 = null;
		bvt_ResultSet bvt_rs2 = null;
		try {
			stmt1 = conn().createStatement();
			stmt2 = conn().createStatement();

			// SELECT * FROM <table2> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table2, pk);
			rs1 = stmt1.executeQuery(query);

			String query2 = Tables.select_Orderby(table1, pk);
			rs2 = stmt2.executeQuery(query2);
			bvt_rs1 = new bvt_ResultSet(rs1);
			bvt_rs2 = new bvt_ResultSet(rs2);

			// Interleave resultset calls
			bvt_rs1.next();
			bvt_rs1.verifyCurrentRow();
			bvt_rs2.next();
			bvt_rs2.verifyCurrentRow();
			bvt_rs1.next();
			bvt_rs1.verifyCurrentRow();
			bvt_rs1.verify();
			bvt_rs1.close();
			bvt_rs2.next();
			bvt_rs2.verify();
		} finally {
			if (null != bvt_rs2) {
				bvt_rs2.close();
			}
			if (null != rs1) {
				rs1.close();
			}
			if (null != rs2) {
				rs2.close();
			}
			if (null != stmt1) {
				stmt1.close();
			}
			if (null != stmt2) {
				stmt2.close();
			}
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Verify two concurrent resultsets from same connection,
	// same statement
	///////////////////////////////////////////////////////////////////
	@Test
	public void testTwoResultsetsSameStmt() throws SQLException {

		ResultSet rs1 = null;
		ResultSet rs2 = null;
		try {
			stmt = conn().createStatement();

			// SELECT * FROM <table2> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table2, pk);
			rs1 = stmt.executeQuery(query);

			// SELECT * FROM <table1> ORDER BY <key>
			String query2 = Tables.select_Orderby(table1, pk);
			rs2 = stmt.executeQuery(query2);

			bvt_ResultSet bvt_rs1 = new bvt_ResultSet(rs1);
			bvt_ResultSet bvt_rs2 = new bvt_ResultSet(rs2);

			// Interleave resultset calls. rs is expected to be closed
			try {
				bvt_rs1.next();
			} catch (SQLException e) {
				assertEquals(e.toString(),
						"com.microsoft.sqlserver.jdbc.SQLServerException: The result set is closed.");
			}
			bvt_rs2.next();
			bvt_rs2.verifyCurrentRow();
			try {
				bvt_rs1.next();
			} catch (SQLException e) {
				assertEquals(e.toString(),
						"com.microsoft.sqlserver.jdbc.SQLServerException: The result set is closed.");
			}
			bvt_rs1.close();
			bvt_rs2.next();
			bvt_rs2.verify();
		} finally {
			if (null != rs1) {
				rs1.close();
			}
			if (null != rs2) {
				rs2.close();
			}
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Verify resultset closed after statement is closed
	///////////////////////////////////////////////////////////////////
	@Test
	public void testResultSetAndCloseStmt() throws SQLException {
		try {
			stmt = conn().createStatement();

			// SELECT * FROM <table2> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table2, pk);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// close statement and verify resultSet
			stmt.close(); // this should close the resultSet
			try {
				bvt_rs.next();
			} catch (SQLException e) {
				assertEquals(e.toString(),
						"com.microsoft.sqlserver.jdbc.SQLServerException: The result set is closed.");
			}
		} finally {
			terminateVariation();
		}
	}

	///////////////////////////////////////////////////////////////////
	// Verify resultset using SelectMethod
	///////////////////////////////////////////////////////////////////
	@Test
	public void testResultSetSelectMethod() throws SQLException {

		// guarantees selectMethod=cursor
		cursor = true;
		try {
			stmt = conn().createStatement();

			// SELECT * FROM <table2> ORDER BY <key>
			String pk = Tables.primaryKey();
			String query = Tables.select_Orderby(table2, pk);
			rs = stmt.executeQuery(query);
			bvt_rs = new bvt_ResultSet(rs);

			// verify resultSet
			bvt_rs.verify();
			cursor = false;
		} finally {
			terminateVariation();
		}
	}

	@AfterClass
	public static void terminate() throws SQLException {

		try {
			stmt = conn().createStatement();
			stmt.executeUpdate(Tables.dropTable(table2));
			stmt.executeUpdate(Tables.dropTable(table1));
		} finally {
			terminateVariation();
		}
	}

	public static String getConnectionURL() {

		// Create a variable for the connection string.
		connectionUrl = "jdbc:sqlserver://" + serverName + ":" + portNumber + ";" + "databaseName=" + databaseName
				+ ";username=" + username + ";password=" + password + ";";

		if (cursor)
			connectionUrl += "selectMethod=cursor;";

		if (querytimeout)
			connectionUrl += "queryTimeout=10";
		
		return connectionUrl;
	}

	public static void readFromConfig() {

		try (BufferedReader in = new BufferedReader(new FileReader("src/test/serverConfig.cfg"));) {

			line = in.readLine();
			String[] parts = line.split("=");
			serverName = parts[1].trim();

			line = in.readLine();
			parts = line.split("=");
			portNumber = parts[1].trim();

			line = in.readLine();
			parts = line.split("=");
			databaseName = parts[1].trim();

			line = in.readLine();
			parts = line.split("=");
			username = parts[1].trim();

			line = in.readLine();
			parts = line.split("=");
			password = parts[1].trim();

		} catch (Exception e) {
			fail(e.toString());
		}
	}

	public static Connection conn() {

		connectionUrl = getConnectionURL();
		// Establish the connection.
		try {

			Connection con = DriverManager.getConnection(connectionUrl);
			return con;
			// Handle any errors that may have occurred.
		} catch (SQLException e) {
			fail("Please make sure the serverConfig.cfg file is updated with correct connection properties.\n" 
					+ e.toString());
		}
		return null;
	}

	public static void terminateVariation() throws SQLException {
		if (con != null && !con.isClosed()) {
			try {
				con.close();
			} catch (SQLException e) {
				fail("Connection close threw : " + e.toString());
			} finally {
				if (null != bvt_rs)
					bvt_rs.close();
				if (null != rs)
					rs.close();
				if (null != stmt)
					stmt.close();
				if (null != pstmt)
					pstmt.close();
			}
		}
	}

}
