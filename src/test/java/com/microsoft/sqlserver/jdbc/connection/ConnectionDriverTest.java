package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.DriverPropertyInfo;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.sql.Statement;
import java.util.Properties;
import java.util.logging.Logger;

import javax.sql.ConnectionEvent;
import javax.sql.PooledConnection;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.ISQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;
import com.microsoft.sqlserver.testframework.util.RandomUtil;

@RunWith(JUnitPlatform.class)
public class ConnectionDriverTest extends AbstractTest {
	String randomServer = RandomUtil.getIdentifier("Server");

	@Test
	public void testConnectionDriver() throws SQLServerException {
		SQLServerDriver d = new SQLServerDriver();
		Properties info = new Properties();
		StringBuffer url = new StringBuffer();
		url.append("jdbc:sqlserver://" + randomServer + ";packetSize=512;");
		// test defaults 
		DriverPropertyInfo[] infoArray = d.getPropertyInfo(url.toString(), info);
		for (int i = 0; i < infoArray.length; i++) {
			logger.fine(infoArray[i].name);
			logger.fine(infoArray[i].description);
			logger.fine(new Boolean(infoArray[i].required).toString());
			logger.fine(infoArray[i].value);
		}

		// test SSL properties
		url.append("encrypt=true; trustStore=someStore; trustStorePassword=somepassword;");
		url.append("hostNameInCertificate=someHost; trustServerCertificate=true");
		infoArray = d.getPropertyInfo(url.toString(), info);
		for (int i = 0; i < infoArray.length; i++) {
			if (infoArray[i].name.equals("encrypt")) {
				assertTrue(infoArray[i].value.equals("true"), "Values are different");
			}
			if (infoArray[i].name.equals("trustStore")) {
				assertTrue(infoArray[i].value.equals("someStore"), "Values are different");
			}
			if (infoArray[i].name.equals("trustStorePassword")) {
				assertTrue(infoArray[i].value.equals("somepassword"), "Values are different");
			}
			if (infoArray[i].name.equals("hostNameInCertificate")) {
				assertTrue(infoArray[i].value.equals("someHost"), "Values are different");
			}
		}
	}

	@Test
	public void testDataSource() {
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setUser("User");
		ds.setPassword("sUser");
		ds.setApplicationName("User");
		ds.setURL("jdbc:sqlserver://" + randomServer + ";packetSize=512");

		String trustStore = "Store";
		String trustStorePassword = "pwd";

		ds.setTrustStore(trustStore);
		ds.setEncrypt(true);
		ds.setTrustStorePassword(trustStorePassword);
		ds.setTrustServerCertificate(true);
		assertEquals(trustStore, ds.getTrustStore(), "Values are different");
		assertEquals(true, ds.getEncrypt(), "Values are different");
		assertEquals(true, ds.getTrustServerCertificate(), "Values are different");
	}

	@Test
	public void testEncryptedConnection() throws SQLException {
		SQLServerDataSource ds = new SQLServerDataSource();
		ds.setApplicationName("User");
		ds.setURL(connectionString);
		ds.setEncrypt(true);
		ds.setTrustServerCertificate(true);
		ds.setPacketSize(8192);
		Connection con = ds.getConnection();
		con.close();
	}

	@Test
	public void testJdbc41DriverMethod() throws SQLFeatureNotSupportedException {
		SQLServerDriver serverDriver = new SQLServerDriver();
		Logger logger = serverDriver.getParentLogger();
		assertEquals(logger.getName(), "com.microsoft.sqlserver.jdbc", "Parent Logger name is wrong");
	}

	@Test
	public void testJdbc41DataSourceMethod() throws SQLFeatureNotSupportedException {
		SQLServerDataSource fxds = new SQLServerDataSource();
		Logger logger = fxds.getParentLogger();
		assertEquals(logger.getName(), "com.microsoft.sqlserver.jdbc", "Parent Logger name is wrong");
	}

	class MyEventListener implements javax.sql.ConnectionEventListener {
		boolean connClosed = false;
		boolean errorOccurred = false;

		public MyEventListener() {
		}

		public void connectionClosed(ConnectionEvent event) {
			connClosed = true;
		}

		public void connectionErrorOccurred(ConnectionEvent event) {
			errorOccurred = true;
		}
	}

	@Test
	public void testConnectionEvents() throws Exception {
		SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
		mds.setURL(connectionString);
		PooledConnection pooledConnection = mds.getPooledConnection();

		//Attach the Event listener and listen for connection events.
		MyEventListener myE = new MyEventListener();
		pooledConnection.addConnectionEventListener(myE);	// ConnectionListener implements ConnectionEventListener
		Connection con = pooledConnection.getConnection();

		Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

		boolean exceptionThrown = false;
		try {
			// raise a severe exception and make sure that the connection is not closed.
			stmt.executeUpdate("RAISERROR ('foo', 20,1) WITH LOG");
		} catch (Exception e) {
			exceptionThrown = true;
		}
		assertTrue(exceptionThrown, "Expected exception is not thrown.");

		// Check to see if error occurred.
		assertTrue(myE.errorOccurred, "Error occurred is not called.");
		//make sure that connection is closed.
		assertTrue(con.isClosed(), "Connection is not closed.");
	}

	@Test
	public void testConnectionPoolGetTwice() throws Exception {
		SQLServerConnectionPoolDataSource mds = new SQLServerConnectionPoolDataSource();
		mds.setURL(connectionString);
		PooledConnection pooledConnection = mds.getPooledConnection();

		//Attach the Event listener and listen for connection events.
		MyEventListener myE = new MyEventListener();
		pooledConnection.addConnectionEventListener(myE);	// ConnectionListener implements ConnectionEventListener

		Connection con = pooledConnection.getConnection();
		Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

		// raise a non severe exception and make sure that the connection is not closed.
		stmt.executeUpdate("RAISERROR ('foo', 3,1) WITH LOG");

		// not a serious error there should not be any errors.
		assertTrue(!myE.errorOccurred, "Error occurred is called.");
		// check to make sure that connection is not closed.
		assertTrue(!con.isClosed(), "Connection is closed.");

		con.close();
		//check to make sure that connection is closed.
		assertTrue(con.isClosed(), "Connection is not closed.");
	}

	@Test
	public void testConnectionClosed() throws Exception {
		SQLServerDataSource mds = new SQLServerDataSource();
		mds.setURL(connectionString);
		Connection con = mds.getConnection();
		// System.out.println(con.getCatalog());
		Statement stmt = con.createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);

		boolean exceptionThrown = false;
		try {
			stmt.executeUpdate("RAISERROR ('foo', 20,1) WITH LOG");
		} catch (Exception e) {
			exceptionThrown = true;
		}
		assertTrue(exceptionThrown, "Expected exception is not thrown.");

		// check to make sure that connection is closed.
		assertTrue(con.isClosed(), "Connection is not closed.");
	}

	@Test
	public void testIsWrapperFor() throws Exception {
		Connection conn = DriverManager.getConnection(connectionString);
		SQLServerConnection ssconn = (SQLServerConnection) conn;
		boolean isWrapper;
		try {
			isWrapper = ssconn.isWrapperFor(ssconn.getClass());
			assertTrue(isWrapper, "SQLServerConnection supports unwrapping");
			assertEquals(ssconn.TRANSACTION_SNAPSHOT, ssconn.TRANSACTION_SNAPSHOT, "Cant access the TRANSACTION_SNAPSHOT ");

			isWrapper = ssconn.isWrapperFor(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection"));
			assertTrue(isWrapper, "ISQLServerConnection supports unwrapping");
			ISQLServerConnection iSql = (ISQLServerConnection) ssconn.unwrap(Class.forName("com.microsoft.sqlserver.jdbc.ISQLServerConnection"));
			assertEquals(iSql.TRANSACTION_SNAPSHOT, iSql.TRANSACTION_SNAPSHOT, "Cant access the TRANSACTION_SNAPSHOT ");

			ssconn.unwrap(Class.forName("java.sql.Connection"));
		} catch (UnsupportedOperationException e) {
			assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
		}

		conn.close();
	}

	@Test
	public void testNewConnection() throws Exception {
		SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
		try {
			assertTrue(conn.isValid(0), "Newly created connection should be valid");
		} catch (UnsupportedOperationException e) {
			assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
		}

		conn.close();
	}

	@Test
	public void testClosedConnection() throws Exception {
		SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
		try {
			conn.close();
			assertTrue(!conn.isValid(0), "Closed connection should be invalid");
		} catch (UnsupportedOperationException e) {
			assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
		}
	}
	
	@Test
	public void testNegativeTimeout() throws Exception 
	{
		Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
		SQLServerConnection conn = (SQLServerConnection) DriverManager.getConnection(connectionString);
		try 
		{
			conn.isValid(-42);
			throw new Exception("No exception thrown with negative timeout");
		} 
		catch (SQLException e)
		{
			assertEquals(e.getMessage(), "The query timeout value -42 is not valid.", "Wrong exception message");
		} 
		catch (UnsupportedOperationException e) 
		{
			assertEquals(e.getMessage(), "This operation is not supported.", "Wrong exception message");
		}

		conn.close();
	}
}
