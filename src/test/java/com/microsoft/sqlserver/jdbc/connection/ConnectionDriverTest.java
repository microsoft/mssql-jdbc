package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.Connection;
import java.sql.DriverPropertyInfo;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

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
		url.append("jdbc:sqlserver://"+randomServer+";packetSize=512;");
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
		ds.setURL("jdbc:sqlserver://"+randomServer+";packetSize=512");

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
}
