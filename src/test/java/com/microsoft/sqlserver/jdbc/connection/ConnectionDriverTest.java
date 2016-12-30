package com.microsoft.sqlserver.jdbc.connection;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.sql.DriverPropertyInfo;
import java.util.Properties;

import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.testframework.AbstractTest;

@RunWith(JUnitPlatform.class)
public class ConnectionDriverTest extends AbstractTest {
	@Test
	public void testConnectionDriver() throws SQLServerException {
		SQLServerDriver d = new SQLServerDriver();
		Properties info = new Properties();
		StringBuffer url = new StringBuffer();
		url.append("jdbc:sqlserver://abcdefg;packetSize=512;");
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
}
