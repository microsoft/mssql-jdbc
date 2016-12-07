/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.sql.Connection;
import java.util.Properties;
import java.util.logging.Logger;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;

/**
 * Think about following things: 
 * <li>Connection pool
 * <li>Configured Property file instead of passing from args.
 * <li>Think of different property files for different settings. / flag etc.
 * <Li>Think about what kind of logging we are going use it. <B>util.logging<B> will be preference.
 */
public abstract class AbstractTest {

	protected static Logger logger = Logger.getLogger("AbstractTest");

	protected static String secretstrJks = "changeit";

	protected static String applicationClientID = null;
	protected static String applicationKey = null;
	protected static String[] keyIDs = null;

	protected static String[] jksPaths = null;
	protected static String[] javaKeyAliases = null;
	protected static String windowsKeyPath = null;

	protected static SQLServerConnection connection = null;
	protected static Connection connectionAzure = null;

	protected static String connectionString = null;

	protected static Properties info = new Properties();

	/**
	 * This will take care of all initialization before running the Test Suite.
	 * @throws Exception
	 */
	@BeforeAll
	public static void setup() throws Exception {
		//TODO: Here we want to read config.property file or depend upon app.jvmarg
		applicationClientID = System.getProperty("applicationClientID");
		applicationKey = System.getProperty("applicationKey");
		keyIDs = System.getProperty("keyID", "").split(";");

		connectionString = System.getProperty("connectionstring");

		jksPaths = System.getProperty("jksPaths", "").split(";");
		javaKeyAliases = System.getProperty("javaKeyAliases", "").split(";");
		windowsKeyPath = System.getProperty("windowsKeyPath");

		//		info.setProperty("ColumnEncryptionSetting", "Enabled"); // we should not use this instead we should come up with some methodology
		if (!jksPaths[0].isEmpty()) {
			info.setProperty("keyStoreAuthentication", "JavaKeyStorePassword");
			info.setProperty("keyStoreLocation", jksPaths[0]);
			info.setProperty("keyStoreSecret", secretstrJks);
		}
		logger.info("In AbstractTest:setup");

		try {
			Assertions.assertNotNull(connectionString, "Connection String should not be null");
			connection = PrepUtil.getConnection(connectionString, info);
		} catch (Exception e) {
			throw e;
		}
	}

	/**
	 * This will take care of all clean ups after running the Test Suite.
	 * @throws Exception
	 */
	@AfterAll
	public static void teardown() throws Exception {
		try {
			if (connection != null && !connection.isClosed()) {
				connection.close();
			}
		} catch (Exception e) {
			connection.close();
		} finally {
			connection = null;
		}
	}

}
