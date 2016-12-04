/**
 * File Name: AbstractTest.java 
 * Created : Dec 3, 2016
 *
 * Microsoft JDBC Driver for SQL Server
 * The MIT License (MIT)
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files (the "Software"),
 * to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
 * and/or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions:
 *  
 * The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR 
 * ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH 
 * THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE. 
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
 * 
 * @author Microsoft
 *
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
