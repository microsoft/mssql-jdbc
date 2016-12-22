//---------------------------------------------------------------------------------------------------------------------------------
// File: AbstractTest.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

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

		applicationClientID = getConfiguredProperty("applicationClientID");
		applicationKey = getConfiguredProperty("applicationKey");
		keyIDs = getConfiguredProperty("keyID", "").split(";");

		connectionString = getConfiguredProperty("mssql_jdbc_test_connection_properties");

		jksPaths = getConfiguredProperty("jksPaths", "").split(";");
		javaKeyAliases = getConfiguredProperty("javaKeyAliases", "").split(";");
		windowsKeyPath = getConfiguredProperty("windowsKeyPath");

		//		info.setProperty("ColumnEncryptionSetting", "Enabled"); // May be we can use parameterized way to change this value
		if (!jksPaths[0].isEmpty()) {
			info.setProperty("keyStoreAuthentication", "JavaKeyStorePassword");
			info.setProperty("keyStoreLocation", jksPaths[0]);
			info.setProperty("keyStoreSecret", secretstrJks);
		}
		logger.info("In AbstractTest:setup");

		try {
			Assertions.assertNotNull(connectionString, "Connection String should not be null");
			//TODO: use DBConnection to getConnenction
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

	/**
	 * Read variable from property files if found null try to read from env.
	 *  
	 * @param key
	 * @return Value
	 */
	public static String getConfiguredProperty(String key) {
		String value = System.getProperty(key);

		if (value == null) {
			value = System.getenv(key);
		}

		return value;
	}

	/**
	 * Convenient  method for {@link #getConfiguredProperty(String)}
	 *  
	 * @param key
	 * @return Value
	 */
	public static String getConfiguredProperty(String key, String defaultValue) {
		String value = getConfiguredProperty(key);

		if (value == null) {
			value = defaultValue;
		}

		return value;
	}
}
