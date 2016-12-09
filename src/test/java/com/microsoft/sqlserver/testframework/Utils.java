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

/**
 * Generic Utility class which we can access by test classes. 
 * @since 6.1.2 
 */
public class Utils {

	/**
	 * Read variable from property files if found null try to read from env.
	 *  
	 * @param key
	 * @return Value
	 */
	public static String getConfiguredProperty(String key) {
		String value = System.getProperty(key);
		
		if(value == null) {
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
		
		if(value == null) {
			value = defaultValue;
		}
		
		return value;
	}
}
