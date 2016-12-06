/**
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework.util;

import java.util.UUID;
import java.util.concurrent.ThreadLocalRandom;

/**
 * @author Microsoft
 *
 */
public class RandomUtil {

	/**
	 * 
	 * @param prefix		Prefix
	 * @param maxLength		max length
	 * @param unique		Includes UUID.	
	 * @param isDatabase	Do you want for db name. 
	 * @return
	 */
	public String getIdentifier(String prefix, int maxLength, boolean unique, boolean isDatabase) {
		String identifier;
		StringBuilder sb = new StringBuilder();
		sb.append(prefix);
		sb.append("_");
		sb.append("jdbc_");
		sb.append(System.getProperty("user.name"));
		sb.append("_");
		if (unique) {
			//Create UUID.
			sb.append(UUID.randomUUID().toString());
		}

		identifier = sb.toString();

		if (maxLength < identifier.length()) {
			identifier = identifier.substring(0, maxLength);
		}
		return identifier;
	}

	/**
	 * Get Identifier for DB Name. 
	 * @param prefix		Prefix
	 * @param maxLength		max length
	 * @param unique		Includes UUID.	
	 * @return
	 */
	public String getIdentifierForDB(String prefix, int maxLength, boolean unique) {
		String identifier = getIdentifier(prefix, maxLength, unique, true);

		return removeInvalidDBChars(identifier);
	}
	
	/**
	 * 
	 * @param prefix		Prefix
	 * @param maxLength		max length
	 * @param isDatabase	Do you want for db name. 
	 * @return
	 */
	public String getUniqueIdentifier(String prefix, int maxLength, boolean isDatabase) {
		return getIdentifier(prefix, maxLength, true, isDatabase);
	}

	/**
	 * Remove Invalid DB Chars.  
	 * @param s
	 * @return
	 */
	private String removeInvalidDBChars(String s) {
		return s.replaceAll("[:-]", "_");
	}
	
	public Integer getMaxInteger() {
		return new Integer(Integer.MAX_VALUE);
	}
	
	public Integer getMinInteger() {
		return new Integer(Integer.MIN_VALUE);
	}
	
	/**
	 * Get Random int
	 * @param min
	 * @param max
	 * @return
	 */
	public int getRandomInt(int min, int max) {
		return ThreadLocalRandom.current().nextInt(min, max);
	}
	
	/**
	 * Get Random double
	 * @param min
	 * @param max
	 * @return
	 */
	public double getRandomDouble(double min, double max) {
		return ThreadLocalRandom.current().nextDouble(min, max);
	}
	
	/**
	 * Get Random long.
	 * @param min
	 * @param max
	 * @return
	 */
	public long getRandomLong(long min, long max) {
		return ThreadLocalRandom.current().nextLong(min, max);
	}
	
}
