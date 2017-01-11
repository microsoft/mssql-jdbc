/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

/**
 * Utility class for Strings.
 * 
 * @since 6.1.2
 */
public class StringUtils {

	public static final String SPACE = " ";

	public static final String EMPTY = "";
	
	/**
	 * Developer should not create {@code StringUtils} instance in standard programming. 
	 * Instead, the class should be used as {@code StringUtils.isEmpty("String")} 
	 */
	private StringUtils() {
		// Hiding constructor.
	}

	/**
	 * Checks if String is null 
	 * @param charSequence {@link CharSequence} Can provide null
	 * @return {@link Boolean} if provided char sequence is null or empty / blank
	 * @since 6.1.2
	 */
	public static boolean isEmpty(final CharSequence charSequence) {
		return charSequence == null || charSequence.length() == 0;
	}
	
}
