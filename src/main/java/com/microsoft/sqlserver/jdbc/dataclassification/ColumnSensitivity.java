/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.dataclassification;

import java.util.ArrayList;
import java.util.List;

/**
 * This class is used to retrieve Sensitivity Information for columns as
 * configured in Database.
 */
public class ColumnSensitivity {
	private List<SensitivityProperty> sensitivityProperties;

	/**
	 * Constructor for ColumnSensitivity
	 * 
	 * @param sensitivityProperties
	 *            List of sensitivity properties as received from SQL Server
	 */
	public ColumnSensitivity(List<SensitivityProperty> sensitivityProperties) {
		this.sensitivityProperties = new ArrayList<SensitivityProperty>(
				sensitivityProperties);
	}

	/**
	 * Retrieves list of sensitivity properties as received from Server for this
	 * <code>ColumnSensitivity</code> information
	 * 
	 * @return sensitivityProperties for this Class Object
	 */
	public List<SensitivityProperty> getSensitivityProperties() {
		return sensitivityProperties;
	}
}
