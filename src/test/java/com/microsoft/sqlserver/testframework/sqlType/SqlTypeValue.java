/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

import java.math.BigDecimal;

/*
 * Maps SQL type to its minimum, maximum and null value
 * 
 * temporal min/max values used are not DATEFORMAT dependent as in https://msdn.microsoft.com/en-us/library/ms180878.aspx
 */
enum SqlTypeValue {
					// minValue												// maxValue													// nullValue
	BIGINT			(new Long(Long.MIN_VALUE), 								new Long(Long.MAX_VALUE), 									new Long(0)),
	INTEGER			(new Integer(Integer.MIN_VALUE), 						new Integer(Integer.MAX_VALUE), 							new Integer(0)),
	SMALLINT		(new Short(Short.MIN_VALUE), 							new Short(Short.MAX_VALUE), 								new Short((short) 0)),
	TINYINT			(new Short((short) 0), 									new Short((short) 255), 									new Short((short) 0)),
	BIT				(0, 													1, 															null),
	DECIMAL			(new BigDecimal("-1.0E38").add(new BigDecimal("1")), 	new BigDecimal("1.0E38").subtract(new BigDecimal("1")), 	null),
	MONEY			(new BigDecimal("-922337203685477.5808"), 				new BigDecimal("+922337203685477.5807"), 					null),
	SMALLMONEY		(new BigDecimal("-214748.3648"), 						new BigDecimal("214748.3647"), 								null),
	FLOAT			(new Double(-1.79E308), 								new Double(+1.79E308), 										new Double(0)),
	REAL			(new Float(-3.4E38), 									new Float(+3.4E38), 										new Float(0)),
	CHAR			(null, 													null, 														null),// CHAR used by char, nchar, varchar, nvarchar
	BINARY          (null,                                                  null,                                                       null),
	DATETIME		("17530101T00:00:00.000", 							    "99991231T23:59:59.997", 									null),
	DATE			("00010101",											"99991231",												    null),
	TIME			("00:00:00.0000000", 									"23:59:59.9999999", 										null),
	SMALLDATETIME	("19000101T00:00:00",									"20790606T23:59:59",										null),
	DATETIME2		("00010101T00:00:00.0000000",							"99991231T23:59:59.9999999", 								null),
	DATETIMEOFFSET	("0001-01-01 00:00:00",					                "9999-12-31 23:59:59",						null),
	;
   
    Object minValue;
    Object maxValue;
    Object nullValue;

    SqlTypeValue(Object minValue, Object maxValue, Object nullValue) {
        this.minValue = minValue;
        this.maxValue = maxValue;
        this.nullValue = nullValue;
    }
}
