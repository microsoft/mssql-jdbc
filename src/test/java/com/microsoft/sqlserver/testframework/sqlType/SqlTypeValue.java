// ---------------------------------------------------------------------------------------------------------------------------------
// File: SqlTypeValue.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

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
	DATETIME		("17530101T00:00:00.000", 							    "99991231T23:59:59.997", 									null),
	DATE			("00010101",											"99991231",												null),
	TIME			("00:00:00.0000000", 									"23:59:59.9999999", 										null),
	SMALLDATETIME	("19000101T00:00:00",									"20790606T23:59:59",										null),
	DATETIME2		("00010101T00:00:00.0000000",							"99991231T23:59:59.9999999", 								null),
	DATETIMEOFFSET	("00010101T00:00:00.0000000-1400",					    "99991231T23:59:59.9999999+1400",							null),
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
