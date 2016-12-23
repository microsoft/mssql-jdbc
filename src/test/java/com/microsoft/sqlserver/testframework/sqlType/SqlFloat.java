//---------------------------------------------------------------------------------------------------------------------------------
// File: SqlFloat.java
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
 

package com.microsoft.sqlserver.testframework.sqlType;

import java.sql.JDBCType;
import java.util.concurrent.ThreadLocalRandom;

public class SqlFloat extends SqlType {

	// called from real
	SqlFloat(String name, JDBCType jdbctype, int precision, Object min, Object max, Object nullvalue, VariableLengthType variableLengthType) {
		super(name, jdbctype, precision, 0, min, max, nullvalue, variableLengthType);
		generatePrecision();
	}

	public SqlFloat() {
		super("float", 
				JDBCType.DOUBLE, 
				53,	//default precision
				0,	//scale
				SqlTypeValue.FLOAT.minValue,
				SqlTypeValue.FLOAT.maxValue,
				SqlTypeValue.FLOAT.nullValue, 
				VariableLengthType.Precision);
		generatePrecision();
	}

	public Object createdata() {
		//TODO: include max value
		if (precision > 24)
			return Double.longBitsToDouble(ThreadLocalRandom.current().nextLong(((Double) minvalue).longValue(), ((Double) maxvalue).longValue()));
		else
			return new Float(ThreadLocalRandom.current().nextDouble(new Float(-3.4E38), new Float(+3.4E38)));
	}
}