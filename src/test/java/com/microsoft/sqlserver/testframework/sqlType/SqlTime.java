//---------------------------------------------------------------------------------------------------------------------------------
// File: SqlTime.java
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
import java.sql.Time;
import java.text.SimpleDateFormat;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.temporal.ChronoField;
import java.util.concurrent.ThreadLocalRandom;

import org.apache.commons.lang3.RandomStringUtils;

public class SqlTime extends SqlDateTime {

	static SimpleDateFormat dateFormat = new SimpleDateFormat("HH:mm:ss.SSSSSSS");
	static String basePattern = "HH:mm:ss";
	static DateTimeFormatter formatter;

	public SqlTime() throws Exception {
		super("time", 
				JDBCType.TIME, 
				new Time(dateFormat.parse((String)SqlTypeValue.TIME.minValue).getTime()), 
				new Time(dateFormat.parse((String)SqlTypeValue.TIME.maxValue).getTime())
				);
		this.precision = 7;
		this.variableLengthType = VariableLengthType.Precision;
		// should we let SQL server handle scale for temporal types?
		// java just has millisecond precision anyways
		generatePrecision();
		formatter = new DateTimeFormatterBuilder().appendPattern(basePattern)
			        .appendFraction(ChronoField.NANO_OF_SECOND, 0, this.precision, true).toFormatter();
		
	}

	public Object createdata() {
		Time temp = new Time(ThreadLocalRandom.current().nextLong(((Time) minvalue).getTime(), ((Time) maxvalue).getTime()));
		String timeNano = temp.toString() +"." +RandomStringUtils.randomNumeric(this.precision);
		// can pass String rather than converting to loacTime, but leaving it unchanged for now to handle prepared statements
		return LocalTime.parse(timeNano, formatter);
	}
}