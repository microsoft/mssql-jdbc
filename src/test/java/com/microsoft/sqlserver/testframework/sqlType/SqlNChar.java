//---------------------------------------------------------------------------------------------------------------------------------
// File: SqlNChar.java
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

import org.apache.commons.lang.StringEscapeUtils;

public class SqlNChar extends SqlChar {

	SqlNChar(String name, JDBCType jdbctype, int precision) {
		super(name, jdbctype, precision);
	}

	public SqlNChar() {
		this("nchar", JDBCType.NCHAR, 2000);
	}

	public Object createdata() {
		int dataLength = ThreadLocalRandom.current().nextInt(precision);
		/*
		 Just supporting Latin character sets for now,
		 as the entire valid code point Character.MIN_CODE_POINT to  Character.MAX_CODE_POINT has many unassigned code points
		 CodePoints Used:
				Basic Latin
				Latin-1 Supplement
			    Latin Extended-A
			    Latin Extended-B
		*/
		int minCodePoint = 0x000;
		int maxCodePoint = 0x24F;
		StringBuilder sb = new StringBuilder();
		for (int i = 0; i < dataLength; i++) {
			// TODO: need to remove uassigned 
			int rand = ThreadLocalRandom.current().nextInt(minCodePoint, maxCodePoint);
			char c = (char) rand;
			sb.append(c);
		}
		return StringEscapeUtils.escapeSql(sb.toString());
	}
}