// ---------------------------------------------------------------------------------------------------------------------------------
// File: SqlDateTime.java
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

import static org.junit.jupiter.api.Assertions.fail;

import java.sql.JDBCType;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

public class SqlDateTime extends SqlType {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd'T'HH:mm:ss.SSS");

    public SqlDateTime() {
        this("datetime", JDBCType.TIMESTAMP, null, null);
        try {
            minvalue = new Timestamp(
                    dateFormat.parse((String) SqlTypeValue.DATETIME.minValue).getTime());
            maxvalue = new Timestamp(
                    dateFormat.parse((String) SqlTypeValue.DATETIME.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
    }

    SqlDateTime(String name, JDBCType jdbctype, Object min, Object max) {
        super(name, jdbctype, 0, 0, min, max, SqlTypeValue.DATETIME.nullValue,
                VariableLengthType.Fixed);
    }

    public Object createdata() {
        return new Timestamp(ThreadLocalRandom.current().nextLong(((Timestamp) minvalue).getTime(),
                ((Timestamp) maxvalue).getTime()));
    }
}