// ---------------------------------------------------------------------------------------------------------------------------------
// File: SqlDate.java
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

import java.sql.Date;
import java.sql.JDBCType;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.concurrent.ThreadLocalRandom;

public class SqlDate extends SqlDateTime {

    static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd");

    public SqlDate() {
        super("date", JDBCType.DATE, null, null);
        try {
            minvalue = new Date(dateFormat.parse((String) SqlTypeValue.DATE.minValue).getTime());
            maxvalue = new Date(dateFormat.parse((String) SqlTypeValue.DATE.maxValue).getTime());
        }
        catch (ParseException ex) {
            fail(ex.getMessage());
        }
    }

    public Object createdata() {
        return new Date(ThreadLocalRandom.current().nextLong(((Date) minvalue).getTime(),
                ((Date) maxvalue).getTime()));
    }
}
