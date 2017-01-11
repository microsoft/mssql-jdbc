// ---------------------------------------------------------------------------------------------------------------------------------
// File: SqlDecimal.java
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
import java.math.RoundingMode;
import java.sql.JDBCType;
import java.util.concurrent.ThreadLocalRandom;

public class SqlDecimal extends SqlType {

    // TODO:add overloaded consturtcor to avoid resetting scale and preccision
    public SqlDecimal() {
        this("decimal", JDBCType.DECIMAL);
    }

    // called for decimal and numeric type
    SqlDecimal(String name, JDBCType jdbctype) {
        this(name, 
                jdbctype, 
                38,	// precision
                0,	// scale
                SqlTypeValue.DECIMAL.minValue, 
                SqlTypeValue.DECIMAL.maxValue, 
                VariableLengthType.Scale);
    }

    // called from money/smallmoney
    SqlDecimal(String name, int precision, int scale, Object min, Object max, VariableLengthType variableLengthType) {
        this(name, JDBCType.DECIMAL, precision, scale, min, max, variableLengthType);
    }

    SqlDecimal(String name, JDBCType jdbctype, int precision, int scale, Object min, Object max, VariableLengthType variableLengthType) {
        super(name, jdbctype, precision, scale, min, max, SqlTypeValue.DECIMAL.nullValue, variableLengthType);

        // update random precision and scale
        generatePrecision();

        int minScale = 0;
        int maxScale = this.precision;

        // for money and smallmoney, valid max scale should be smallest of (4 or
        // precision)
        if (0 != this.scale) {
            maxScale = (this.scale <= this.precision) ? this.scale : this.precision;
        }

        this.scale = ThreadLocalRandom.current().nextInt(minScale, maxScale + 1);
    }

    public Object createdata() {

        double lowerBound = 0;
        double upperBound = 1;
        /**
         * value to add for Math.random() to include upperBound - to choose random value between 0 to 1 (inclusive of both)
         */
        double incrementValue = 0.1d;

        Boolean inValidData = true;
        BigDecimal randomValue = null;
        while (inValidData) {
            randomValue = new BigDecimal(ThreadLocalRandom.current().nextDouble(lowerBound, upperBound + incrementValue));
            Boolean isNegative = (0 == ThreadLocalRandom.current().nextInt(2)) ? true : false;

            // Restrict the BigInteger to the length of precision
            // i.e., if the precision is say 5, then get unscaledRandom%10^5
            if (randomValue.compareTo(new BigDecimal("1")) >= 0) {
                randomValue = randomValue.movePointRight(precision - scale - 1);
            }
            else {
                randomValue = randomValue.movePointRight(precision - scale);
            }
            randomValue = randomValue.setScale(scale, RoundingMode.FLOOR);

            randomValue = (isNegative) ? randomValue.multiply(new BigDecimal("-1")) : randomValue;

            // must be 0 or -ve
            int exceedsMax = randomValue.compareTo((BigDecimal) maxvalue);

            // must be 0 or +ve
            int exceedsMin = randomValue.compareTo((BigDecimal) minvalue);

            // recursion if data generated is < than min accepted value or >
            // than max bigdecimal
            if (!((exceedsMin < 0) || (exceedsMax > 0))) {
                inValidData = false;
            }
        }

        return randomValue;
    }
}