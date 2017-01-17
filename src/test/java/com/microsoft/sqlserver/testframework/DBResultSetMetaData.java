// ---------------------------------------------------------------------------------------------------------------------------------
// File: DBResultSetMetaData.java
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

package com.microsoft.sqlserver.testframework;

import static org.junit.Assert.fail;

import java.sql.SQLException;

import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData;

/**
 * 
 *
 */
public class DBResultSetMetaData extends AbstractParentWrapper {

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBResultSetMetaData(AbstractParentWrapper parent, Object internal, String name) {
        super(parent, internal, name);
        // TODO Auto-generated constructor stub
    }

    /**
     * 
     * @throws SQLException
     */
    public void verify() throws SQLException {
        // getColumnCount
        int columns = this.getColumnCount();

        // Loop through the columns
        for (int i = 1; i <= columns; i++) {
            // Note: Just calling these performs the verification, in each method
            this.getColumnName(i);
            this.getColumnType(i);
            this.getColumnTypeName(i);
            this.getScale(i);
            this.isCaseSensitive(i);
            this.isAutoIncrement(i);
            this.isCurrency(i);
            this.isNullable(i);
            this.isSigned(i);
        }
    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public int getColumnCount() throws SQLException {
        int current = 0;
        try {
            current = ((SQLServerResultSetMetaData) product()).getColumnCount();
        }
        catch (SQLException e) {
            fail(e.toString());
        }
        return current;
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public String getColumnName(int index) throws SQLException {
        String current = ((SQLServerResultSetMetaData) product()).getColumnName(index);
        return current;
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public int getColumnType(int index) throws SQLException {
        int current = ((SQLServerResultSetMetaData) product()).getColumnType(index);
        return current;
    }

    /**
     * 
     * @param index
     * @return
     * @throws SQLException
     */
    public String getColumnTypeName(int index) throws SQLException {
        String current = ((SQLServerResultSetMetaData) product()).getColumnTypeName(index);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public int getPrecision(int x) throws SQLException {
        int current = ((SQLServerResultSetMetaData) product()).getPrecision(x);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public int getScale(int x) throws SQLException {
        int current = ((SQLServerResultSetMetaData) product()).getScale(x);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isCaseSensitive(int x) throws SQLException {
        boolean current = ((SQLServerResultSetMetaData) product()).isCaseSensitive(x);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isCurrency(int x) throws SQLException {
        boolean current = ((SQLServerResultSetMetaData) product()).isCurrency(x);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isAutoIncrement(int x) throws SQLException {
        boolean current = ((SQLServerResultSetMetaData) product()).isAutoIncrement(x);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public int isNullable(int x) throws SQLException {
        int current = ((SQLServerResultSetMetaData) product()).isNullable(x);
        return current;
    }

    /**
     * 
     * @param x
     * @return
     * @throws SQLException
     */
    public boolean isSigned(int x) throws SQLException {
        boolean current = ((SQLServerResultSetMetaData) product()).isSigned(x);
        return current;
    }
}
