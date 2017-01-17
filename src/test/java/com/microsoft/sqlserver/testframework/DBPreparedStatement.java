// ---------------------------------------------------------------------------------------------------------------------------------
// File: DBPreparedStatement.java
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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * 
 * Wrapper class PreparedStatement 
 */
public class DBPreparedStatement extends AbstractParentWrapper {

    PreparedStatement pstmt = null;
    DBResultSet dbresultSet = null;

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBPreparedStatement(AbstractParentWrapper parent, Object internal, String name) {
        super(parent, internal, name);
    }

    /**
     * @throws SQLException
     * 
     */
    DBPreparedStatement prepareStatement(String query) throws SQLException {
        pstmt = ((Connection) product()).prepareStatement(query);
        setInternal(pstmt);
        return this;
    }

    @Override
    void setInternal(Object internal) {
        this.internal = internal;
    }

    /**
     * @param i
     * @param bigDecimal
     * @throws SQLException
     */
    public void setObject(int parameterIndex, Object targetObject) throws SQLException {

        ((PreparedStatement) product()).setObject(parameterIndex, targetObject);

    }

    /**
     * 
     * @return
     * @throws SQLException
     */
    public DBResultSet executeQuery() throws SQLException {
        ResultSet rs = null;
        rs = pstmt.executeQuery();
        dbresultSet = new DBResultSet(this, rs);
        return dbresultSet;

    }

}
