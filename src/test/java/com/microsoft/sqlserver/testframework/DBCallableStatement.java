/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;

/**
 * Wrapper class CallableStatement
 *
 */
public class DBCallableStatement extends AbstractParentWrapper{
    
    PreparedStatement cstmt = null;

    /**
     * 
     */
    public DBCallableStatement(DBConnection dbconnection) {
        super(dbconnection, null, "preparedStatement");
    }
    
    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBCallableStatement(AbstractParentWrapper parent,
            Object internal,
            String name) {
        super(parent, internal, name);
        // TODO Auto-generated constructor stub
    }
    
    DBCallableStatement prepareCall(String query) throws SQLException {
        cstmt = ((Connection) parent().product()).prepareCall(query);
        setInternal(cstmt);
        return this;
    }
    
    /**
     * 
     * @param x
     * @param y
     * @param z
     * @throws SQLException
     */
    public void registerOutParameter(String x, int y, int z) throws SQLException
    {
       //product
       ((CallableStatement)product()).registerOutParameter(x, y, z);
    }
    
    /**
     * 
     * @param index
     * @param sqltype
     * @throws SQLException
     */
    public void registerOutParameter(int index, int sqltype) throws SQLException
    {
       ((CallableStatement)product()).registerOutParameter(index, sqltype);

    }

}