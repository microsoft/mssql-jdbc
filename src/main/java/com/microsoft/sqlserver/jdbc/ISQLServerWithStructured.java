//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerWithStructured.java
//
// Nayana Hettiarachchi nayana.hettiarachchi@agoda.com  
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;

import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * interface added for better testability
 */

public interface ISQLServerWithStructured extends PreparedStatement, ISQLServerStatement
{
    public void setStructured(int n, String tvpName, SQLServerDataTable tvpDataTbale) throws SQLServerException;

    public void setStructured(int n, String tvpName, ResultSet tvpResultSet) throws SQLServerException;

    public void setStructured(int n, String tvpName, ISQLServerDataRecord tvpBulkRecord) throws SQLServerException;
}

