//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerConnectionPoolProxy.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;
import java.sql.*;
import java.util.*;
import java.util.logging.*;
import java.util.concurrent.Executor;
import java.text.*;

/**
* SQLServerConnectionPoolProxy is a wrapper around SQLServerConnection object.
* When returning a connection object from PooledConnection.getConnection we returnt this proxy per SPEC.
* <li>
* This class's public functions need to be kept identical to the SQLServerConnection's.
* <li>
* The API javadoc for JDBC API methods that this class implements are not repeated here. Please
* see Sun's JDBC API interfaces javadoc for those details.
*/

class SQLServerConnectionPoolProxy implements ISQLServerConnection, java.io.Serializable
{
        private SQLServerConnection wrappedConnection;
        private boolean bIsOpen;
        static private int baseConnectionID=0;       //connection id dispenser
        final private String traceID ;
        
        // Permission targets
        // currently only callAbort is implemented
    	private static final String callAbortPerm = "callAbort";
    	
	/**
	* Generate the next unique connection id.
	* @return the next conn id
	*/
	/*L0*/ private synchronized static int nextConnectionID() 
	{
		baseConnectionID++;
		return baseConnectionID;
	}

	public String toString() 
	{
		 return traceID;
	}

	/*L0*/ SQLServerConnectionPoolProxy(SQLServerConnection con) 
	{
	    traceID= " ProxyConnectionID:"  + nextConnectionID();
		wrappedConnection = con;
		// the Proxy is created with an open conn
		con.setAssociatedProxy(this);
 		bIsOpen  =true;
	}

   /*L0*/ void checkClosed() throws SQLServerException 
	{
		if (!bIsOpen) 
		{
			SQLServerException.makeFromDriverError(null, null, SQLServerException.getErrString("R_connectionIsClosed"), null, false);
		}
	}
	/*L0*/ public Statement createStatement() throws SQLServerException
	{
		checkClosed();
		return wrappedConnection.createStatement();
	}

	/*L0*/ public PreparedStatement prepareStatement(String sql) throws SQLServerException
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql);
	}

	/*L0*/ public CallableStatement prepareCall(String sql) throws SQLServerException
	{
		checkClosed();
		return wrappedConnection.prepareCall(sql);
	}

   /*L0*/ public String nativeSQL(String sql) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.nativeSQL( sql);
	}

	public void setAutoCommit(boolean newAutoCommitMode) throws SQLServerException
	{
		checkClosed();
		wrappedConnection.setAutoCommit(newAutoCommitMode);
	}

	/*L0*/ public boolean getAutoCommit() throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.getAutoCommit();
	}

	public void commit() throws SQLServerException
	{
		checkClosed();
		wrappedConnection.commit();
	}

	/**
	* Rollback a transcation.
	*
	* @throws SQLServerException if no transaction exists or if the connection is in auto-commit mode.
	*/
	public void rollback() throws SQLServerException
	{
		checkClosed();
		wrappedConnection.rollback();
	}

    public void abort(Executor executor) throws SQLException
    {
		DriverJDBCVersion.checkSupportsJDBC41();

		if (!bIsOpen || (null == wrappedConnection))
			return;	
		
		if (null == executor)
		{
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidArgument"));
            Object[] msgArgs = {"executor"};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, false);
        }
	        
        // check for callAbort permission
        SecurityManager secMgr = System.getSecurityManager();
        if (secMgr != null)
        {
        	try
        	{
        		SQLPermission perm = new SQLPermission(callAbortPerm);
        		secMgr.checkPermission(perm);
        	}
        	catch (SecurityException ex)
        	{
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_permissionDenied"));
                Object[] msgArgs = {callAbortPerm};
                throw new SQLServerException(form.format(msgArgs), null, 0, ex);
        	}
        }
        
        bIsOpen = false;

 		
        executor.execute(new Runnable() {
        	public void run()
        	{
    			if(wrappedConnection.getConnectionLogger().isLoggable(Level.FINER))
    				wrappedConnection.getConnectionLogger().finer ( toString() + " Connection proxy aborted ");
    			try
    			{
    				wrappedConnection.poolCloseEventNotify();
    				wrappedConnection = null;
    			}
    			catch (SQLException e)
    			{
    	            throw new RuntimeException(e);
    			}
        	}
        });
    }

   /*L0*/ public void close() throws SQLServerException 
	{
		if(bIsOpen && (null != wrappedConnection))
		{
			if(wrappedConnection.getConnectionLogger().isLoggable(Level.FINER))
				wrappedConnection.getConnectionLogger().finer ( toString() + " Connection proxy closed "  );
			wrappedConnection.poolCloseEventNotify();
 			wrappedConnection = null;
		}
		bIsOpen = false;
	}
  	/*L0*/ void internalClose() 
	{
		bIsOpen = false;
		wrappedConnection = null;
	}
   

	/*L0*/ public boolean isClosed() throws SQLServerException 
	{
		return !bIsOpen;
	}

	/*L0*/ public DatabaseMetaData getMetaData() throws SQLServerException
	{
		checkClosed();
		return wrappedConnection.getMetaData();
	}

	/*L0*/ public void setReadOnly (boolean readOnly) throws SQLServerException 
	{
		checkClosed();
		wrappedConnection.setReadOnly(readOnly);
	}

	/*L0*/ public boolean isReadOnly() throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.isReadOnly();
	}

   /*L0*/ public void setCatalog(String catalog) throws SQLServerException 
	{
		checkClosed();
		wrappedConnection.setCatalog(catalog);
	}

	/*L0*/ public String getCatalog() throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.getCatalog();
	}

	/*L0*/ public void setTransactionIsolation(int level) throws SQLServerException
	{
		checkClosed();
		wrappedConnection.setTransactionIsolation(level);
	}

   /*L0*/ public int getTransactionIsolation() throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.getTransactionIsolation();
	}   

   /*L0*/ public SQLWarning getWarnings() throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.getWarnings(); // Warnings support added
	}

   /*L2*/ public  void clearWarnings() throws SQLServerException 
	{
		checkClosed();
		wrappedConnection.clearWarnings();
	}

   //--------------------------JDBC 2.0-----------------------------

   /*L2*/ public  Statement createStatement(
					      int resultSetType,
					      int resultSetConcurrency) throws SQLException 
	{
		checkClosed();
		return wrappedConnection.createStatement(resultSetType, resultSetConcurrency);
	}

   /*L2*/ public  PreparedStatement prepareStatement(
		      String sSql,
		      int resultSetType,
		      int resultSetConcurrency)  throws SQLException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement( sSql, resultSetType, resultSetConcurrency);
	}

   /*L2*/ public  CallableStatement prepareCall(String sql,  int resultSetType,
                                                       int resultSetConcurrency) throws SQLException
	{
		checkClosed();
		return wrappedConnection.prepareCall(sql, resultSetType, resultSetConcurrency);
	}

	/*L2*/ public  void setTypeMap(java.util.Map<String,Class<?>> map) throws SQLServerException 
	{
		checkClosed();
		wrappedConnection.setTypeMap(map);
	}

    public java.util.Map<String, Class<?>> getTypeMap() throws SQLServerException
    {
        checkClosed();
        return wrappedConnection.getTypeMap();
    }

	/*L3*/ public Statement createStatement(int nType, int nConcur, int nHold) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.createStatement(nType, nConcur, nHold);
	}
	
	public Statement createStatement(
			int nType,
			int nConcur,
			int nHold,
			SQLServerStatementColumnEncryptionSetting stmtColEncSetting)
			throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.createStatement(nType, nConcur, nHold, stmtColEncSetting);
	}	

  /*L3*/ public PreparedStatement prepareStatement(java.lang.String sql, int nType, int nConcur, int nHold) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql, nType, nConcur, nHold);
	}
  
	public PreparedStatement prepareStatement(
			String sql,
			int nType,
			int nConcur,
			int nHold,
			SQLServerStatementColumnEncryptionSetting stmtColEncSetting)
			throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql, nType, nConcur, nHold, stmtColEncSetting);
	}  

  /*L3*/ public CallableStatement prepareCall(String sql, int nType, int nConcur, int nHold) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareCall(sql, nType, nConcur, nHold);
	}
  
	public CallableStatement prepareCall(
			String sql,
			int nType,
			int nConcur,
			int nHold,
			SQLServerStatementColumnEncryptionSetting stmtColEncSetiing)
			throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareCall(sql, nType, nConcur, nHold, stmtColEncSetiing);
	}  

  /* JDBC 3.0 Auto generated keys */

  /*L3*/ public PreparedStatement prepareStatement(String sql, int flag) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql, flag);
	}

	public PreparedStatement prepareStatement(String sql, int flag, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql, flag, stmtColEncSetting);
	}
  
  /*L3*/ public PreparedStatement prepareStatement(String sql, int[] columnIndexes) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql,  columnIndexes);
	}

	public PreparedStatement prepareStatement(String sql, int[] columnIndexes, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql,  columnIndexes, stmtColEncSetting);
	}

  /*L3*/ public PreparedStatement prepareStatement(String sql, String[] columnNames) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql, columnNames);
	}
  
	public PreparedStatement prepareStatement(String sql, String[] columnNames, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.prepareStatement(sql, columnNames, stmtColEncSetting);
	}  

  /* JDBC 3.0 Savepoints */

  /*L3*/ public void releaseSavepoint(Savepoint savepoint) throws SQLServerException 
	{
		checkClosed();
		wrappedConnection.releaseSavepoint(savepoint);
	}

	/*L3*/ public Savepoint setSavepoint(String sName) throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.setSavepoint(sName);
	}

  /*L3*/ public Savepoint setSavepoint() throws SQLServerException 
	{
		checkClosed();
		return wrappedConnection.setSavepoint();
	}

	/*L3*/ public void rollback(Savepoint s) throws SQLServerException 
	{
		checkClosed();
		wrappedConnection.rollback(s);
	}

  /*L3*/ public int getHoldability() throws SQLServerException 
  	{
  		checkClosed();
		return wrappedConnection.getHoldability();
	}

  /*L3*/ public void setHoldability(int nNewHold) throws SQLServerException 
  	{
		checkClosed();
		wrappedConnection.setHoldability(nNewHold);
	}
  
	public int getNetworkTimeout() throws SQLException
	{
        DriverJDBCVersion.checkSupportsJDBC41();

        // The driver currently does not implement JDDBC 4.1 APIs
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
	}
	
	public void setNetworkTimeout(Executor executor, int timeout) throws SQLException
	{
        DriverJDBCVersion.checkSupportsJDBC41();

        // The driver currently does not implement JDDBC 4.1 APIs
        throw new SQLFeatureNotSupportedException(SQLServerException.getErrString("R_notSupported"));
	}  
	
    public String getSchema() throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC41();
			
    	checkClosed();
    	return wrappedConnection.getSchema();  	
    }
    
    public void setSchema(String schema) throws SQLException
    {
		DriverJDBCVersion.checkSupportsJDBC41();
			
    	checkClosed();
    	wrappedConnection.setSchema(schema); 	    	
    }	

    public java.sql.Array createArrayOf(String typeName, Object[] elements) throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.createArrayOf(typeName, elements);
    }

    public Blob createBlob() throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.createBlob();
    }

    public Clob createClob() throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.createClob();
    }

    public NClob createNClob() throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.createNClob();
    }

    public SQLXML createSQLXML() throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.createSQLXML();
    }

    public Struct createStruct(String typeName, Object[] attributes) throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.createStruct(typeName, attributes);
    }

    public Properties getClientInfo() throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.getClientInfo();
    }

    public String getClientInfo(String name) throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.getClientInfo(name);
    }

    public void setClientInfo(Properties properties) throws SQLClientInfoException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        // No checkClosed() call since we can only throw SQLClientInfoException from here
        wrappedConnection.setClientInfo(properties);
    }

    public void setClientInfo(String name, String value) throws SQLClientInfoException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        // No checkClosed() call since we can only throw SQLClientInfoException from here
        wrappedConnection.setClientInfo(name, value);
    }

    public boolean isValid(int timeout) throws SQLException
    {
        DriverJDBCVersion.checkSupportsJDBC4();

        checkClosed();
        return wrappedConnection.isValid(timeout);
    }

    public boolean isWrapperFor(Class<?> iface) throws SQLException
    {
    	wrappedConnection.getConnectionLogger().entering ( toString(), "isWrapperFor", iface );
        DriverJDBCVersion.checkSupportsJDBC4();
        boolean f = iface.isInstance(this);
        wrappedConnection.getConnectionLogger().exiting ( toString(), "isWrapperFor", f );
        return f;
    }

    public <T> T unwrap(Class<T> iface) throws SQLException
    {
        wrappedConnection.getConnectionLogger().entering ( toString(), "unwrap", iface );
        DriverJDBCVersion.checkSupportsJDBC4();
        T t;
        try
        {
            t = iface.cast(this);
        }
        catch (ClassCastException e)
        {
            SQLServerException newe =  new SQLServerException(e.getMessage(), e);
            throw newe;
        }
        wrappedConnection.getConnectionLogger().exiting ( toString(), "unwrap", t );
        return t;
    }
    
    public UUID getClientConnectionId() throws SQLServerException
    {
    	checkClosed();
    	return wrappedConnection.getClientConnectionId();
    }
    
	public synchronized void setSendTimeAsDatetime(boolean sendTimeAsDateTimeValue)  throws SQLServerException
	{
		checkClosed();
    	wrappedConnection.setSendTimeAsDatetime(sendTimeAsDateTimeValue);
	}

	public synchronized final boolean getSendTimeAsDatetime() throws SQLServerException
	{
		checkClosed();
    	return wrappedConnection.getSendTimeAsDatetime();		
	}
}
