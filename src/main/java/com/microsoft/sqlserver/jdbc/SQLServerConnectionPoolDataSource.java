//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerConnectionPoolDataSource.java
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
import javax.sql.*;
import javax.naming.*;
import java.util.logging.*;

/**
 * SQLServerConnectionPoolDataSource provides physical database connections for connection pool managers.
 * SQLServerConnectionPoolDataSource is typically used in Java Application Server environments that support built-in
 * connection pooling and require a ConnectionPoolDataSource to provide physical connections.
 * For example, J2EE application servers that provide JDBC 3.0 API spec connection pooling.
 */

public class SQLServerConnectionPoolDataSource extends SQLServerDataSource implements ConnectionPoolDataSource {
    //Get a new physical connection that the pool manager will issue logical connections from
    /*L0*/
    public PooledConnection getPooledConnection() throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getPooledConnection");
        PooledConnection pcon = getPooledConnection(getUser(), getPassword());
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getPooledConnection", pcon);
        return pcon;
    }

    /*L0*/
    public PooledConnection getPooledConnection(String user, String password) throws SQLException {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getPooledConnection", new Object[]{user, "Password not traced"});
        SQLServerPooledConnection pc = new SQLServerPooledConnection(this, user, password);
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getPooledConnection", pc);
        return pc;
    }

    // Implement javax.naming.Referenceable interface methods.

    public Reference getReference() {
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.entering(getClassNameLogging(), "getReference");
        Reference ref = getReferenceInternal("com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource");
        if (loggerExternal.isLoggable(Level.FINER))
            loggerExternal.exiting(getClassNameLogging(), "getReference", ref);
        return ref;
    }

    private Object writeReplace() throws java.io.ObjectStreamException {
        return new SerializationProxy(this);
    }

    private void readObject(java.io.ObjectInputStream stream) throws java.io.InvalidObjectException {
        // For added security/robustness, the only way to rehydrate a serialized SQLServerDataSource
        // is to use a SerializationProxy.  Direct use of readObject() is not supported.
        throw new java.io.InvalidObjectException("");
    }

    // This is 90% duplicate from the SQLServerDataSource, the serialization proxy pattern does not lend itself to inheritance
    // so the duplication is necessary
    private static class SerializationProxy
            implements java.io.Serializable {
        private final Reference ref;
        private static final long serialVersionUID = 654661379842314126L;

        SerializationProxy(SQLServerConnectionPoolDataSource ds) {
            // We do not need the class name so pass null, serialization mechanism 
            // stores the class info.
            ref = ds.getReferenceInternal(null);
        }

        private Object readResolve() {
            SQLServerConnectionPoolDataSource ds = new SQLServerConnectionPoolDataSource();
            ds.initializeFromReference(ref);
            return ds;
        }
    }

}
