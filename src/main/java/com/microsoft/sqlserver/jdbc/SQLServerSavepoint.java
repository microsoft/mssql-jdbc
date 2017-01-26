/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.Savepoint;
import java.text.MessageFormat;

/**
 * SQLServerSavepoint implements JDBC 3.0 savepoints. A savepoint is checkpoint to which a transaction can be rolled back. Savepoints are defined
 * relative to a connection.
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API interfaces javadoc for those
 * details.
 */

public final class SQLServerSavepoint implements Savepoint {
    private final String sName;
    private final int nId;
    private final SQLServerConnection con;

    /**
     * Create a new savepoint
     * 
     * @param con
     *            the connection
     * @param sName
     *            the savepoint name
     */
    public SQLServerSavepoint(SQLServerConnection con,
            String sName) {
        this.con = con;
        if (sName == null) {
            nId = con.getNextSavepointId();
            this.sName = null;
        }
        else {
            this.sName = sName;
            nId = 0;
        }
    }

    public String getSavepointName() throws SQLServerException {
        if (sName == null)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_savepointNotNamed"), null, false);
        return sName;
    }

    /**
     * Get the savepoint label
     * 
     * @return the name
     */
    public String getLabel() {
        if (sName == null)
            return "S" + nId;
        else
            return sName;
    }

    /**
     * Checks if the savepoint label is null
     * 
     * @return true is the savepoint is named. Otherwise, false.
     */
    public boolean isNamed() {
        return sName != null;
    }

    public int getSavepointId() throws SQLServerException {
        if (sName != null) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_savepointNamed"));
            Object[] msgArgs = {sName};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, false);
        }
        return nId;
    }
}
