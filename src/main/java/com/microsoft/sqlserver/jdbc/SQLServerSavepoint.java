/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;


/**
 * Provides an implementation of JDBC Interface java.sql.Savepoint. A savepoint is checkpoint to which a transaction can
 * be rolled back. Savepoints are defined relative to a connection.F
 * <p>
 * The API javadoc for JDBC API methods that this class implements are not repeated here. Please see Sun's JDBC API
 * interfaces javadoc for those details.
 */

public final class SQLServerSavepoint implements ISQLServerSavepoint {
    /**
     * Always update serialVersionUID when prompted.
     */
    private static final long serialVersionUID = 1857415943191289598L;

    /** sName */
    private final String sName;

    /** nID */
    private final int nId;

    /** connection */
    private final SQLServerConnection con;

    /**
     * Constructs a SQLServerSavepoint.
     * 
     * @param con
     *        the connection
     * @param sName
     *        the savepoint name
     */
    public SQLServerSavepoint(SQLServerConnection con, String sName) {
        this.con = con;
        if (sName == null) {
            nId = con.getNextSavepointId();
            this.sName = null;
        } else {
            this.sName = sName;
            nId = 0;
        }
    }

    @Override
    public String getSavepointName() throws SQLServerException {
        if (sName == null)
            SQLServerException.makeFromDriverError(con, null, SQLServerException.getErrString("R_savepointNotNamed"),
                    null, false);
        return sName;
    }

    @Override
    public String getLabel() {
        if (sName == null)
            return "S" + nId;
        else
            return sName;
    }

    @Override
    public boolean isNamed() {
        return sName != null;
    }

    @Override
    public int getSavepointId() throws SQLServerException {
        if (sName != null) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_savepointNamed"));
            Object[] msgArgs = {sName};
            SQLServerException.makeFromDriverError(con, null, form.format(msgArgs), null, false);
        }
        return nId;
    }
}
