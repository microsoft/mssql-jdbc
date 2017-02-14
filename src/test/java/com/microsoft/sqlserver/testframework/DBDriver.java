/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import java.sql.SQLException;
import java.util.logging.Logger;

public class DBDriver extends AbstractParentWrapper {
    public static final Logger log = Logger.getLogger("DBDriver");
    private static double _serverversion = 0;

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBDriver(AbstractParentWrapper parent,
            Object internal,
            String name) {
        super(parent, internal, name);
        // TODO Auto-generated constructor stub
    }

    // Returns the default server version, not to be used in multiple server versions case
    public static double serverversion() throws Exception {
        if (_serverversion == 0) {
            try {
                DBConnection conn = new DBConnection(AbstractTest.getConnectionString());
                _serverversion = conn.serverversion();
                conn.close();
            }
            catch (SQLException e) {
                log.fine("Default server version can not be obtained in the driver class");
                _serverversion = 9.0;
            }
        }
        return _serverversion;
    }

}
