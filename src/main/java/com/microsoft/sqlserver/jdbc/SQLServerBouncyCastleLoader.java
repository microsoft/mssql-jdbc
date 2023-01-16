/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.Provider;
import java.security.Security;


/*
 * Class that is meant to statically load the BouncyCastle Provider for JDK 8. Hides the call so JDK 11/13 don't have to include the dependency.
 * Also loads BouncyCastle provider for PKCS1 private key parsing.
 */
class SQLServerBouncyCastleLoader {
    private SQLServerBouncyCastleLoader() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    static void loadBouncyCastle() {
        Provider p = new org.bouncycastle.jce.provider.BouncyCastleProvider();
        if (null == Security.getProvider(p.getName())) {
            Security.addProvider(p);
        }
    }
}
