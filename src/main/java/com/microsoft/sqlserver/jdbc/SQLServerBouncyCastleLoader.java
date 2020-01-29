package com.microsoft.sqlserver.jdbc;

import java.security.Security;

/*
 * Class that is meant to statically load the BouncyCastle Provider for JDK 8. Hides the call so JDK 11/13 don't have to include the dependency.
 */
class SQLServerBouncyCastleLoader {
    static void loadBouncyCastle() {
        Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
    }
}
