/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * 
 * Base class which represents Symmetric key
 *
 */
class SQLServerSymmetricKey {
    private byte[] rootKey;

    SQLServerSymmetricKey(byte[] rootKey) throws SQLServerException {
        if (null == rootKey) {
            throw new SQLServerException(this, SQLServerException.getErrString("R_NullColumnEncryptionKey"), null, 0, false);
        }
        else if (0 == rootKey.length) {
            throw new SQLServerException(this, SQLServerException.getErrString("R_EmptyColumnEncryptionKey"), null, 0, false);
        }
        this.rootKey = rootKey;
    }

    byte[] getRootKey() {
        return rootKey;
    }

    int length() {
        return rootKey.length;
    }

    void zeroOutKey() {
        for (int i = 0; i < rootKey.length; i++) {
            rootKey[i] = (byte) 0;
        }
    }
}
