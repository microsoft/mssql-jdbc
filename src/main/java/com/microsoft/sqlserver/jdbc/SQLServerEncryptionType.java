/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;


/**
 * 
 * Encryption types supported
 *
 */
enum SQLServerEncryptionType {
    DETERMINISTIC((byte) 1),
    RANDOMIZED((byte) 2),
    PLAINTEXT((byte) 0);

    final byte value;
    private static final SQLServerEncryptionType[] VALUES = values();

    SQLServerEncryptionType(byte val) {
        this.value = val;
    }

    byte getValue() {
        return this.value;
    }

    static SQLServerEncryptionType of(byte val) throws SQLServerException {
        for (SQLServerEncryptionType type : VALUES)
            if (val == type.value)
                return type;

        // Invalid type.
        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownColumnEncryptionType"));
        Object[] msgArgs = {val};
        SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);

        // Make the compiler happy.
        return null;
    }
}
