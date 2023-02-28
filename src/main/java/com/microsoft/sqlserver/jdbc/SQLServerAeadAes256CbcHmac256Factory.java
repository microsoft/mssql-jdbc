/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.text.MessageFormat;
import java.util.Base64;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Factory for SQLServerAeadAes256CbcHmac256Algorithm
 */
class SQLServerAeadAes256CbcHmac256Factory extends SQLServerEncryptionAlgorithmFactory {
    // In future we can have more
    private byte algorithmVersion = 0x1;
    private ConcurrentHashMap<String, SQLServerAeadAes256CbcHmac256Algorithm> encryptionAlgorithms = new ConcurrentHashMap<>();

    @Override
    SQLServerEncryptionAlgorithm create(SQLServerSymmetricKey columnEncryptionKey,
            SQLServerEncryptionType encryptionType, String encryptionAlgorithm) throws SQLServerException {

        assert (columnEncryptionKey != null);
        if (encryptionType != SQLServerEncryptionType.DETERMINISTIC
                && encryptionType != SQLServerEncryptionType.RANDOMIZED) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidEncryptionType"));
            Object[] msgArgs = {encryptionType, encryptionAlgorithm,
                    "'" + SQLServerEncryptionType.DETERMINISTIC + "," + SQLServerEncryptionType.RANDOMIZED + "'"};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);

        }

        StringBuilder factoryKeyBuilder = new StringBuilder();
        factoryKeyBuilder.append(
                Base64.getEncoder().encodeToString(new String(columnEncryptionKey.getRootKey(), UTF_8).getBytes()));

        factoryKeyBuilder.append(":");
        factoryKeyBuilder.append(encryptionType);
        factoryKeyBuilder.append(":");
        factoryKeyBuilder.append(algorithmVersion);

        String factoryKey = factoryKeyBuilder.toString();

        SQLServerAeadAes256CbcHmac256Algorithm aesAlgorithm;

        if (!encryptionAlgorithms.containsKey(factoryKey)) {
            SQLServerAeadAes256CbcHmac256EncryptionKey encryptedKey = new SQLServerAeadAes256CbcHmac256EncryptionKey(
                    columnEncryptionKey.getRootKey(),
                    SQLServerAeadAes256CbcHmac256Algorithm.AEAD_AES_256_CBC_HMAC_SHA256);
            aesAlgorithm = new SQLServerAeadAes256CbcHmac256Algorithm(encryptedKey, encryptionType, algorithmVersion);
            encryptionAlgorithms.putIfAbsent(factoryKey, aesAlgorithm);
        }

        return encryptionAlgorithms.get(factoryKey);
    }

}
