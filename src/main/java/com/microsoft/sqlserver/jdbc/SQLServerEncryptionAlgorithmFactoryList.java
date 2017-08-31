/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintain list of all the encryption algorithm factory classes
 */
final class SQLServerEncryptionAlgorithmFactoryList {

    private ConcurrentHashMap<String, SQLServerEncryptionAlgorithmFactory> encryptionAlgoFactoryMap;

    private static final SQLServerEncryptionAlgorithmFactoryList instance = new SQLServerEncryptionAlgorithmFactoryList();

    private SQLServerEncryptionAlgorithmFactoryList() {
        encryptionAlgoFactoryMap = new ConcurrentHashMap<>();
        encryptionAlgoFactoryMap.putIfAbsent(SQLServerAeadAes256CbcHmac256Algorithm.algorithmName, new SQLServerAeadAes256CbcHmac256Factory());
    }

    static SQLServerEncryptionAlgorithmFactoryList getInstance() {
        return instance;
    }

    /**
     * @return list of registered algorithms separated by comma
     */
    String getRegisteredCipherAlgorithmNames() {
        StringBuffer stringBuff = new StringBuffer();
        boolean first = true;
        for (String key : encryptionAlgoFactoryMap.keySet()) {
            if (first) {
                stringBuff.append("'");
                first = false;
            }
            else {
                stringBuff.append(", '");
            }
            stringBuff.append(key);
            stringBuff.append("'");

        }
        return stringBuff.toString();
    }

    /**
     * Return instance for given algorithm
     * 
     * @param key
     * @param encryptionType
     * @param algorithmName
     * @return instance for given algorithm
     * @throws SQLServerException
     */
    SQLServerEncryptionAlgorithm getAlgorithm(SQLServerSymmetricKey key,
            SQLServerEncryptionType encryptionType,
            String algorithmName) throws SQLServerException {
        SQLServerEncryptionAlgorithm encryptionAlgorithm = null;
        SQLServerEncryptionAlgorithmFactory factory = null;
        if (!encryptionAlgoFactoryMap.containsKey(algorithmName)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnknownColumnEncryptionAlgorithm"));
            Object[] msgArgs = {algorithmName, SQLServerEncryptionAlgorithmFactoryList.getInstance().getRegisteredCipherAlgorithmNames()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        factory = encryptionAlgoFactoryMap.get(algorithmName);
        assert null != factory : "Null Algorithm Factory class detected";
        encryptionAlgorithm = factory.create(key, encryptionType, algorithmName);
        return encryptionAlgorithm;
    }

}
