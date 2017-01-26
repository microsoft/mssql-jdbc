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
 * Abstract base class for all the encryption algorithm factory classes.
 *
 */
abstract class SQLServerEncryptionAlgorithmFactory {

    /**
     * 
     * @param columnEncryptionKey
     *            key which will be used in encryption/decryption
     * @param encryptionType
     *            specifies kind of encryption
     * @param encryptionAlgorithm
     *            name of encryption algorithm
     * @return created SQLServerEncryptionAlgorithm instance
     * @throws SQLServerException
     *             when an error occurs
     */
    abstract SQLServerEncryptionAlgorithm create(SQLServerSymmetricKey columnEncryptionKey,
            SQLServerEncryptionType encryptionType,
            String encryptionAlgorithm) throws SQLServerException;

}
