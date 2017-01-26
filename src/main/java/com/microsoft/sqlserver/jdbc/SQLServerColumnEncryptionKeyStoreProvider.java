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
 * Extend this class to implement a custom key store provider.
 *
 */
public abstract class SQLServerColumnEncryptionKeyStoreProvider {

    /**
     * Sets the name of this key store provider.
     * 
     * @param name
     *            value to be set for the key store provider.
     */
    public abstract void setName(String name);

    /**
     * Retrieves the name of this key store provider.
     * 
     * @return the name of this key store provider.
     */
    public abstract String getName();

    /**
     * Base class method for decrypting the specified encrypted value of a column encryption key. The encrypted value is expected to be encrypted
     * using the column master key with the specified key path and using the specified algorithm.
     * 
     * @param masterKeyPath
     *            The column master key path.
     * @param encryptionAlgorithm
     *            the specific encryption algorithm.
     * @param encryptedColumnEncryptionKey
     *            the encrypted column encryption key
     * @return the decrypted value of column encryption key.
     * @throws SQLServerException
     *             when an error occurs while decrypting the CEK
     */
    public abstract byte[] decryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException;

    /**
     * Base class method for encrypting a column encryption key using the column master key with the specified key path and using the specified
     * algorithm.
     * 
     * @param masterKeyPath
     *            The column master key path.
     * @param encryptionAlgorithm
     *            the specific encryption algorithm.
     * @param columnEncryptionKey
     *            column encryption key to be encrypted.
     * @return the encrypted column encryption key.
     * @throws SQLServerException
     *             when an error occurs while encrypting the CEK
     */
    public abstract byte[] encryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] columnEncryptionKey) throws SQLServerException;

}
