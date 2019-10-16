/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * 
 * Defines the abtract class for a SQL Server Column Encryption key store provider Extend this class to implement a
 * custom key store provider.
 *
 */
public abstract class SQLServerColumnEncryptionKeyStoreProvider {

    /**
     * Sets the name of this key store provider.
     * 
     * @param name
     *        value to be set for the key store provider.
     */
    public abstract void setName(String name);

    /**
     * Returns the name of this key store provider.
     * 
     * @return the name of this key store provider.
     */
    public abstract String getName();

    /**
     * Decrypts the specified encrypted value of a column encryption key. The encrypted value is expected to be
     * encrypted using the column master key with the specified key path and using the specified algorithm.
     * 
     * @param masterKeyPath
     *        The column master key path.
     * @param encryptionAlgorithm
     *        the specific encryption algorithm.
     * @param encryptedColumnEncryptionKey
     *        the encrypted column encryption key
     * @return the decrypted value of column encryption key.
     * @throws SQLServerException
     *         when an error occurs while decrypting the CEK
     */
    public abstract byte[] decryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException;

    /**
     * Encrypts a column encryption key using the column master key with the specified key path and using the specified
     * algorithm.
     * 
     * @param masterKeyPath
     *        The column master key path.
     * @param encryptionAlgorithm
     *        the specific encryption algorithm.
     * @param columnEncryptionKey
     *        column encryption key to be encrypted.
     * @return the encrypted column encryption key.
     * @throws SQLServerException
     *         when an error occurs while encrypting the CEK
     */
    public abstract byte[] encryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] columnEncryptionKey) throws SQLServerException;

    /**
     * Verify the signature is valid for the column master key
     * 
     * @param masterKeyPath
     *        column master key path
     * @param allowEnclaveComputations
     *        indicates whether the column master key supports enclave computations
     * @param signature
     *        signature of the column master key metadata
     * @return
     *        whether the signature is valid for the column master key
     * @throws SQLServerException
     *      when an error occurs while verifying the signature
     */
    public abstract boolean verifyColumnMasterKeyMetadata (String masterKeyPath, boolean allowEnclaveComputations, byte[] signature) throws SQLServerException;
}
