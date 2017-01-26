/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/*
 * Abstract base class for all AE encryption algorithms. It exposes two functions 1. encryptData - This function is used by the driver under the
 * covers to transparently encrypt AE enabled column data. 2. decryptData - This function is used by the driver under the covers to transparently
 * decrypt AE enabled column data.
 */
abstract class SQLServerEncryptionAlgorithm {

    /**
     * Perform encryption of the plain text
     * 
     * @param plainText
     *            data to be encrypted
     * @return cipher text after encryption
     */
    abstract byte[] encryptData(byte[] plainText) throws SQLServerException;

    /**
     * Decrypt cipher text to plain text
     * 
     * @param cipherText
     *            data to be decrypted
     * @return plain text after decryption
     */
    abstract byte[] decryptData(byte[] cipherText) throws SQLServerException;
}
