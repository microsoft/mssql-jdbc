/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) 2016 Microsoft Corporation All rights reserved. This program is
 * made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.testframework;

import com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionKeyStoreProvider;
import com.microsoft.sqlserver.jdbc.SQLServerException;


/**
 * DummyKeyStoreProvider Class for Tests. 
 * 
 */
public class DummyKeyStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {
    public String name = "DUMMY_PROVIDER";

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public byte[] decryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        // Not implemented
        throw new UnsupportedOperationException();
            }

    @Override
    public byte[] encryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] columnEncryptionKey) throws SQLServerException {
        // Not implemented
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean verifyColumnMasterKeyMetadata(String masterKeyPath,
            boolean allowEnclaveComputations,
            byte[] signature) throws SQLServerException {
        // Not implemented
        throw new UnsupportedOperationException();
    }
}