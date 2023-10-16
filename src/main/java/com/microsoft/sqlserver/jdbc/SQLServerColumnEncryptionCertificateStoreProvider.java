/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Locale;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Provides the implementation of the key store provider for the Windows Certificate Store. This class enables using
 * keys stored in the Windows Certificate Store as column master keys.
 *
 */
public final class SQLServerColumnEncryptionCertificateStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {
    static final private java.util.logging.Logger windowsCertificateStoreLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionCertificateStoreProvider");

    String name = "MSSQL_CERTIFICATE_STORE";

    static final String LOCAL_MACHINE_DIRECTORY = "LocalMachine";
    static final String CURRENT_USER_DIRECTORY = "CurrentUser";
    static final String MY_CERTIFICATE_STORE = "My";

    private static final Lock lock = new ReentrantLock();

    /**
     * Constructs a SQLServerColumnEncryptionCertificateStoreProvider.
     */
    public SQLServerColumnEncryptionCertificateStoreProvider() {
        windowsCertificateStoreLogger.entering(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(),
                "SQLServerColumnEncryptionCertificateStoreProvider");
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    @Override
    public byte[] encryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] plainTextColumnEncryptionKey) throws SQLServerException {
        // not supported
        throw new SQLServerException(null,
                SQLServerException.getErrString("R_InvalidWindowsCertificateStoreEncryption"), null, 0, false);
    }

    @Override
    public byte[] decryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        windowsCertificateStoreLogger.entering(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(),
                "decryptColumnEncryptionKey", "Decrypting Column Encryption Key.");
        byte[] plainCek;
        if (SQLServerConnection.isWindows) {
            plainCek = decryptColumnEncryptionKeyWindows(masterKeyPath, encryptionAlgorithm,
                    encryptedColumnEncryptionKey);
        } else {
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), null);
        }
        windowsCertificateStoreLogger.exiting(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(),
                "decryptColumnEncryptionKey", "Finished decrypting Column Encryption Key.");
        return plainCek;
    }

    @Override
    public boolean verifyColumnMasterKeyMetadata(String masterKeyPath, boolean allowEnclaveComputations,
            byte[] signature) throws SQLServerException {
        try {
            lock.lock();

            return AuthenticationJNI.VerifyColumnMasterKeyMetadata(masterKeyPath, allowEnclaveComputations, signature);
        } catch (DLLException e) {
            DLLException.buildException(e.getErrCode(), e.getParam1(), e.getParam2(), e.getParam3());
            return false;
        } finally {
            lock.unlock();
        }
    }

    private byte[] decryptColumnEncryptionKeyWindows(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        try {
            lock.lock();

            return AuthenticationJNI.DecryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm,
                    encryptedColumnEncryptionKey);
        } catch (DLLException e) {
            DLLException.buildException(e.getErrCode(), e.getParam1(), e.getParam2(), e.getParam3());
            return null;
        } finally {
            lock.unlock();
        }
    }

}
