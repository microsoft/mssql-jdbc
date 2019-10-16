/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Locale;


/**
 * Provides the implementation of the key store provider for the Windows Certificate Store. This class enables using
 * keys stored in the Windows Certificate Store as column master keys.
 *
 */
public final class SQLServerColumnEncryptionCertificateStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {
    static final private java.util.logging.Logger windowsCertificateStoreLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionCertificateStoreProvider");

    static boolean isWindows;

    String name = "MSSQL_CERTIFICATE_STORE";

    static final String localMachineDirectory = "LocalMachine";
    static final String currentUserDirectory = "CurrentUser";
    static final String myCertificateStore = "My";

    static {
        if (System.getProperty("os.name").toLowerCase(Locale.ENGLISH).startsWith("windows")) {
            isWindows = true;
        } else {
            isWindows = false;
        }
    }

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

    public byte[] encryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] plainTextColumnEncryptionKey) throws SQLServerException {
        throw new SQLServerException(null,
                SQLServerException.getErrString("R_InvalidWindowsCertificateStoreEncryption"), null, 0, false);
    }

    private byte[] decryptColumnEncryptionKeyWindows(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        try {
            return AuthenticationJNI.DecryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm,
                    encryptedColumnEncryptionKey);
        } catch (DLLException e) {
            DLLException.buildException(e.GetErrCode(), e.GetParam1(), e.GetParam2(), e.GetParam3());
            return null;
        }
    }

    public byte[] decryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        windowsCertificateStoreLogger.entering(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(),
                "decryptColumnEncryptionKey", "Decrypting Column Encryption Key.");
        byte[] plainCek;
        if (isWindows) {
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
            return AuthenticationJNI.VerifyColumnMasterKeyMetadata(masterKeyPath, allowEnclaveComputations, signature);
        } catch (DLLException e) {
            DLLException.buildException(e.GetErrCode(), e.GetParam1(), e.GetParam2(), e.GetParam3());
            return false;
        }
    }
}
