/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.InvalidKeyException;
import java.security.KeyStore;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
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

        if (!SQLServerConnection.isWindows) {
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), null);
        }

        windowsCertificateStoreLogger.entering(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(),
                "decryptColumnEncryptionKey", "Decrypting Column Encryption Key.");

        KeyStoreProviderCommon.validateNonEmptyMasterKeyPath(masterKeyPath);

        CertificateDetails certificateDetails = getCertificateDetails(masterKeyPath);
        byte[] plainCEK = KeyStoreProviderCommon.decryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm,
                encryptedColumnEncryptionKey, certificateDetails);

        windowsCertificateStoreLogger.exiting(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(),
                "decryptColumnEncryptionKey", "Finished decrypting Column Encryption Key.");

        return plainCEK;
    }

    @Override
    public boolean verifyColumnMasterKeyMetadata(String masterKeyPath, boolean allowEnclaveComputations,
            byte[] signature) throws SQLServerException {

        if (!allowEnclaveComputations) {
            return false;
        }

        KeyStoreProviderCommon.validateNonEmptyMasterKeyPath(masterKeyPath);
        CertificateDetails certificateDetails = getCertificateDetails(masterKeyPath);

        byte[] signedHash = null;
        boolean isValid = false;

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(name.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
            md.update(masterKeyPath.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
            // value of allowEnclaveComputations is always true here
            md.update("true".getBytes(java.nio.charset.StandardCharsets.UTF_16LE));

            byte[] dataToVerify = md.digest();
            Signature sig = Signature.getInstance("SHA256withRSA");

            sig.initSign((PrivateKey) certificateDetails.privateKey);
            sig.update(dataToVerify);

            signedHash = sig.sign();

            sig.initVerify(certificateDetails.certificate.getPublicKey());
            sig.update(dataToVerify);
            isValid = sig.verify(signature);
        } catch (NoSuchAlgorithmException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_NoSHA256Algorithm"), e);
        } catch (InvalidKeyException | SignatureException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SignatureNotMatch"));
            Object[] msgArgs = {Util.byteToHexDisplayString(signature),
                    (signedHash != null) ? Util.byteToHexDisplayString(signedHash) : " ", masterKeyPath,
                    ": " + e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        if (!isValid) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SignatureNotMatch"));
            Object[] msgArgs = {Util.byteToHexDisplayString(signature), Util.byteToHexDisplayString(signedHash),
                    masterKeyPath, ""};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        return isValid;
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

    private CertificateDetails getCertificateDetails(String masterKeyPath) throws SQLServerException {
        CertificateDetails certificateDetails = null;

        try {
            if (null == masterKeyPath || 0 == masterKeyPath.length()) {
                throw new SQLServerException(null, SQLServerException.getErrString("R_InvalidMasterKeyDetails"), null,
                        0, false);
            }

            KeyStore ks = KeyStore.getInstance("Windows-MY-CURRENTUSER");
            ks.load(null, null);
            // Enumeration<String> en = ks.aliases();

            X509Certificate cert = (X509Certificate) ks.getCertificate(masterKeyPath);
            PrivateKey privateKey = (PrivateKey) ks.getKey(masterKeyPath, null);
            certificateDetails = new CertificateDetails(cert, privateKey);

            // System.out.println(" Certificate : " + cert);
            // System.out.println("---> alias : " + masterKeyPath);
            // System.out.println(" subjectDN : " + cert.getSubjectDN());
            // System.out.println(" issuerDN : " + cert.getIssuerDN());

        } catch (Exception e) {
            e.printStackTrace();
        }

        return certificateDetails;
    }
}
