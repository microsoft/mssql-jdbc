/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.SignatureException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * 
 * The implementation of the key store provider for Java Key Store. This class enables using certificates stored in the Java keystore as column master
 * keys.
 *
 */
public class SQLServerColumnEncryptionJavaKeyStoreProvider extends SQLServerColumnEncryptionKeyStoreProvider {
    String name = "MSSQL_JAVA_KEYSTORE";
    String keyStorePath = null;
    char[] keyStorePwd = null;

    static final private java.util.logging.Logger javaKeyStoreLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionJavaKeyStoreProvider");

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    /**
     * Key store provider for the Java Key Store.
     * 
     * @param keyStoreLocation
     *            specifies the location of the keystore
     * @param keyStoreSecret
     *            specifies the secret used for keystore
     * @throws SQLServerException
     *             when an error occurs
     */
    public SQLServerColumnEncryptionJavaKeyStoreProvider(String keyStoreLocation,
            char[] keyStoreSecret) throws SQLServerException {
        javaKeyStoreLogger.entering(SQLServerColumnEncryptionJavaKeyStoreProvider.class.getName(), "SQLServerColumnEncryptionJavaKeyStoreProvider");

        if ((null == keyStoreLocation) || (0 == keyStoreLocation.length())) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidConnectionSetting"));
            Object[] msgArgs = {"keyStoreLocation", keyStoreLocation};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        this.keyStorePath = keyStoreLocation;

        if (javaKeyStoreLogger.isLoggable(java.util.logging.Level.FINE)) {
            javaKeyStoreLogger.fine("Path of key store provider is set.");
        }

        // Password can be null or empty, PKCS12 type allows that.
        if (null == keyStoreSecret) {
            keyStoreSecret = "".toCharArray();
        }

        this.keyStorePwd = new char[keyStoreSecret.length];
        System.arraycopy(keyStoreSecret, 0, this.keyStorePwd, 0, keyStoreSecret.length);

        if (javaKeyStoreLogger.isLoggable(java.util.logging.Level.FINE)) {
            javaKeyStoreLogger.fine("Password for key store provider is set.");
        }

        javaKeyStoreLogger.exiting(SQLServerColumnEncryptionJavaKeyStoreProvider.class.getName(), "SQLServerColumnEncryptionJavaKeyStoreProvider");
    }

    @Override
    public byte[] decryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        javaKeyStoreLogger.entering(SQLServerColumnEncryptionJavaKeyStoreProvider.class.getName(), "decryptColumnEncryptionKey",
                "Decrypting Column Encryption Key.");

        KeyStoreProviderCommon.validateNonEmptyMasterKeyPath(masterKeyPath);
        CertificateDetails certificateDetails = getCertificateDetails(masterKeyPath);
        byte[] plainCEK = KeyStoreProviderCommon.decryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm, encryptedColumnEncryptionKey,
                certificateDetails);

        javaKeyStoreLogger.exiting(SQLServerColumnEncryptionJavaKeyStoreProvider.class.getName(), "decryptColumnEncryptionKey",
                "Finished decrypting Column Encryption Key.");
        return plainCEK;
    }

    private CertificateDetails getCertificateDetails(String masterKeyPath) throws SQLServerException {
        FileInputStream fis = null;
        KeyStore keyStore = null;
        CertificateDetails certificateDetails = null;

        try {
            if (null == masterKeyPath || 0 == masterKeyPath.length()) {
                throw new SQLServerException(null, SQLServerException.getErrString("R_InvalidMasterKeyDetails"), null, 0, false);
            }

            try {
                // Try to load JKS first, if fails try PKCS12
                keyStore = KeyStore.getInstance("JKS");
                fis = new FileInputStream(keyStorePath);
                keyStore.load(fis, keyStorePwd);
            }
            catch (IOException e) {
                if (null != fis)
                    fis.close();

                // Loading as JKS failed, try to load as PKCS12
                keyStore = KeyStore.getInstance("PKCS12");
                fis = new FileInputStream(keyStorePath);
                keyStore.load(fis, keyStorePwd);
            }

            certificateDetails = getCertificateDetailsByAlias(keyStore, masterKeyPath);
        }
        catch (FileNotFoundException fileNotFound) {
            throw new SQLServerException(this, SQLServerException.getErrString("R_KeyStoreNotFound"), null, 0, false);
        }
        catch (IOException | CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidKeyStoreFile"));
            Object[] msgArgs = {keyStorePath};
            throw new SQLServerException(form.format(msgArgs), e);
        }
        finally {
            try {
                if (null != fis)
                    fis.close();
            }
            // Ignore the exception as we are cleaning up.
            catch (IOException e) {
            }
        }

        return certificateDetails;
    }

    private CertificateDetails getCertificateDetailsByAlias(KeyStore keyStore,
            String alias) throws SQLServerException {
        try {
            X509Certificate publicCertificate = (X509Certificate) keyStore.getCertificate(alias);
            Key keyPrivate = keyStore.getKey(alias, keyStorePwd);
            if (null == publicCertificate) {
                // Certificate not found. Throw an exception.
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CertificateNotFoundForAlias"));
                Object[] msgArgs = {alias, "MSSQL_JAVA_KEYSTORE"};
                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
            }

            // found certificate but corresponding private key not found, throw exception
            if (null == keyPrivate) {
                throw new UnrecoverableKeyException();
            }

            return new CertificateDetails(publicCertificate, keyPrivate);
        }
        catch (UnrecoverableKeyException unrecoverableKeyException) {

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnrecoverableKeyAE"));
            Object[] msgArgs = {alias};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
        catch (NoSuchAlgorithmException | KeyStoreException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CertificateError"));
            Object[] msgArgs = {alias, name};
            throw new SQLServerException(form.format(msgArgs), e);
        }
    }

    @Override
    public byte[] encryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] plainTextColumnEncryptionKey) throws SQLServerException {
        javaKeyStoreLogger.entering(SQLServerColumnEncryptionJavaKeyStoreProvider.class.getName(),
                Thread.currentThread().getStackTrace()[1].getMethodName(), "Encrypting Column Encryption Key.");

        byte[] version = KeyStoreProviderCommon.version;
        KeyStoreProviderCommon.validateNonEmptyMasterKeyPath(masterKeyPath);

        if (null == plainTextColumnEncryptionKey) {

            throw new SQLServerException(null, SQLServerException.getErrString("R_NullColumnEncryptionKey"), null, 0, false);

        }
        else if (0 == plainTextColumnEncryptionKey.length) {

            throw new SQLServerException(null, SQLServerException.getErrString("R_EmptyColumnEncryptionKey"), null, 0, false);

        }

        KeyStoreProviderCommon.validateEncryptionAlgorithm(encryptionAlgorithm, true);

        CertificateDetails certificateDetails = getCertificateDetails(masterKeyPath);
        byte[] cipherText = encryptRSAOAEP(plainTextColumnEncryptionKey, certificateDetails);
        byte[] cipherTextLength = getLittleEndianBytesFromShort((short) cipherText.length);
        byte[] masterKeyPathBytes = masterKeyPath.toLowerCase().getBytes(UTF_16LE);

        byte[] keyPathLength = getLittleEndianBytesFromShort((short) masterKeyPathBytes.length);

        byte[] dataToSign = new byte[version.length + keyPathLength.length + cipherTextLength.length + masterKeyPathBytes.length + cipherText.length];
        int destinationPosition = version.length;
        System.arraycopy(version, 0, dataToSign, 0, version.length);

        System.arraycopy(keyPathLength, 0, dataToSign, destinationPosition, keyPathLength.length);
        destinationPosition += keyPathLength.length;

        System.arraycopy(cipherTextLength, 0, dataToSign, destinationPosition, cipherTextLength.length);
        destinationPosition += cipherTextLength.length;

        System.arraycopy(masterKeyPathBytes, 0, dataToSign, destinationPosition, masterKeyPathBytes.length);
        destinationPosition += masterKeyPathBytes.length;

        System.arraycopy(cipherText, 0, dataToSign, destinationPosition, cipherText.length);
        byte[] signedHash = rsaSignHashedData(dataToSign, certificateDetails);

        int encryptedColumnEncryptionKeyLength = version.length + cipherTextLength.length + keyPathLength.length + cipherText.length
                + masterKeyPathBytes.length + signedHash.length;
        byte[] encryptedColumnEncryptionKey = new byte[encryptedColumnEncryptionKeyLength];

        int currentIndex = 0;
        System.arraycopy(version, 0, encryptedColumnEncryptionKey, currentIndex, version.length);
        currentIndex += version.length;

        System.arraycopy(keyPathLength, 0, encryptedColumnEncryptionKey, currentIndex, keyPathLength.length);
        currentIndex += keyPathLength.length;

        System.arraycopy(cipherTextLength, 0, encryptedColumnEncryptionKey, currentIndex, cipherTextLength.length);
        currentIndex += cipherTextLength.length;

        System.arraycopy(masterKeyPathBytes, 0, encryptedColumnEncryptionKey, currentIndex, masterKeyPathBytes.length);
        currentIndex += masterKeyPathBytes.length;

        System.arraycopy(cipherText, 0, encryptedColumnEncryptionKey, currentIndex, cipherText.length);
        currentIndex += cipherText.length;

        System.arraycopy(signedHash, 0, encryptedColumnEncryptionKey, currentIndex, signedHash.length);

        javaKeyStoreLogger.exiting(SQLServerColumnEncryptionJavaKeyStoreProvider.class.getName(),
                Thread.currentThread().getStackTrace()[1].getMethodName(), "Finished encrypting Column Encryption Key.");
        return encryptedColumnEncryptionKey;

    }

    /**
     * Encrypt plainText with the certificate provided
     * 
     * @param plainText
     *            plain CEK to be encrypted
     * @param certificateDetails
     * @return encrypted CEK
     * @throws SQLServerException
     */
    private byte[] encryptRSAOAEP(byte[] plainText,
            CertificateDetails certificateDetails) throws SQLServerException {
        byte[] cipherText = null;
        try {
            Cipher rsa = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            rsa.init(Cipher.ENCRYPT_MODE, certificateDetails.certificate.getPublicKey());
            rsa.update(plainText);
            cipherText = rsa.doFinal();
        }
        catch (InvalidKeyException | NoSuchAlgorithmException | IllegalBlockSizeException | NoSuchPaddingException | BadPaddingException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_EncryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        return cipherText;

    }

    private byte[] rsaSignHashedData(byte[] dataToSign,
            CertificateDetails certificateDetails) throws SQLServerException {
        Signature signature;
        byte[] signedHash = null;

        try {
            signature = Signature.getInstance("SHA256withRSA");
            signature.initSign((PrivateKey) certificateDetails.privateKey);
            signature.update(dataToSign);
            signedHash = signature.sign();
        }
        catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_EncryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);

        }
        return signedHash;

    }

    private byte[] getLittleEndianBytesFromShort(short value) {
        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byte[] byteValue = byteBuffer.putShort(value).array();
        return byteValue;

    }

}
