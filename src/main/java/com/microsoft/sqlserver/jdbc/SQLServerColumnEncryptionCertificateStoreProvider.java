/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.security.Key;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.UnrecoverableKeyException;
import java.security.cert.CertificateEncodingException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;
import java.util.Base64;
import java.util.Enumeration;
import java.util.Locale;


/**
 * The implementation of the key store provider for the Windows Certificate Store. This class enables using keys stored in the Windows Certificate
 * Store as column master keys.
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
        }
        else {
            isWindows = false;
        }
    }
    private Path keyStoreDirectoryPath = null;

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

    public byte[] encryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] plainTextColumnEncryptionKey) throws SQLServerException {
        throw new SQLServerException(null, SQLServerException.getErrString("R_InvalidWindowsCertificateStoreEncryption"), null, 0, false);
    }

    private byte[] decryptColumnEncryptionKeyWindows(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        try {
            return AuthenticationJNI.DecryptColumnEncryptionKey(masterKeyPath, encryptionAlgorithm, encryptedColumnEncryptionKey);
        }
        catch (DLLException e) {
            DLLException.buildException(e.GetErrCode(), e.GetParam1(), e.GetParam2(), e.GetParam3());
            return null;
        }
    }

    private CertificateDetails getCertificateDetails(String masterKeyPath) throws SQLServerException {
        String storeLocation = null;

        String[] certParts = masterKeyPath.split("/");

        // Validate certificate path
        // Certificate path should only contain 3 parts (Certificate Location, Certificate Store Name and Thumbprint)
        if (certParts.length > 3) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AECertpathBad"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        // Extract the store location where the cert is stored
        if (certParts.length > 2) {
            if (certParts[0].equalsIgnoreCase(localMachineDirectory)) {
                storeLocation = localMachineDirectory;
            }
            else if (certParts[0].equalsIgnoreCase(currentUserDirectory)) {
                storeLocation = currentUserDirectory;
            }
            else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AECertLocBad"));
                Object[] msgArgs = {certParts[0], masterKeyPath};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }

        // Parse the certificate store name. Only store name "My" is supported.
        if (certParts.length > 1) {
            if (!certParts[certParts.length - 2].equalsIgnoreCase(myCertificateStore)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AECertStoreBad"));
                Object[] msgArgs = {certParts[certParts.length - 2], masterKeyPath};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }

        // Get thumpbrint
        String thumbprint = certParts[certParts.length - 1];
        if ((null == thumbprint) || (0 == thumbprint.length())) {
            // An empty thumbprint specified
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AECertHashEmpty"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        // Find the certificate and return
        return getCertificateByThumbprint(storeLocation, thumbprint, masterKeyPath);
    }

    private String getThumbPrint(X509Certificate cert) throws NoSuchAlgorithmException, CertificateEncodingException {
        MessageDigest md = MessageDigest.getInstance("SHA-1");
        byte[] der = cert.getEncoded();
        md.update(der);
        byte[] digest = md.digest();
        return Base64.getEncoder().encodeToString(digest);
    }

    private CertificateDetails getCertificateByThumbprint(String storeLocation,
            String thumbprint,
            String masterKeyPath) throws SQLServerException {
        FileInputStream fis;

        if ((null == keyStoreDirectoryPath)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AEKeyPathEmptyOrReserved"));
            Object[] msgArgs = {keyStoreDirectoryPath};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        Path keyStoreFullPath = keyStoreDirectoryPath.resolve(storeLocation);

        KeyStore keyStore = null;
        try {
            keyStore = KeyStore.getInstance("PKCS12");
        }
        catch (KeyStoreException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CertificateError"));
            Object[] msgArgs = {masterKeyPath, name};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        File keyStoreDirectory = keyStoreFullPath.toFile();
        File[] listOfFiles = keyStoreDirectory.listFiles();

        if ((null == listOfFiles) || (0 == listOfFiles.length)) {
            throw new SQLServerException(SQLServerException.getErrString("R_KeyStoreNotFound"), null);
        }

        for (File f : listOfFiles) {

            if (f.isDirectory()) {
                continue;
            }

            char[] password = "".toCharArray();
            try {
                fis = new FileInputStream(f);
                keyStore.load(fis, password);
            }
            catch (IOException | CertificateException | NoSuchAlgorithmException e) {
                // Cannot parse the current file, continue to the next.
                continue;
            }

            // If we are here, we were able to load a PKCS12 file.
            try {
                for (Enumeration<String> enumeration = keyStore.aliases(); enumeration.hasMoreElements();) {

                    String alias = enumeration.nextElement();

                    X509Certificate publicCertificate = (X509Certificate) keyStore.getCertificate(alias);

                    if (thumbprint.matches(getThumbPrint(publicCertificate))) {
                        // Found the right certificate
                        Key keyPrivate = null;
                        try {
                            keyPrivate = keyStore.getKey(alias, "".toCharArray());
                            if (null == keyPrivate) {
                                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnrecoverableKeyAE"));
                                Object[] msgArgs = {masterKeyPath};
                                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                            }
                        }
                        catch (UnrecoverableKeyException | NoSuchAlgorithmException | KeyStoreException e) {
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnrecoverableKeyAE"));
                            Object[] msgArgs = {masterKeyPath};
                            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                        }
                        return new CertificateDetails(publicCertificate, keyPrivate);
                    }
                }// end of for for alias
            }
            catch (CertificateException | NoSuchAlgorithmException | KeyStoreException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CertificateError"));
                Object[] msgArgs = {masterKeyPath, name};
                throw new SQLServerException(form.format(msgArgs), e);
            }
        }
        // Looped over all files, haven't found the certificate
        throw new SQLServerException(SQLServerException.getErrString("R_KeyStoreNotFound"), null);
    }

    public byte[] decryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        windowsCertificateStoreLogger.entering(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(), "decryptColumnEncryptionKey",
                "Decrypting Column Encryption Key.");
        byte[] plainCek;
        if (isWindows) {
            plainCek = decryptColumnEncryptionKeyWindows(masterKeyPath, encryptionAlgorithm, encryptedColumnEncryptionKey);
        }
        else {
            throw new SQLServerException(SQLServerException.getErrString("R_notSupported"), null);
        }
        windowsCertificateStoreLogger.exiting(SQLServerColumnEncryptionCertificateStoreProvider.class.getName(), "decryptColumnEncryptionKey",
                "Finished decrypting Column Encryption Key.");
        return plainCek;
    }
}
