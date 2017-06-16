/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.InvalidKeyException;
import java.security.Key;
import java.security.NoSuchAlgorithmException;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.X509Certificate;
import java.text.MessageFormat;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.NoSuchPaddingException;

/**
 * 
 * This class holds information about the certificate
 *
 */
class CertificateDetails {
    X509Certificate certificate;
    Key privateKey;

    CertificateDetails(X509Certificate certificate,
            Key privateKey) {
        this.certificate = certificate;
        this.privateKey = privateKey;
    }
}

class KeyStoreProviderCommon {

    static final String rsaEncryptionAlgorithmWithOAEP = "RSA_OAEP";
    static byte[] version = new byte[] {0x01};

    static void validateEncryptionAlgorithm(String encryptionAlgorithm,
            boolean isEncrypt) throws SQLServerException {
        String errString = isEncrypt ? "R_NullKeyEncryptionAlgorithm" : "R_NullKeyEncryptionAlgorithmInternal";
        if (null == encryptionAlgorithm) {

            throw new SQLServerException(null, SQLServerException.getErrString(errString), null, 0, false);

        }

        errString = isEncrypt ? "R_InvalidKeyEncryptionAlgorithm" : "R_InvalidKeyEncryptionAlgorithmInternal";
        if (!rsaEncryptionAlgorithmWithOAEP.equalsIgnoreCase(encryptionAlgorithm.trim())) {

            MessageFormat form = new MessageFormat(SQLServerException.getErrString(errString));
            Object[] msgArgs = {encryptionAlgorithm, rsaEncryptionAlgorithmWithOAEP};
            throw new SQLServerException(form.format(msgArgs), null);

        }
    }

    static void validateNonEmptyMasterKeyPath(String masterKeyPath) throws SQLServerException {
        if (null == masterKeyPath || masterKeyPath.trim().length() == 0) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_InvalidMasterKeyDetails"), null, 0, false);
        }
    }

    static byte[] decryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey,
            CertificateDetails certificateDetails) throws SQLServerException {
        if (null == encryptedColumnEncryptionKey) {

            throw new SQLServerException(null, SQLServerException.getErrString("R_NullEncryptedColumnEncryptionKey"), null, 0, false);

        }
        else if (0 == encryptedColumnEncryptionKey.length) {

            throw new SQLServerException(null, SQLServerException.getErrString("R_EmptyEncryptedColumnEncryptionKey"), null, 0, false);

        }

        validateEncryptionAlgorithm(encryptionAlgorithm, false);

        int currentIndex = version.length;
        int keyPathLength = convertTwoBytesToShort(encryptedColumnEncryptionKey, currentIndex);
        // We just read 2 bytes
        currentIndex += 2;

        // Get ciphertext length
        int cipherTextLength = convertTwoBytesToShort(encryptedColumnEncryptionKey, currentIndex);
        currentIndex += 2;

        currentIndex += keyPathLength;

        int signatureLength = encryptedColumnEncryptionKey.length - currentIndex - cipherTextLength;

        // Get ciphertext
        byte[] cipherText = new byte[cipherTextLength];
        System.arraycopy(encryptedColumnEncryptionKey, currentIndex, cipherText, 0, cipherTextLength);
        currentIndex += cipherTextLength;

        byte[] signature = new byte[signatureLength];
        System.arraycopy(encryptedColumnEncryptionKey, currentIndex, signature, 0, signatureLength);

        byte[] hash = new byte[encryptedColumnEncryptionKey.length - signature.length];

        System.arraycopy(encryptedColumnEncryptionKey, 0, hash, 0, encryptedColumnEncryptionKey.length - signature.length);

        if (!verifyRSASignature(hash, signature, certificateDetails.certificate, masterKeyPath)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidCertificateSignature"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        byte[] plainCEK = decryptRSAOAEP(cipherText, certificateDetails);

        return plainCEK;
    }

    private static byte[] decryptRSAOAEP(byte[] cipherText,
            CertificateDetails certificateDetails) throws SQLServerException {
        byte[] plainCEK = null;
        try {
            Cipher rsa = Cipher.getInstance("RSA/ECB/OAEPWithSHA-1AndMGF1Padding");
            rsa.init(Cipher.DECRYPT_MODE, certificateDetails.privateKey);
            rsa.update(cipherText);
            plainCEK = rsa.doFinal();
        }
        catch (InvalidKeyException | NoSuchAlgorithmException | NoSuchPaddingException | IllegalBlockSizeException | BadPaddingException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CEKDecryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(form.format(msgArgs), e);
        }

        return plainCEK;

    }

    private static boolean verifyRSASignature(byte[] hash,
            byte[] signature,
            X509Certificate certificate,
            String masterKeyPath) throws SQLServerException {
        Signature signVerify;
        boolean verificationSucess = false;
        try {
            signVerify = Signature.getInstance("SHA256withRSA");
            signVerify.initVerify(certificate.getPublicKey());
            signVerify.update(hash);
            verificationSucess = signVerify.verify(signature);
        }
        catch (InvalidKeyException | NoSuchAlgorithmException | SignatureException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidCertificateSignature"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(form.format(msgArgs), e);
        }

        return verificationSucess;

    }

    private static short convertTwoBytesToShort(byte[] input,
            int index) throws SQLServerException {

        short shortVal;
        if (index + 1 >= input.length) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_ByteToShortConversion"), null, 0, false);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put(input[index]);
        byteBuffer.put(input[index + 1]);
        shortVal = byteBuffer.getShort(0);
        return shortVal;

    }
}
