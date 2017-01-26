/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.text.MessageFormat;

import javax.crypto.BadPaddingException;
import javax.crypto.Cipher;
import javax.crypto.IllegalBlockSizeException;
import javax.crypto.Mac;
import javax.crypto.NoSuchPaddingException;
import javax.crypto.ShortBufferException;
import javax.crypto.spec.IvParameterSpec;
import javax.crypto.spec.SecretKeySpec;

/**
 * 
 * This class implements authenticated encryption with associated data (AEAD_AES_256_CBC_HMAC_SHA256) algorithm specified at
 * <a href="http://tools.ietf.org/html/draft-mcgrew-aead-aes-cbc-hmac-sha2-05"> http://tools.ietf.org/html/draft-mcgrew-aead-aes-cbc-hmac-sha2-05</a>
 *
 */
class SQLServerAeadAes256CbcHmac256Algorithm extends SQLServerEncryptionAlgorithm {

    static final private java.util.logging.Logger aeLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerAeadAes256CbcHmac256Algorithm");

    final static String algorithmName = "AEAD_AES_256_CBC_HMAC_SHA256";
    // Stores column encryption key which includes root key and derived keys
    private SQLServerAeadAes256CbcHmac256EncryptionKey columnEncryptionkey;
    private byte algorithmVersion;
    // This variable indicate whether encryption type is deterministic (if true)
    // or random (if false)
    private boolean isDeterministic = false;
    // Each block in the AES is 128 bits
    private int blockSizeInBytes = 16;
    private int keySizeInBytes = SQLServerAeadAes256CbcHmac256EncryptionKey.keySize / 8;
    private byte[] version = new byte[] {0x01};
    // Added so that java hashing algorithm is similar to c#
    private byte[] versionSize = new byte[] {1};

    /*
     * Minimum Length of cipherText without authentication tag. This value is 1 (version byte) + 16 (IV) + 16 (minimum of 1 block of cipher Text)
     */
    private int minimumCipherTextLengthInBytesNoAuthenticationTag = 1 + blockSizeInBytes + blockSizeInBytes;

    /*
     * Minimum Length of cipherText. This value is 1 (version byte) + 32 (authentication tag) + 16 (IV) + 16 (minimum of 1 block of cipher Text)
     */
    private int minimumCipherTextLengthInBytesWithAuthenticationTag = minimumCipherTextLengthInBytesNoAuthenticationTag + keySizeInBytes;

    /**
     * Initializes a new instance of SQLServerAeadAes256CbcHmac256Algorithm with a given key, encryption type and algorithm version
     * 
     * @param columnEncryptionkey
     *            Root encryption key from which three other keys will be derived
     * @param encryptionType
     *            Encryption Type, accepted values are Deterministic and Randomized.
     * @param algorithmVersion
     *            Algorithm version
     */
    SQLServerAeadAes256CbcHmac256Algorithm(SQLServerAeadAes256CbcHmac256EncryptionKey columnEncryptionkey,
            SQLServerEncryptionType encryptionType,
            byte algorithmVersion) {
        this.columnEncryptionkey = columnEncryptionkey;

        if (encryptionType == SQLServerEncryptionType.Deterministic) {
            this.isDeterministic = true;
        }
        this.algorithmVersion = algorithmVersion;
        version[0] = algorithmVersion;
    }

    @Override
    byte[] encryptData(byte[] plainText) throws SQLServerException {
        // hasAuthenticationTag is true for this algorithm
        return encryptData(plainText, true);
    }

    /**
     * Performs encryption of plain text
     * 
     * @param plainText
     *            text to be encrypted
     * @param hasAuthenticationTag
     *            specify if encryption needs authentication
     * @return cipher text
     * @throws SQLServerException
     */
    protected byte[] encryptData(byte[] plainText,
            boolean hasAuthenticationTag) throws SQLServerException {
        aeLogger.entering(SQLServerAeadAes256CbcHmac256Algorithm.class.getName(), Thread.currentThread().getStackTrace()[1].getMethodName(),
                "Encrypting data.");
        // we will generate this initialization vector based whether
        // this encryption type is deterministic
        assert (plainText != null);
        byte[] iv = new byte[blockSizeInBytes];
        // Secret/private key to be used in AES encryption
        SecretKeySpec skeySpec = new SecretKeySpec(columnEncryptionkey.getEncryptionKey(), "AES");

        if (isDeterministic) {
            // this method makes sure this is 16 bytes key
            try {
                iv = SQLServerSecurityUtility.getHMACWithSHA256(plainText, columnEncryptionkey.getIVKey(), blockSizeInBytes);
            }
            catch (InvalidKeyException | NoSuchAlgorithmException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_EncryptionFailed"));
                Object[] msgArgs = {e.getMessage()};
                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
            }
        }
        else {
            SecureRandom random = new SecureRandom();
            random.nextBytes(iv);
        }

        int numBlocks = plainText.length / blockSizeInBytes + 1;

        int hmacStartIndex = 1;
        int authenticationTagLen = hasAuthenticationTag ? keySizeInBytes : 0;
        int ivStartIndex = hmacStartIndex + authenticationTagLen;
        int cipherStartIndex = ivStartIndex + blockSizeInBytes;

        // Output buffer size = size of VersionByte + Authentication Tag + IV + cipher Text blocks.
        int outputBufSize = 1 + authenticationTagLen + iv.length + (numBlocks * blockSizeInBytes);
        byte[] outBuffer = new byte[outputBufSize];

        // Copying the version to output buffer
        outBuffer[0] = algorithmVersion;
        // Coping IV to the output buffer
        System.arraycopy(iv, 0, outBuffer, ivStartIndex, iv.length);

        // Start the AES encryption

        try {
            // initialization vector
            IvParameterSpec ivector = new IvParameterSpec(iv);
            Cipher encryptCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            encryptCipher.init(Cipher.ENCRYPT_MODE, skeySpec, ivector);

            int count = 0;
            int cipherIndex = cipherStartIndex; // this is where cipherText starts

            if (numBlocks > 1) {
                count = (numBlocks - 1) * blockSizeInBytes;
                cipherIndex += encryptCipher.update(plainText, 0, count, outBuffer, cipherIndex);
            }
            // doFinal will complete the encryption
            byte[] buffTmp = encryptCipher.doFinal(plainText, count, plainText.length - count);
            // Encryption completed
            System.arraycopy(buffTmp, 0, outBuffer, cipherIndex, buffTmp.length);

            if (hasAuthenticationTag) {

                Mac hmac = Mac.getInstance("HmacSHA256");
                SecretKeySpec initkey = new SecretKeySpec(columnEncryptionkey.getMacKey(), "HmacSHA256");
                hmac.init(initkey);
                hmac.update(version, 0, version.length);
                hmac.update(iv, 0, iv.length);
                hmac.update(outBuffer, cipherStartIndex, numBlocks * blockSizeInBytes);
                hmac.update(versionSize, 0, version.length);
                byte[] hash = hmac.doFinal();
                // coping the authentication tag in the output buffer which holds cipher text
                System.arraycopy(hash, 0, outBuffer, hmacStartIndex, authenticationTagLen);

            }
        }
        catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchPaddingException
                | IllegalBlockSizeException | BadPaddingException | ShortBufferException e) {

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_EncryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        aeLogger.exiting(SQLServerAeadAes256CbcHmac256Algorithm.class.getName(), Thread.currentThread().getStackTrace()[1].getMethodName(),
                "Data encrypted.");
        return outBuffer;

    }

    @Override
    byte[] decryptData(byte[] cipherText) throws SQLServerException {
        return decryptData(cipherText, true);

    }

    /**
     * Decrypt the cipher text and return plain text
     * 
     * @param cipherText
     *            data to be decrypted
     * @param hasAuthenticationTag
     *            tells whether cipher text contain authentication tag
     * @return plain text
     * @throws SQLServerException
     */
    private byte[] decryptData(byte[] cipherText,
            boolean hasAuthenticationTag) throws SQLServerException {
        assert (cipherText != null);

        byte[] iv = new byte[blockSizeInBytes];

        int minimumCipherTextLength = hasAuthenticationTag ? minimumCipherTextLengthInBytesWithAuthenticationTag
                : minimumCipherTextLengthInBytesNoAuthenticationTag;

        // Here we check if length of cipher text is more than minimum value,
        // if not exception is thrown
        if (cipherText.length < minimumCipherTextLength) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidCipherTextSize"));
            Object[] msgArgs = {cipherText.length, minimumCipherTextLength};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);

        }

        // Validate the version byte
        int startIndex = 0;
        if (cipherText[startIndex] != algorithmVersion) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidAlgorithmVersion"));
            // converting byte to Hexa Decimal
            Object[] msgArgs = {String.format("%02X ", cipherText[startIndex]), String.format("%02X ", algorithmVersion)};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);

        }

        startIndex += 1;
        int authenticationTagOffset = 0;

        // Read authentication tag
        if (hasAuthenticationTag) {
            authenticationTagOffset = startIndex;
            // authentication tag size is keySizeInBytes
            startIndex += keySizeInBytes;
        }

        // Read IV from cipher text
        System.arraycopy(cipherText, startIndex, iv, 0, iv.length);
        startIndex += iv.length;

        // To read encrypted text from cipher
        int cipherTextOffset = startIndex;
        // All data after IV is encrypted data
        int cipherTextCount = cipherText.length - startIndex;

        if (hasAuthenticationTag) {
            byte[] authenticationTag;
            try {
                authenticationTag = prepareAuthenticationTag(iv, cipherText, cipherTextOffset, cipherTextCount);
            }
            catch (InvalidKeyException | NoSuchAlgorithmException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_DecryptionFailed"));
                Object[] msgArgs = {e.getMessage()};
                throw new SQLServerException(this, form.format(msgArgs), null, 0, false);

            }
            if (!(SQLServerSecurityUtility.compareBytes(authenticationTag, cipherText, authenticationTagOffset, cipherTextCount))) {

                throw new SQLServerException(this, SQLServerException.getErrString("R_InvalidAuthenticationTag"), null, 0, false);

            }

        }

        // Decrypt the text and return
        return decryptData(iv, cipherText, cipherTextOffset, cipherTextCount);
    }

    /**
     * Decrypt data with specified IV
     * 
     * @param iv
     *            initialization vector
     * @param cipherText
     *            text to be decrypted
     * @param offset
     *            of cipher text
     * @param count
     *            length of cipher text
     * @return plain text
     * @throws SQLServerException
     */
    private byte[] decryptData(byte[] iv,
            byte[] cipherText,
            int offset,
            int count) throws SQLServerException {
        aeLogger.entering(SQLServerAeadAes256CbcHmac256Algorithm.class.getName(), Thread.currentThread().getStackTrace()[1].getMethodName(),
                "Decrypting data.");
        assert (cipherText != null);
        assert (iv != null);
        byte[] plainText = null;
        // key to be used for decryption
        SecretKeySpec skeySpec = new SecretKeySpec(columnEncryptionkey.getEncryptionKey(), "AES");
        IvParameterSpec ivector = new IvParameterSpec(iv);
        Cipher decryptCipher;
        try {
            // AES encryption CBC mode and PKCS5 padding
            decryptCipher = Cipher.getInstance("AES/CBC/PKCS5Padding");
            decryptCipher.init(Cipher.DECRYPT_MODE, skeySpec, ivector);
            plainText = decryptCipher.doFinal(cipherText, offset, count);
        }
        catch (NoSuchAlgorithmException | InvalidAlgorithmParameterException | InvalidKeyException | NoSuchPaddingException
                | IllegalBlockSizeException | BadPaddingException e) {

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_DecryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        aeLogger.exiting(SQLServerAeadAes256CbcHmac256Algorithm.class.getName(), Thread.currentThread().getStackTrace()[1].getMethodName(),
                "Data decrypted.");
        return plainText;

    }

    /**
     * Prepare the authentication tag
     * 
     * @param iv
     *            initialization vector
     * @param cipherText
     * @param offset
     * @param length
     *            length of cipher text
     * @return authentication tag
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    private byte[] prepareAuthenticationTag(byte[] iv,
            byte[] cipherText,
            int offset,
            int length) throws NoSuchAlgorithmException, InvalidKeyException {
        assert (cipherText != null);
        byte[] computedHash;
        byte[] authenticationTag = new byte[keySizeInBytes];

        Mac hmac = Mac.getInstance("HmacSHA256");
        SecretKeySpec key = new SecretKeySpec(columnEncryptionkey.getMacKey(), "HmacSHA256");
        hmac.init(key);
        hmac.update(version, 0, version.length);
        hmac.update(iv, 0, iv.length);
        hmac.update(cipherText, offset, length);
        hmac.update(versionSize, 0, version.length);
        computedHash = hmac.doFinal();
        System.arraycopy(computedHash, 0, authenticationTag, 0, authenticationTag.length);

        return authenticationTag;

    }

}
