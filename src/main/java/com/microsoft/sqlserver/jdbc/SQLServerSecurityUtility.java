/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.util.Iterator;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

/**
 * Various SQLServer security utilities.
 *
 */
class SQLServerSecurityUtility {

    /**
     * Give the hash of given plain text
     * 
     * @param plainText
     * @param key
     * @param length
     * @return hash of the plain text using provided key
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    static byte[] getHMACWithSHA256(byte[] plainText,
            byte[] key,
            int length) throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] computedHash;
        byte[] hash = new byte[length];
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec ivkeySpec = new SecretKeySpec(key, "HmacSHA256");
        mac.init(ivkeySpec);
        computedHash = mac.doFinal(plainText);
        // truncating hash if needed
        System.arraycopy(computedHash, 0, hash, 0, hash.length);
        return hash;
    }

    /**
     * Compare two arrays
     * 
     * @param buffer1
     *            first array
     * @param buffer2
     *            second array
     * @param buffer2Index
     * @param lengthToCompare
     * @return true if array contains same bytes otherwise false
     */
    static boolean compareBytes(byte[] buffer1,
            byte[] buffer2,
            int buffer2Index,
            int lengthToCompare) {
        if (null == buffer1 || null == buffer2) {
            return false;
        }

        if ((buffer2.length - buffer2Index) < lengthToCompare) {
            return false;
        }

        for (int index = 0; index < buffer1.length && index < lengthToCompare; ++index) {
            if (buffer1[index] != buffer2[buffer2Index + index]) {
                return false;
            }
        }
        return true;

    }

    /*
     * Encrypts the ciphertext.
     */
    static byte[] encryptWithKey(byte[] plainText,
            CryptoMetadata md,
            SQLServerConnection connection) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert serverName != null : "Server name should npt be null in EncryptWithKey";

        // Initialize cipherAlgo if not already done.
        if (!md.IsAlgorithmInitialized()) {
            SQLServerSecurityUtility.decryptSymmetricKey(md, connection);
        }

        assert md.IsAlgorithmInitialized();
        byte[] cipherText = md.cipherAlgorithm.encryptData(plainText); // this call succeeds or throws.
        if (null == cipherText || 0 == cipherText.length) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_NullCipherTextAE"), null, 0, false);
        }
        return cipherText;
    }

    /**
     * Return the algorithm name mapped to an Id
     * 
     * @param cipherAlgorithmId
     *            The cipher algorithm Id
     * @param cipherAlgorithmName
     *            The cipher algorithm name
     * @return The cipher algorithm name
     */
    private static String ValidateAndGetEncryptionAlgorithmName(byte cipherAlgorithmId,
            String cipherAlgorithmName) throws SQLServerException {
        // Custom cipher algorithm not supported for CTP.
        if (TDS.AEAD_AES_256_CBC_HMAC_SHA256 != cipherAlgorithmId) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_CustomCipherAlgorithmNotSupportedAE"), null, 0, false);
        }
        return SQLServerAeadAes256CbcHmac256Algorithm.algorithmName;
    }

    /**
     * Decrypts the symmetric key and saves it in metadata. In addition, initializes the SqlClientEncryptionAlgorithm for rapid decryption.
     * 
     * @param md
     *            The cipher metadata
     * @param connection
     *            The connection
     */
    static void decryptSymmetricKey(CryptoMetadata md,
            SQLServerConnection connection) throws SQLServerException {
        assert null != md : "md should not be null in DecryptSymmetricKey.";
        assert null != md.cekTableEntry : "md.EncryptionInfo should not be null in DecryptSymmetricKey.";
        assert null != md.cekTableEntry.columnEncryptionKeyValues : "md.EncryptionInfo.ColumnEncryptionKeyValues should not be null in DecryptSymmetricKey.";

        SQLServerSymmetricKey symKey = null;
        EncryptionKeyInfo encryptionkeyInfoChosen = null;
        SQLServerSymmetricKeyCache cache = SQLServerSymmetricKeyCache.getInstance();
        Iterator<EncryptionKeyInfo> it = md.cekTableEntry.columnEncryptionKeyValues.iterator();
        SQLServerException lastException = null;
        while (it.hasNext()) {
            EncryptionKeyInfo keyInfo = it.next();
            try {
                symKey = cache.getKey(keyInfo, connection);
                if (null != symKey) {
                    encryptionkeyInfoChosen = keyInfo;
                    break;
                }
            }
            catch (SQLServerException e) {
                lastException = e;
            }
        }

        if (null == symKey) {
            if (null != lastException) {
                throw lastException;
            }
            else {
                throw new SQLServerException(null, SQLServerException.getErrString("R_CEKDecryptionFailed"), null, 0, false);
            }
        }

        // Given the symmetric key instantiate a SqlClientEncryptionAlgorithm object and cache it in metadata.
        md.cipherAlgorithm = null;
        SQLServerEncryptionAlgorithm cipherAlgorithm = null;
        String algorithmName = ValidateAndGetEncryptionAlgorithmName(md.cipherAlgorithmId, md.cipherAlgorithmName); // may throw
        cipherAlgorithm = SQLServerEncryptionAlgorithmFactoryList.getInstance().getAlgorithm(symKey, md.encryptionType, algorithmName); // will
                                                                                                                                        // validate
                                                                                                                                        // algorithm
                                                                                                                                        // name and
                                                                                                                                        // type
        assert null != cipherAlgorithm : "Cipher algorithm cannot be null in DecryptSymmetricKey";
        md.cipherAlgorithm = cipherAlgorithm;
        md.encryptionKeyInfo = encryptionkeyInfoChosen;
    }

    /*
     * Decrypts the ciphertext.
     */
    static byte[] decryptWithKey(byte[] cipherText,
            CryptoMetadata md,
            SQLServerConnection connection) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert null != serverName : "serverName should not be null in DecryptWithKey.";

        // Initialize cipherAlgo if not already done.
        if (!md.IsAlgorithmInitialized()) {
            SQLServerSecurityUtility.decryptSymmetricKey(md, connection);
        }

        assert md.IsAlgorithmInitialized() : "Decryption Algorithm is not initialized";
        byte[] plainText = md.cipherAlgorithm.decryptData(cipherText); // this call succeeds or throws.
        if (null == plainText) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_PlainTextNullAE"), null, 0, false);
        }

        return plainText;
    }
}
