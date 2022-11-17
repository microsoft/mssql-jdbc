package com.microsoft.sqlserver.jdbc;

import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;


/**
 * 
 * This is an utility class to encrypt/decrypt strings. This is used to obfuscate passwords so they won't be visible as
 * plaintext.
 */
final class SecureStringUtil {
    /* cipher transformation in the form of algorithm/mode/padding */
    static final String CIPHER_TRANSFORMATION = "AES/GCM/NoPadding";

    /* key generator algorithm */
    static final String KEYGEN_ALGORITHEM = "AES";

    /* length of initialization vector buffer */
    static final int IV_LENGTH = 12;

    /* key size */
    static final int KEY_SIZE = 256;

    /* authentication tag length in bits */
    static final int TAG_LENGTH = 16;

    /** secret key for encryption/decryption */
    SecretKeySpec secretKey;

    /* cryptographic cipher for encryption */
    private Cipher encryptCipher;

    /* cryptographic cipher for decryption */
    private Cipher decryptCipher;

    /* singleton instance */
    private static volatile SecureStringUtil instance;

    private static final Lock INSTANCE_LOCK = new ReentrantLock();
    private static final Lock ENCRYPT_LOCK = new ReentrantLock();
    private static final Lock DECRYPT_LOCK = new ReentrantLock();

    /**
     * Get reference to SecureStringUtil instance
     * 
     * @return the SecureStringUtil instance
     * 
     * @throws SQLServerException
     *         if error
     */
    static SecureStringUtil getInstance() throws SQLServerException {
        if (instance == null) {
            INSTANCE_LOCK.lock();
            try {
                if (instance == null) {
                    instance = new SecureStringUtil();
                }
            } finally {
                INSTANCE_LOCK.unlock();
            }
        }
        return instance;
    }

    /**
     * Creates an instance of the SecureStringUtil object and initialize values to encrypt/decrypt strings
     * 
     * @throws SQLServerException
     *         if error
     */
    private SecureStringUtil() throws SQLServerException {
        try {
            // generate key */
            KeyGenerator keygen = KeyGenerator.getInstance(KEYGEN_ALGORITHEM);
            keygen.init(KEY_SIZE);
            secretKey = new SecretKeySpec(keygen.generateKey().getEncoded(), "AES");

            // get ciphers for encryption/decryption
            encryptCipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
            decryptCipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SecureStringInitFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
    }

    /**
     * Get encrypted value of given string
     * 
     * @param chars
     *        string to encrypt
     * 
     * @return encrypted string
     * 
     * @throws SQLServerException
     *         Throws an exception if the method fails to encrypt the character array
     */
    byte[] getEncryptedBytes(char[] chars) throws SQLServerException {
        ENCRYPT_LOCK.lock();
        try {
            if (chars == null) {
                return null;
            }

            byte[] iv = new byte[IV_LENGTH];
            SecureRandom random = new SecureRandom();
            random.nextBytes(iv);
            GCMParameterSpec ivParamSpec = new GCMParameterSpec(TAG_LENGTH * 8, iv);
            encryptCipher.init(Cipher.ENCRYPT_MODE, secretKey, ivParamSpec);

            byte[] cipherText = encryptCipher.doFinal(Util.charsToBytes(chars));
            byte[] bytes = new byte[iv.length + cipherText.length];
            System.arraycopy(iv, 0, bytes, 0, iv.length);
            System.arraycopy(cipherText, 0, bytes, iv.length, cipherText.length);
            return bytes;
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_EncryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        } finally {
            ENCRYPT_LOCK.unlock();
        }
    }

    /**
     * Get decrypted value of an encrypted string
     * 
     * @param bytes
     *        The byte array to decrypt into a character array
     * 
     * @return decrypted string
     * 
     * @throws SQLServerException
     *         Throws an exception if the method fails to decrypt the byte array
     */
    char[] getDecryptedChars(byte[] bytes) throws SQLServerException {
        DECRYPT_LOCK.lock();
        byte[] plainText = null;
        try {
            if (bytes == null) {
                return null;
            }

            byte[] iv = new byte[IV_LENGTH];
            System.arraycopy(bytes, 0, iv, 0, IV_LENGTH);

            GCMParameterSpec ivParamSpec = new GCMParameterSpec(TAG_LENGTH * 8, iv);
            decryptCipher.init(Cipher.DECRYPT_MODE, secretKey, ivParamSpec);

            plainText = decryptCipher.doFinal(bytes, IV_LENGTH, bytes.length - IV_LENGTH);
            return Util.bytesToChars(plainText);
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_DecryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        } finally {
            if (plainText != null) {
                Arrays.fill(plainText, (byte) 0);
            }
            DECRYPT_LOCK.unlock();
        }
    }
}
