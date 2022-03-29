package com.microsoft.sqlserver.jdbc;

import java.security.SecureRandom;
import java.text.MessageFormat;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import javax.crypto.spec.IvParameterSpec;


/**
 * 
 * This is an utility class to encrypt/decrypt strings. This is used to obfuscate passwords so they won't be visible as
 * plaintext.
 */
final class SecureStringUtil {
    /* cipher transformation in the form of algorithm/mode/padding */
    static final String CIPHER_TRANSFORMATION = "AES/CBC/PKCS5Padding";

    /* key generator algorithm */
    static final String KEYGEN_ALGORITHEM = "AES";

    /* length of initialization vector buffer */
    static final int IV_LENGTH = 16;

    /* key size */
    static final int KEY_SIZE = 128;

    /* initialization vector params */
    private IvParameterSpec ivParamSpec;

    /** secret key for encryption/decryption */
    private SecretKey key;

    /* cryptographic cipher for encryption */
    private Cipher encryptCipher;

    /* cryptographic cipher for decryption */
    private Cipher decryptCipher;

    /* singleton instance */
    private static SecureStringUtil instance;

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
            instance = new SecureStringUtil();
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
        byte[] iv = new byte[IV_LENGTH];
        new SecureRandom().nextBytes(iv);
        ivParamSpec = new IvParameterSpec(iv);

        try {
            // init key */
            KeyGenerator keyGenerator = KeyGenerator.getInstance(KEYGEN_ALGORITHEM);
            keyGenerator.init(KEY_SIZE);
            key = keyGenerator.generateKey();

            encryptCipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
            encryptCipher.init(Cipher.ENCRYPT_MODE, key, ivParamSpec);
            decryptCipher = Cipher.getInstance(CIPHER_TRANSFORMATION);
            decryptCipher.init(Cipher.DECRYPT_MODE, key, ivParamSpec);

        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SecureStringInitFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
    }

    /**
     * Get encrypted value of given string
     * 
     * @param str
     *        string to encrypt
     * 
     * @return encrypted string
     * 
     * @throws SQLServerException
     *         if error
     */
    String getEncryptedString(String str) throws SQLServerException {
        try {
            byte[] cipherText = encryptCipher.doFinal(str.getBytes());
            return Base64.getEncoder().encodeToString(cipherText);
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_EncryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
    }

    /**
     * Get decrypted value of an encrypted string
     * 
     * @param str
     * 
     * @return decrypted string
     * 
     * @throws SQLServerException
     */
    String getDecryptedString(String str) throws SQLServerException {
        try {
            byte[] plainText = decryptCipher.doFinal(Base64.getDecoder().decode(str));
            return new String(plainText);
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_DecryptionFailed"));
            Object[] msgArgs = {e.getMessage()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }
    }
}
