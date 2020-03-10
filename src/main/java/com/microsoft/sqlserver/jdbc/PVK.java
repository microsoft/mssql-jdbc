/**
 * Copyright 2012 Emmanuel Bourg
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.microsoft.sqlserver.jdbc;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.util.Arrays;
import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;

/**
 * PVK file parser. Based on the documentation and the code from Stephen Norman Henson.
 * 
 * @see <a href="http://www.drh-consultancy.demon.co.uk/pvk.html">PVK file information</a>
 * @see <a href="https://github.com/openssl/openssl/blob/OpenSSL_1_1_0-stable/crypto/pem/pvkfmt.c">OpenSSL PVK format implementation</a>
 * 
 * @author Emmanuel Bourg
 * @since 1.0
 */
public class PVK {

    /** Header signature of PVK files */
    private static final long PVK_MAGIC = 0xB0B5F11EL;

    /** Header of the unencrypted key */
    private static final byte[] RSA2_KEY_MAGIC = "RSA2".getBytes();

    private PVK() {
    }

    public static PrivateKey parse(File file, String password) throws GeneralSecurityException, IOException {
        ByteBuffer buffer = ByteBuffer.allocate((int) file.length());
        
        try (FileInputStream in = new FileInputStream(file)) {
            in.getChannel().read(buffer);
            return parse(buffer, password);
        }
    }

    public static PrivateKey parse(ByteBuffer buffer, String password) throws GeneralSecurityException {
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        buffer.rewind();
        
        long magic = buffer.getInt() & 0xFFFFFFFFL;
        if (PVK_MAGIC != magic) {
            throw new IllegalArgumentException("PVK header signature not found");
        }
        
        buffer.position(buffer.position() + 4); // reserved
        int keyType = buffer.getInt();
        boolean encrypted = buffer.getInt() != 0;
        int saltLength = buffer.getInt();
        int keyLength = buffer.getInt();
        byte[] salt = new byte[saltLength];
        buffer.get(salt);
        
        // BLOBHEADER structure: https://msdn.microsoft.com/en-us/library/cc235198.aspx
        byte btype = buffer.get();
        byte version = buffer.get();
        buffer.position(buffer.position() + 2); // reserved
        int keyalg = buffer.getInt();
        
        byte[] key = new byte[keyLength - 8];
        buffer.get(key);
        
        if (encrypted) {
            key = decrypt(key, salt, password);
        }
        
        return parseKey(key);
    }

    private static byte[] decrypt(byte[] encoded, byte[] salt, String password) throws GeneralSecurityException {
        byte[] hash = deriveKey(salt, password);
        String algorithm = "RC4";
        
        SecretKey strongKey = new SecretKeySpec(hash, 0, 16, algorithm);
        byte[] decoded = decrypt(strongKey, encoded);
        if (startsWith(decoded, RSA2_KEY_MAGIC)) {
            return decoded;
        }
        
        // trim the key to 40 bits
        Arrays.fill(hash, 5, hash.length, (byte) 0);
        SecretKey weakKey = new SecretKeySpec(hash, 0, 16, algorithm);
        decoded = decrypt(weakKey, encoded);
        if (startsWith(decoded, RSA2_KEY_MAGIC)) {
            return decoded;
        }
        
        throw new IllegalArgumentException("Unable to decrypt the PVK key, please verify the password");
    }

    private static byte[] decrypt(SecretKey key, byte[] encoded) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(key.getAlgorithm());
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(encoded);
    }

    /**
     * Key derivation SHA1(salt + password)
     */
    private static byte[] deriveKey(byte[] salt, String password) throws NoSuchAlgorithmException {
        MessageDigest digest = MessageDigest.getInstance("SHA1");
        digest.update(salt);
        if (password != null) {
            digest.update(password.getBytes());
        }
        return digest.digest();
    }

    /**
     * Parses a private key blob.
     *
     * @see <a href="https://msdn.microsoft.com/en-us/library/windows/desktop/aa375601.aspx#priv_BLOB">MSDN - Private Key BLOBs</a>
     */
    private static PrivateKey parseKey(byte[] key) throws GeneralSecurityException {
        ByteBuffer buffer = ByteBuffer.wrap(key);
        buffer.order(ByteOrder.LITTLE_ENDIAN);
        
        if (!startsWith(key, RSA2_KEY_MAGIC)) {
            throw new IllegalArgumentException("Unable to parse the PVK key, unsupported key format: " + new String(key, 0, RSA2_KEY_MAGIC.length));
        }
        
        buffer.position(buffer.position() + RSA2_KEY_MAGIC.length); // skip the header
        
        int bitlength = buffer.getInt();
        BigInteger publicExponent = new BigInteger(String.valueOf(buffer.getInt()));
        
        int l = bitlength / 8;
        
        BigInteger modulus = getBigInteger(buffer, l);
        BigInteger primeP = getBigInteger(buffer, l / 2);
        BigInteger primeQ = getBigInteger(buffer, l / 2);
        BigInteger primeExponentP = getBigInteger(buffer, l / 2);
        BigInteger primeExponentQ = getBigInteger(buffer, l / 2);
        BigInteger crtCoefficient = getBigInteger(buffer, l / 2);
        BigInteger privateExponent = getBigInteger(buffer, l);
        
        RSAPrivateCrtKeySpec spec = new RSAPrivateCrtKeySpec(modulus, publicExponent, privateExponent, primeP, primeQ, primeExponentP, primeExponentQ, crtCoefficient);
        KeyFactory factory = KeyFactory.getInstance("RSA");
        
        return factory.generatePrivate(spec);
    }

    /**
     * Read a BigInteger from the specified buffer (in little-endian byte-order).
     * 
     * @param buffer the input buffer
     * @param length the number of bytes read
     */
    private static BigInteger getBigInteger(ByteBuffer buffer, int length) {
        byte[] array = new byte[length + 1];
        buffer.get(array, 0, length);
        
        return new BigInteger(reverse(array));
    }

    /**
     * Reverses the specified array.
     */
    private static byte[] reverse(byte[] array) {
        for (int i = 0; i < array.length / 2; i++) {
            byte b = array[i];
            array[i] = array[array.length - 1 - i];
            array[array.length - 1 - i] = b;
        }
        
        return array;
    }

    /**
     * Tells if an array starts with the specified prefix.
     */
    private static boolean startsWith(byte[] array, byte[] prefix) {
        for (int i = 0; i < prefix.length; i++) {
            if (prefix[i] != array[i]) {
                return false;
            }
        }
        
        return true;
    }
}





