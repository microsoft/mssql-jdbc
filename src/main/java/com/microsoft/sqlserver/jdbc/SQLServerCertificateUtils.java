/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringReader;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.util.Arrays;
import java.util.Base64;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;


final class SQLServerCertificateUtils {

    static KeyManager[] getKeyManagerFromFile(String certPath, String keyPath,
            String keyPassword) throws IOException, GeneralSecurityException, SQLServerException {
        if (keyPath != null && keyPath.length() > 0) {
            return readPKCS8Certificate(certPath, keyPath, keyPassword);
        } else {
            return readPKCS12Certificate(certPath, keyPassword);
        }
    }

    // PKCS#12 format
    private static final String PKCS12_ALG = "PKCS12";
    private static final String SUN_X_509 = "SunX509";
    // PKCS#8 format
    private static final String PEM_PRIVATE_START = "-----BEGIN PRIVATE KEY-----";
    private static final String PEM_PRIVATE_END = "-----END PRIVATE KEY-----";
    private static final String JAVA_KEY_STORE = "JKS";
    private static final String CLIENT_CERT = "client-cert";
    private static final String CLIENT_KEY = "client-key";
    // PKCS#1 format
    private static final String PEM_RSA_PRIVATE_START = "-----BEGIN RSA PRIVATE KEY-----";
    // PVK format
    private static final long PVK_MAGIC = 0xB0B5F11EL;
    private static final byte[] RSA2_MAGIC = {82, 83, 65, 50};
    private static final String RC4_ALG = "RC4";
    private static final String RSA_ALG = "RSA";

    private static KeyManager[] readPKCS12Certificate(String certPath,
            String keyPassword) throws NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException, KeyStoreException {
        KeyStore keystore = KeyStore.getInstance(PKCS12_ALG);
        keystore.load(new FileInputStream(certPath), keyPassword.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(SUN_X_509);
        keyManagerFactory.init(keystore, keyPassword.toCharArray());
        return keyManagerFactory.getKeyManagers();
    }

    private static KeyManager[] readPKCS8Certificate(String certPath, String keyPath,
            String keyPassword) throws IOException, GeneralSecurityException, SQLServerException {
        Certificate clientCertificate = loadCertificate(certPath);
        PrivateKey privateKey = loadPrivateKey(keyPath, keyPassword);

        KeyStore keyStore = KeyStore.getInstance(JAVA_KEY_STORE);
        keyStore.load(null, null);
        keyStore.setCertificateEntry(CLIENT_CERT, clientCertificate);
        keyStore.setKeyEntry(CLIENT_KEY, privateKey, keyPassword.toCharArray(), new Certificate[] {clientCertificate});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    private static PrivateKey loadPrivateKeyFromPKCS8(
            String key) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        StringBuilder sb = new StringBuilder(key);
        deleteFirst(sb, PEM_PRIVATE_START);
        deleteFirst(sb, PEM_PRIVATE_END);
        replaceAll(sb, "\\s", "");
        byte[] pkcs8EncodedKey = Base64.getDecoder().decode(sb.toString());

        KeyFactory factory = KeyFactory.getInstance(RSA_ALG);
        return factory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8EncodedKey));
    }

    private static void deleteFirst(StringBuilder sb, String str) {
        int i = sb.indexOf(str);
        if (i != -1) {
            sb.delete(i, i + str.length());
        }
    }

    private static void replaceAll(StringBuilder sb, String oldStr, String newStr) {
        int index = sb.indexOf(oldStr);
        while (index != -1) {
            sb.replace(index, index + oldStr.length(), newStr);
            index += newStr.length();
            index = sb.indexOf(oldStr, index);
        }
    }

    private static PrivateKey loadPrivateKeyFromPKCS1(String key,
            String keyPass) throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
        PEMParser pemParser = null;
        try {
            pemParser = new PEMParser(new StringReader(key));
        } catch (Exception e) {
            SQLServerBouncyCastleLoader.loadBouncyCastle();
            pemParser = new PEMParser(new StringReader(key));
        }

        try {
            Object object = pemParser.readObject();
            JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
            KeyPair kp;
            if (object instanceof PEMEncryptedKeyPair && keyPass != null) {
                PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder().build(keyPass.toCharArray());
                kp = converter.getKeyPair(((PEMEncryptedKeyPair) object).decryptKeyPair(decProv));
            } else {
                kp = converter.getKeyPair((PEMKeyPair) object);
            }
            return kp.getPrivate();
        } finally {
            pemParser.close();
        }
    }

    private static PrivateKey loadPrivateKeyFromPVK(String keyPath,
            String keyPass) throws IOException, GeneralSecurityException, SQLServerException {
        File f = new File(keyPath);
        ByteBuffer buffer = ByteBuffer.allocate((int) f.length());
        try (FileInputStream in = new FileInputStream(f)) {
            in.getChannel().read(buffer);
            buffer.order(ByteOrder.LITTLE_ENDIAN).rewind();

            long magic = buffer.getInt() & 0xFFFFFFFFL;
            if (PVK_MAGIC != magic) {
                SQLServerException.makeFromDriverError(null, magic, SQLServerResource.getResource("R_pvkHeaderError"),
                        "", false);
            }

            buffer.position(buffer.position() + 8); // skip reserved and keytype
            boolean encrypted = buffer.getInt() != 0;
            int saltLength = buffer.getInt();
            int keyLength = buffer.getInt();
            byte[] salt = new byte[saltLength];
            buffer.get(salt);

            buffer.position(buffer.position() + 8); // skip btype(1b), version(1b), reserved(2b), and keyalg(4b)

            byte[] key = new byte[keyLength - 8];
            buffer.get(key);

            if (encrypted) {
                MessageDigest digest = MessageDigest.getInstance("SHA1");
                digest.update(salt);
                if (keyPass != null) {
                    digest.update(keyPass.getBytes());
                }
                byte[] hash = digest.digest();
                key = getSecretKeyFromHash(key, hash);
            }

            ByteBuffer buff = ByteBuffer.wrap(key).order(ByteOrder.LITTLE_ENDIAN);
            buff.position(RSA2_MAGIC.length); // skip the header

            int byteLength = buff.getInt() / 8;
            BigInteger publicExponent = BigInteger.valueOf(buff.getInt());
            BigInteger modulus = getBigInteger(buff, byteLength);
            BigInteger prime1 = getBigInteger(buff, byteLength / 2);
            BigInteger prime2 = getBigInteger(buff, byteLength / 2);
            BigInteger primeExponent1 = getBigInteger(buff, byteLength / 2);
            BigInteger primeExponent2 = getBigInteger(buff, byteLength / 2);
            BigInteger crtCoefficient = getBigInteger(buff, byteLength / 2);
            BigInteger privateExponent = getBigInteger(buff, byteLength);

            RSAPrivateCrtKeySpec spec = new RSAPrivateCrtKeySpec(modulus, publicExponent, privateExponent, prime1,
                    prime2, primeExponent1, primeExponent2, crtCoefficient);
            KeyFactory factory = KeyFactory.getInstance(RSA_ALG);
            return factory.generatePrivate(spec);
        }
    }

    private static Certificate loadCertificate(String certificatePem) throws IOException, GeneralSecurityException {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X509");
        InputStream certstream = fileToStream(certificatePem);
        return certificateFactory.generateCertificate(certstream);
    }

    private static PrivateKey loadPrivateKey(String privateKeyPemPath,
            String privateKeyPassword) throws GeneralSecurityException, IOException, SQLServerException {
        String privateKeyPem = getStringFromFile(privateKeyPemPath);

        if (privateKeyPem.contains(PEM_PRIVATE_START)) { // PKCS#8 format
            return loadPrivateKeyFromPKCS8(privateKeyPem);
        } else if (privateKeyPem.contains(PEM_RSA_PRIVATE_START)) { // PKCS#1 format
            return loadPrivateKeyFromPKCS1(privateKeyPem, privateKeyPassword);
        } else {
            return loadPrivateKeyFromPVK(privateKeyPemPath, privateKeyPassword);
        }
    }

    private static boolean startsWithMagic(byte[] b) {
        for (int i = 0; i < RSA2_MAGIC.length; i++) {
            if (b[i] != RSA2_MAGIC[i])
                return false;
        }
        return true;
    }

    private static byte[] getSecretKeyFromHash(byte[] originalKey,
            byte[] keyHash) throws GeneralSecurityException, SQLServerException {
        SecretKey key = new SecretKeySpec(keyHash, 0, 16, RC4_ALG);
        byte[] decrypted = decryptSecretKey(key, originalKey);
        if (startsWithMagic(decrypted)) {
            return decrypted;
        }

        // Couldn't find magic due to padding, trim the key
        Arrays.fill(keyHash, 5, keyHash.length, (byte) 0);
        key = new SecretKeySpec(keyHash, 0, 16, RC4_ALG);
        decrypted = decryptSecretKey(key, originalKey);
        if (startsWithMagic(decrypted)) {
            return decrypted;
        }

        SQLServerException.makeFromDriverError(null, originalKey, SQLServerResource.getResource("R_pvkParseError"), "",
                false);
        return null;
    }

    private static byte[] decryptSecretKey(SecretKey key, byte[] encoded) throws GeneralSecurityException {
        Cipher cipher = Cipher.getInstance(key.getAlgorithm());
        cipher.init(Cipher.DECRYPT_MODE, key);
        return cipher.doFinal(encoded);
    }

    private static BigInteger getBigInteger(ByteBuffer buffer, int length) {
        // Add an extra bit for signum
        byte[] array = new byte[length + 1];
        // Write in reverse because our buffer was set to Little Endian
        for (int i = 0; i < length; i++) {
            array[array.length - 1 - i] = buffer.get();
        }
        return new BigInteger(array);
    }

    private static InputStream fileToStream(String fname) throws IOException {
        FileInputStream fis = null;
        DataInputStream dis = null;
        try {
            fis = new FileInputStream(fname);
            dis = new DataInputStream(fis);
            byte[] bytes = new byte[dis.available()];
            dis.readFully(bytes);
            ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
            return bais;
        } finally {
            dis.close();
            fis.close();
        }
    }

    private static String getStringFromFile(String filePath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filePath)));
    }
}
