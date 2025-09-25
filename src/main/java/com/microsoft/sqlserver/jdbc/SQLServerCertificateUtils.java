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
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
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
import java.security.cert.X509Certificate;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.RSAPrivateCrtKeySpec;
import java.text.MessageFormat;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collection;
import java.util.List;
import java.util.Locale;
import java.util.logging.Level;
import java.util.logging.Logger;

import javax.crypto.Cipher;
import javax.crypto.SecretKey;
import javax.crypto.spec.SecretKeySpec;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.naming.ldap.LdapName;
import javax.naming.ldap.Rdn;
import javax.security.auth.x500.X500Principal;

import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;


final class SQLServerCertificateUtils {

    private static final Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.SQLServerCertificateUtils");
    private static final String logContext = Thread.currentThread().getStackTrace()[1].getClassName() + ": ";

    private SQLServerCertificateUtils() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    static KeyManager[] getKeyManagerFromFile(String certPath, String keyPath,
            String keyPassword) throws IOException, GeneralSecurityException, SQLServerException {
        if (keyPath != null && keyPath.length() > 0) {
            return readPKCS8Certificate(certPath, keyPath, keyPassword);
        } else {
            return readPKCS12Certificate(certPath, keyPassword);
        }
    }

    /**
     * Validate server name in certificate matches hostname
     * 
     * @param nameInCert
     *        server name in certificate
     * @param hostName
     *        hostname
     * @return if the server name is valid and matches hostname
     */
    static boolean validateServerName(String nameInCert, String hostName) {
        // Failed to get the common name from DN or empty CN
        if (null == nameInCert) {
            if (logger.isLoggable(Level.FINER)) {
                logger.finer(logContext + " Failed to parse the name from the certificate or name is empty.");
            }
            return false;
        }
        // We do not allow wildcards in IDNs (xn--).
        if (!nameInCert.startsWith("xn--") && nameInCert.contains("*")) {
            int hostIndex = 0, certIndex = 0, match = 0, startIndex = -1, periodCount = 0;
            while (hostIndex < hostName.length()) {
                if ('.' == hostName.charAt(hostIndex)) {
                    periodCount++;
                }
                if (certIndex < nameInCert.length() && hostName.charAt(hostIndex) == nameInCert.charAt(certIndex)) {
                    hostIndex++;
                    certIndex++;
                } else if (certIndex < nameInCert.length() && '*' == nameInCert.charAt(certIndex)) {
                    startIndex = certIndex;
                    match = hostIndex;
                    certIndex++;
                } else if (startIndex != -1 && 0 == periodCount) {
                    certIndex = startIndex + 1;
                    match++;
                    hostIndex = match;
                } else {
                    logFailMessage(nameInCert, hostName);
                    return false;
                }
            }
            if (nameInCert.length() == certIndex && periodCount > 1) {
                logSuccessMessage(nameInCert, hostName);
                return true;
            } else {
                logFailMessage(nameInCert, hostName);
                return false;
            }
        }
        // Verify that the name in certificate matches exactly with the host name
        if (!nameInCert.equals(hostName)) {
            logFailMessage(nameInCert, hostName);
            return false;
        }
        logSuccessMessage(nameInCert, hostName);
        return true;
    }

    /**
     * Validate server name in certificate
     * 
     * @param cert
     *        X509 certificate
     * @param hostName
     *        hostname
     * @throws CertificateException
     */
    static void validateServerNameInCertificate(X509Certificate cert, String hostName) throws CertificateException {
        // Use RFC2253 format and secure CN parsing to avoid ambiguities introduced by
        // the "canonical" format (which lowercases and reverses RDN order). We rely
        // on LdapName/Rdn to securely parse the DN and extract the CN attribute only.
        X500Principal subjectPrincipal = cert.getSubjectX500Principal();
        String nameInCertDN = subjectPrincipal.getName(X500Principal.RFC2253);

        if (logger.isLoggable(Level.FINER)) {
            logger.finer(logContext + " Validating the server name:" + hostName);
            logger.finer(logContext + " The DN name in certificate:" + nameInCertDN);
        }

        boolean isServerNameValidated;
        String dnsNameInSANCert = "";

        // the name in cert is in RFC2253 format parse it to get the actual subject name
        String subjectCN = parseCommonNameSecure(cert);
        // X.509 certificate standard requires domain names to be in ASCII.
        // Even IDN (Unicode) names will be represented here in Punycode (ASCII).
        // Normalize case for comparison using English to avoid case issues like Turkish i.
        if (subjectCN != null) {
            subjectCN = subjectCN.toLowerCase(Locale.ENGLISH);
        }

        isServerNameValidated = validateServerName(subjectCN, hostName);

        if (!isServerNameValidated) {
            Collection<List<?>> sanCollection = cert.getSubjectAlternativeNames();

            if (sanCollection != null) {
                // find a subjectAlternateName entry corresponding to DNS Name
                for (List<?> sanEntry : sanCollection) {
                    if (sanEntry != null && sanEntry.size() >= 2) {
                        Object key = sanEntry.get(0);
                        Object value = sanEntry.get(1);

                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer(logContext + "Key: " + key + "; KeyClass:"
                                    + (key != null ? key.getClass() : null) + ";value: " + value + "; valueClass:"
                                    + (value != null ? value.getClass() : null));
                        }

                        // From
                        // Documentation(http://download.oracle.com/javase/6/docs/api/java/security/cert/X509Certificate.html):
                        // "Note that the Collection returned may contain
                        // more than one name of the same type."
                        // So, more than one entry of dnsNameType can be present.
                        // Java docs guarantee that the first entry in the list will be an integer.
                        // 2 is the sequence no of a dnsName
                        if ((key != null) && (key instanceof Integer) && ((Integer) key == 2)) {
                            // As per RFC2459, the DNSName will be in the
                            // "preferred name syntax" as specified by RFC
                            // 1034 and the name can be in upper or lower case.
                            // And no significance is attached to case.
                            // Java docs guarantee that the second entry in the list
                            // will be a string for dnsName
                            if (value != null && value instanceof String) {
                                dnsNameInSANCert = (String) value;

                                // Use English locale to avoid Turkish i issues.
                                // Note that, this conversion was not necessary for
                                // cert.getSubjectX500Principal().getName("canonical");
                                // as the above API already does this by default as per documentation.
                                dnsNameInSANCert = dnsNameInSANCert.toLowerCase(Locale.ENGLISH);

                                isServerNameValidated = validateServerName(dnsNameInSANCert, hostName);
                                if (isServerNameValidated) {
                                    if (logger.isLoggable(Level.FINER)) {
                                        logger.finer(
                                                logContext + " found a valid name in certificate: " + dnsNameInSANCert);
                                    }
                                    break;
                                }
                            }

                            if (logger.isLoggable(Level.FINER)) {
                                logger.finer(logContext
                                        + " the following name in certificate does not match the serverName: " + value);
                                logger.finer(logContext + " certificate:\n" + cert.toString());
                            }
                        }
                    } else {
                        if (logger.isLoggable(Level.FINER)) {
                            logger.finer(logContext + " found an invalid san entry: " + sanEntry);
                            logger.finer(logContext + " certificate:\n" + cert.toString());
                        }
                    }
                }
            }
        }

        if (!isServerNameValidated) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_certNameFailed"));
            Object[] msgArgs = {hostName, dnsNameInSANCert};
            throw new CertificateException(form.format(msgArgs));
        }
    }

    /**
     * Validate certificate provided in path against server X509 certificate
     * 
     * @param cert
     *        X509 certificate
     * @param certFile
     *        path to certificate file to validate
     * @throws CertificateException
     */
    static void validateServerCerticate(X509Certificate cert, String certFile) throws CertificateException {
        try (InputStream is = fileToStream(certFile)) {
            if (!CertificateFactory.getInstance("X509").generateCertificate(is).equals(cert)) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_serverCertError"));
                Object[] msgArgs = {certFile};
                throw new CertificateException(form.format(msgArgs));
            }
        } catch (Exception e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_serverCertError"));
            Object[] msgArgs = {e.getMessage(), certFile, cert.toString()};
            throw new CertificateException(form.format(msgArgs));
        }
    }

    private static void logFailMessage(String nameInCert, String hostName) {
        if (logger.isLoggable(Level.FINER)) {
            logger.finer(logContext + " The name in certificate " + nameInCert + " does not match with the server name "
                    + hostName + ".");
        }
    }

    private static void logSuccessMessage(String nameInCert, String hostName) {
        if (logger.isLoggable(Level.FINER)) {
            logger.finer(logContext + " The name in certificate:" + nameInCert + " validated against server name "
                    + hostName + ".");
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

    static KeyStore loadPKCS12KeyStore(String certPath,
            String keyPassword) throws KeyStoreException, NoSuchAlgorithmException, CertificateException, IOException, SQLServerException {
        KeyStore keyStore = KeyStore.getInstance(PKCS12_ALG);
        try (FileInputStream certStream = new FileInputStream(certPath)) {
            keyStore.load(certStream, (keyPassword != null) ? keyPassword.toCharArray() : null);
        } catch (FileNotFoundException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_readCertError"), null, 0, null);
        }
        return keyStore;
    }

    static KeyManager[] readPKCS8Certificate(String certPath, String keyPath,
            String keyPassword) throws IOException, GeneralSecurityException, SQLServerException {
        Certificate clientCertificate = loadCertificate(certPath);
        ((X509Certificate) clientCertificate).checkValidity();
        PrivateKey privateKey = loadPrivateKey(keyPath, keyPassword);

        KeyStore keyStore = KeyStore.getInstance(JAVA_KEY_STORE);
        keyStore.load(null, null);
        keyStore.setCertificateEntry(CLIENT_CERT, clientCertificate);
        keyStore.setKeyEntry(CLIENT_KEY, privateKey, keyPassword.toCharArray(), new Certificate[] {clientCertificate});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    private static KeyManager[] readPKCS12Certificate(String certPath,
            String keyPassword) throws NoSuchAlgorithmException, CertificateException, IOException, UnrecoverableKeyException, KeyStoreException, SQLServerException {

        KeyStore keyStore = loadPKCS12KeyStore(certPath, keyPassword);
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(SUN_X_509);
        keyManagerFactory.init(keyStore, (keyPassword != null) ? keyPassword.toCharArray() : null);
        return keyManagerFactory.getKeyManagers();
    }

    private static PrivateKey loadPrivateKeyFromPKCS8(
            String key) throws NoSuchAlgorithmException, InvalidKeySpecException {
        StringBuilder sb = new StringBuilder(key);
        deleteFirst(sb, PEM_PRIVATE_START);
        deleteFirst(sb, PEM_PRIVATE_END);
        byte[] formattedKey = Base64.getDecoder().decode(sb.toString().replaceAll("\\s", ""));

        KeyFactory factory = KeyFactory.getInstance(RSA_ALG);
        return factory.generatePrivate(new PKCS8EncodedKeySpec(formattedKey));
    }

    private static void deleteFirst(StringBuilder sb, String str) {
        int i = sb.indexOf(str);
        if (i != -1) {
            sb.delete(i, i + str.length());
        }
    }

    private static PrivateKey loadPrivateKeyFromPKCS1(String key, String keyPass) throws IOException {
        SQLServerBouncyCastleLoader.loadBouncyCastle();
        try (PEMParser pemParser = new PEMParser(new StringReader(key))) {
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
        }
    }

    private static PrivateKey loadPrivateKeyFromPVK(String keyPath,
            String keyPass) throws IOException, GeneralSecurityException, SQLServerException {
        File f = new File(keyPath);
        ByteBuffer buffer = ByteBuffer.allocate((int) f.length());

        try (FileInputStream in = new FileInputStream(f); FileChannel channel = in.getChannel()) {
            ((Buffer) buffer.order(ByteOrder.LITTLE_ENDIAN)).rewind();

            long magic = buffer.getInt() & 0xFFFFFFFFL;
            if (PVK_MAGIC != magic) {
                SQLServerException.makeFromDriverError(null, magic, SQLServerResource.getResource("R_pvkHeaderError"),
                        "", false);
            }

            ((Buffer) buffer).position(((Buffer) buffer).position() + 8); // skip reserved and keytype
            boolean encrypted = buffer.getInt() != 0;
            int saltLength = buffer.getInt();
            int keyLength = buffer.getInt();
            byte[] salt = new byte[saltLength];
            buffer.get(salt);

            ((Buffer) buffer).position(((Buffer) buffer).position() + 8); // skip btype(1b), version(1b), reserved(2b),
                                                                          // and keyalg(4b)

            byte[] key = new byte[keyLength - 8];
            buffer.get(key);

            if (encrypted) {
                MessageDigest digest = MessageDigest.getInstance("SHA1");
                digest.update(salt);
                if (null != keyPass) {
                    digest.update(keyPass.getBytes());
                }
                byte[] hash = digest.digest();
                key = getSecretKeyFromHash(key, hash);
            }

            ByteBuffer buff = ByteBuffer.wrap(key).order(ByteOrder.LITTLE_ENDIAN);
            ((Buffer) buff).position(RSA2_MAGIC.length); // skip the header

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

    static Certificate loadCertificate(
            String certificatePem) throws IOException, GeneralSecurityException, SQLServerException {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X509");
        try (InputStream certStream = fileToStream(certificatePem)) {
            return certificateFactory.generateCertificate(certStream);
        }
    }

    static PrivateKey loadPrivateKey(String privateKeyPemPath,
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

    private static InputStream fileToStream(String fname) throws IOException, SQLServerException {
        try (FileInputStream fis = new FileInputStream(fname); DataInputStream dis = new DataInputStream(fis)) {
            byte[] bytes = new byte[dis.available()];
            dis.readFully(bytes);
            return new ByteArrayInputStream(bytes);
        } catch (FileNotFoundException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_readCertError"), null, 0, null);
        }
    }

    private static String getStringFromFile(String filePath) throws IOException {
        return new String(Files.readAllBytes(Paths.get(filePath)));
    }

    /**
     * Securely parse the Common Name (CN) from the certificate subject using
     * LdapName/Rdn APIs and RFC2253 string representation to avoid mis-parsing
     * values that appear inside other attributes when canonical form is used.
     */
    static String parseCommonNameSecure(X509Certificate cert) {
        try {
            X500Principal subjectPrincipal = cert.getSubjectX500Principal();
            String dn = subjectPrincipal.getName(X500Principal.RFC2253);

            LdapName ldapDN = new LdapName(dn);
            for (Rdn rdn : ldapDN.getRdns()) {
                if ("CN".equalsIgnoreCase(rdn.getType())) {
                    String cnValue = rdn.getValue().toString();
                    return cnValue;
                }
            }

            if (logger.isLoggable(Level.FINER)) {
                logger.finer(logContext + " No CN found in certificate subject");
            }
            return null;

        } catch (Exception e) {
            if (logger.isLoggable(Level.WARNING)) {
                logger.warning(logContext + " Error parsing certificate: " + e.getMessage());
            }
            return null;
        }
    }
}
