package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.StringReader;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.Security;
import java.security.UnrecoverableKeyException;
import java.security.cert.Certificate;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.Scanner;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;

import org.bouncycastle.jce.provider.BouncyCastleProvider;
import org.bouncycastle.openssl.PEMDecryptorProvider;
import org.bouncycastle.openssl.PEMEncryptedKeyPair;
import org.bouncycastle.openssl.PEMKeyPair;
import org.bouncycastle.openssl.PEMParser;
import org.bouncycastle.openssl.jcajce.JcaPEMKeyConverter;
import org.bouncycastle.openssl.jcajce.JcePEMDecryptorProviderBuilder;
import org.bouncycastle.util.io.pem.PemObject;
import org.bouncycastle.util.io.pem.PemReader;


public class SQLServerCertificateUtils {

    public static KeyManager[] getKeyManagerFromFile(String certPath, String keyPath,
            String keyPassword) throws IOException, GeneralSecurityException {
        if (keyPath != null && keyPath.length() > 0) {
            return readPKCS8Certificate(certPath, keyPath, keyPassword);
        } else {
            return readPKCS12Certificate(certPath, keyPassword);
        }
    }

    private static KeyManager[] readPKCS12Certificate(String certPath,
            String keyPassword) throws NoSuchAlgorithmException, CertificateException, FileNotFoundException, IOException, UnrecoverableKeyException, KeyStoreException {
        KeyStore keystore = KeyStore.getInstance("PKCS12");
        keystore.load(new FileInputStream(certPath), keyPassword.toCharArray());
        KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance("SunX509");
        keyManagerFactory.init(keystore, keyPassword.toCharArray());
        return keyManagerFactory.getKeyManagers();
    }

    private static KeyManager[] readPKCS8Certificate(String certPath, String keyPath,
            String keyPassword) throws IOException, GeneralSecurityException {
        Certificate clientCertificate = loadCertificate(certPath);
        PrivateKey privateKey = loadPrivateKey(keyPath, keyPassword);

        KeyStore keyStore = KeyStore.getInstance("JKS");
        keyStore.load(null, null);
        keyStore.setCertificateEntry("client-cert", clientCertificate);
        keyStore.setKeyEntry("client-key", privateKey, keyPassword.toCharArray(),
                new Certificate[] {clientCertificate});

        KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, keyPassword.toCharArray());
        return kmf.getKeyManagers();
    }

    private static Certificate loadCertificate(String certificatePem) throws IOException, GeneralSecurityException {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X509");
        final byte[] content = readPemContent(certificatePem);
        return certificateFactory.generateCertificate(new ByteArrayInputStream(content));
    }

    private static PrivateKey loadPrivateKey(String privateKeyPem,
            String privateKeyPass) throws IOException, GeneralSecurityException {
        return pemLoadPrivateKeyPkcs1OrPkcs8Encoded(privateKeyPem, privateKeyPass);
    }

    private static byte[] readPemContent(String pemPath) throws IOException {
        final byte[] content;
        FileInputStream fis = new FileInputStream(pemPath);
        try (PemReader pemReader = new PemReader(new InputStreamReader(fis))) {
            final PemObject pemObject = pemReader.readPemObject();
            content = pemObject.getContent();
        }
        return content;
    }

    private static String getStringFromFile(String filePath) throws FileNotFoundException {
        Scanner scanner = null;
        try {
            scanner = new Scanner(new File(filePath));
            String text = scanner.useDelimiter("\\A").next();
            return text;
        } finally {
            if (scanner != null) {
                scanner.close();
            }
        }
    }

    private static PrivateKey pemLoadPrivateKeyPkcs1OrPkcs8Encoded(String privateKeyPemPath,
            String privateKeyPassword) throws GeneralSecurityException, IOException {
        String privateKeyPem = getStringFromFile(privateKeyPemPath);

        // PKCS#8 format
        final String PEM_PRIVATE_START = "-----BEGIN PRIVATE KEY-----";
        final String PEM_PRIVATE_END = "-----END PRIVATE KEY-----";

        // PKCS#1 format
        final String PEM_RSA_PRIVATE_START = "-----BEGIN RSA PRIVATE KEY-----";

        if (privateKeyPem.contains(PEM_PRIVATE_START)) { // PKCS#8 format
            privateKeyPem = privateKeyPem.replace(PEM_PRIVATE_START, "").replace(PEM_PRIVATE_END, "");
            privateKeyPem = privateKeyPem.replaceAll("\\s", "");

            byte[] pkcs8EncodedKey = Base64.getDecoder().decode(privateKeyPem);

            KeyFactory factory = KeyFactory.getInstance("RSA");
            return factory.generatePrivate(new PKCS8EncodedKeySpec(pkcs8EncodedKey));

        } else if (privateKeyPem.contains(PEM_RSA_PRIVATE_START)) { // PKCS#1 format
            try {
                Security.addProvider(new BouncyCastleProvider());
            } catch (SecurityException se) {
                // fall through, provider already loaded
            }
            PEMParser pemParser = null;
            try {
                pemParser = new PEMParser(new StringReader(privateKeyPem));
                Object object = pemParser.readObject();
                JcaPEMKeyConverter converter = new JcaPEMKeyConverter().setProvider("BC");
                KeyPair kp;
                if (object instanceof PEMEncryptedKeyPair && privateKeyPassword != null) {
                    PEMDecryptorProvider decProv = new JcePEMDecryptorProviderBuilder()
                            .build(privateKeyPassword.toCharArray());
                    kp = converter.getKeyPair(((PEMEncryptedKeyPair) object).decryptKeyPair(decProv));
                } else {
                    kp = converter.getKeyPair((PEMKeyPair) object);
                }
                return kp.getPrivate();
            } finally {
                pemParser.close();
            }
        }
        throw new GeneralSecurityException("Not supported format of a private key");
    }
}
