/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.ByteArrayOutputStream;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.RSAPublicKeySpec;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Hashtable;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import javax.crypto.KeyAgreement;


/**
 * 
 * Provides an interface to create an Enclave Session
 *
 */
public interface ISQLServerEnclaveProvider {
    default byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs) throws SQLServerException {
        EnclaveSession enclaveSession = getEnclaveSession();
        if (null != enclaveSession) {
            try {
                ByteArrayOutputStream enclavePackage = new ByteArrayOutputStream();
                enclavePackage.writeBytes(enclaveSession.getSessionID());
                ByteArrayOutputStream keys = new ByteArrayOutputStream();
                byte[] randomGUID = new byte[16];
                SecureRandom.getInstanceStrong().nextBytes(randomGUID);
                keys.writeBytes(randomGUID);
                keys.writeBytes(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN)
                        .putLong(enclaveSession.getCounter()).array());
                keys.writeBytes(MessageDigest.getInstance("SHA-256").digest((userSQL).getBytes(UTF_16LE)));
                for (byte[] b : enclaveCEKs) {
                    keys.writeBytes(b);
                }
                enclaveCEKs.clear();
                SQLServerAeadAes256CbcHmac256EncryptionKey encryptedKey = new SQLServerAeadAes256CbcHmac256EncryptionKey(
                        enclaveSession.getSessionSecret(), SQLServerAeadAes256CbcHmac256Algorithm.algorithmName);
                SQLServerAeadAes256CbcHmac256Algorithm algo = new SQLServerAeadAes256CbcHmac256Algorithm(encryptedKey,
                        SQLServerEncryptionType.Randomized, (byte) 0x1);
                enclavePackage.writeBytes(algo.encryptData(keys.toByteArray()));
                return enclavePackage.toByteArray();
            } catch (GeneralSecurityException | SQLServerException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
            }
        }
        return null;
    }
    
    public byte[] getParams();

    /**
     * Returns the attestation parameters
     * 
     * @param url
     *        attestation url
     * @throws SQLServerException
     *         when an error occurs.
     */
    void getAttestationParameters(String url) throws SQLServerException;

    /**
     * Creates the enclave session
     * 
     * @param connection
     *        connection
     * @param userSql
     *        user sql
     * @param preparedTypeDefinitions
     *        preparedTypeDefinitions
     * @param params
     *        params
     * @param parameterNames
     *        parameterNames
     * @return list of enclave requested CEKs
     * @throws SQLServerException
     *         when an error occurs.
     */
    ArrayList<byte[]> createEnclaveSession(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException;

    /**
     * Invalidates an enclave session
     */
    void invalidateEnclaveSession();

    /**
     * Returns the enclave session
     * 
     * @return the enclave session
     */
    EnclaveSession getEnclaveSession();
}


abstract class BaseAttestationRequest {
    protected static final byte[] ECDH_MAGIC = {0x45, 0x43, 0x4b, 0x33, 0x30, 0x00, 0x00, 0x00};
    protected static final int ENCLAVE_LENGTH = 104;

    protected PrivateKey privateKey;
    protected byte[] enclaveChallenge;
    protected byte[] x;
    protected byte[] y;

    byte[] getBytes() {
        return null;
    };

    byte[] createSessionSecret(byte[] serverResponse) throws GeneralSecurityException, SQLServerException {
        if (serverResponse == null || serverResponse.length != ENCLAVE_LENGTH) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_MalformedECDHPublicKey"), "0", false);
        }
        ByteBuffer sr = ByteBuffer.wrap(serverResponse);
        byte[] magic = new byte[8];
        sr.get(magic);
        if (!Arrays.equals(magic, ECDH_MAGIC)) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_MalformedECDHHeader"),
                    "0", false);
        }
        byte[] x = new byte[48];
        byte[] y = new byte[48];
        sr.get(x);
        sr.get(y);
        /*
         * Server returns X and Y coordinates, create a key using the point of the server and our key parameters.
         * Public/Private key parameters are the same.
         */
        ECPublicKeySpec keySpec = new ECPublicKeySpec(new ECPoint(new BigInteger(1, x), new BigInteger(1, y)),
                ((ECPrivateKey) privateKey).getParams());
        KeyAgreement ka = KeyAgreement.getInstance("ECDH");
        ka.init(privateKey);
        // Generate a PublicKey from the above key specifications and do an agreement with our PrivateKey
        ka.doPhase(KeyFactory.getInstance("EC").generatePublic(keySpec), true);
        // Generate a Secret from the agreement and hash with SHA-256 to create Session Secret
        return MessageDigest.getInstance("SHA-256").digest(ka.generateSecret());
    }

    void initBcryptECDH() throws SQLServerException {
        /*
         * Create our BCRYPT_ECCKEY_BLOB
         */
        KeyPairGenerator kpg = null;
        try {
            kpg = KeyPairGenerator.getInstance("EC");
            kpg.initialize(new ECGenParameterSpec("secp384r1"));
        } catch (GeneralSecurityException e) {
            SQLServerException.makeFromDriverError(null, kpg, e.getLocalizedMessage(), "0", false);
        }
        KeyPair kp = kpg.generateKeyPair();
        ECPublicKey publicKey = (ECPublicKey) kp.getPublic();
        privateKey = kp.getPrivate();
        ECPoint w = publicKey.getW();
        x = w.getAffineX().toByteArray();
        y = w.getAffineY().toByteArray();

        /*
         * For some reason toByteArray doesn't have an Signum option like the constructor. Manually remove leading 00
         * byte if it exists.
         */
        if (0 == x[0] && 48 != x.length) {
            x = Arrays.copyOfRange(x, 1, x.length);
        }
        if (0 == y[0] && 48 != y.length) {
            y = Arrays.copyOfRange(y, 1, y.length);
        }
    }
}


abstract class BaseAttestationResponse {
    protected int totalSize;
    protected int identitySize;
    protected int attestationTokenSize;
    protected int enclaveType;

    protected byte[] enclavePK;
    protected int sessionInfoSize;
    protected byte[] sessionID = new byte[8];
    protected int DHPKsize;
    protected int DHPKSsize;
    protected byte[] DHpublicKey;
    protected byte[] publicKeySig;

    @SuppressWarnings("unused")
    void validateDHPublicKey() throws SQLServerException, GeneralSecurityException {
        /*-
         * Java doesn't directly support PKCS1 padding for RSA keys. Parse the key bytes and create a RSAPublicKeySpec
         * with the exponent and modulus.
         * 
         * Static string "RSA1" - 4B (Unused)
         * Bit count - 4B (Unused)
         * Public Exponent Length - 4B
         * Public Modulus Length - 4B
         * Prime 1 - 4B (Unused)
         * Prime 2 - 4B (Unused)
         * Exponent - publicExponentLength bytes
         * Modulus - publicModulusLength bytes
         */
        ByteBuffer enclavePKBuffer = ByteBuffer.wrap(enclavePK).order(ByteOrder.LITTLE_ENDIAN);
        byte[] rsa1 = new byte[4];
        enclavePKBuffer.get(rsa1);
        int bitCount = enclavePKBuffer.getInt();
        int publicExponentLength = enclavePKBuffer.getInt();
        int publicModulusLength = enclavePKBuffer.getInt();
        int prime1 = enclavePKBuffer.getInt();
        int prime2 = enclavePKBuffer.getInt();
        byte[] exponent = new byte[publicExponentLength];
        enclavePKBuffer.get(exponent);
        byte[] modulus = new byte[publicModulusLength];
        enclavePKBuffer.get(modulus);
        if (enclavePKBuffer.remaining() != 0) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_EnclavePKLengthError"),
                    "0", false);
        }
        RSAPublicKeySpec spec = new RSAPublicKeySpec(new BigInteger(1, modulus), new BigInteger(1, exponent));
        KeyFactory factory = KeyFactory.getInstance("RSA");
        PublicKey pub = factory.generatePublic(spec);
        Signature sig = Signature.getInstance("SHA256withRSA");
        sig.initVerify(pub);
        sig.update(DHpublicKey);
        if (!sig.verify(publicKeySig)) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_InvalidDHKeySignature"),
                    "0", false);
        }
    }

    byte[] getDHpublicKey() {
        return DHpublicKey;
    }

    byte[] getSessionID() {
        return sessionID;
    }
}


class EnclaveSession {
    private byte[] sessionID;
    private AtomicInteger counter;
    private byte[] sessionSecret;

    EnclaveSession(byte[] cs, byte[] b) {
        sessionID = cs;
        sessionSecret = b;
        counter = new AtomicInteger(0);
    }

    byte[] getSessionID() {
        return sessionID;
    }

    byte[] getSessionSecret() {
        return sessionSecret;
    }

    synchronized long getCounter() {
        return counter.getAndIncrement();
    }
}


final class EnclaveSessionCache {
    private Hashtable<String, EnclaveCacheEntry> sessionCache;

    EnclaveSessionCache() {
        sessionCache = new Hashtable<>(0);
    }

    void addEntry(String servername, String attestationUrl, BaseAttestationRequest b, EnclaveSession e) {
        sessionCache.put(servername + attestationUrl, new EnclaveCacheEntry(b, e));
    }

    void removeEntry(EnclaveSession e) {
        for (Entry<String, EnclaveCacheEntry> entry : sessionCache.entrySet()) {
            EnclaveCacheEntry ece = entry.getValue();
            if (Arrays.equals(ece.getEnclaveSession().getSessionID(), e.getSessionID())) {
                sessionCache.remove(entry.getKey());
            }
        }
    }

    EnclaveCacheEntry getSession(String key) {
        EnclaveCacheEntry e = sessionCache.get(key);
        if (null != e && e.expired()) {
            sessionCache.remove(key);
            return null;
        }
        return e;
    }
}


class EnclaveCacheEntry {
    private static final long EIGHT_HOURS_IN_SECONDS = 28800;

    private BaseAttestationRequest bar;
    private EnclaveSession es;
    private long timeCreatedInSeconds;

    EnclaveCacheEntry(BaseAttestationRequest b, EnclaveSession e) {
        bar = b;
        es = e;
        timeCreatedInSeconds = Instant.now().getEpochSecond();
    }

    boolean expired() {
        return (Instant.now().getEpochSecond() - timeCreatedInSeconds) > EIGHT_HOURS_IN_SECONDS;
    }

    BaseAttestationRequest getBaseAttestationRequest() {
        return bar;
    }

    EnclaveSession getEnclaveSession() {
        return es;
    }
}
