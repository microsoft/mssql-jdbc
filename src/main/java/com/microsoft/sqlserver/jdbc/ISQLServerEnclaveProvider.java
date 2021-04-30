/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
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
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicLong;

import javax.crypto.KeyAgreement;


/**
 * 
 * Provides an interface to create an Enclave Session
 *
 */
interface ISQLServerEnclaveProvider {
    /**
     * sp_describe_parameter_encryption stored procedure with 2 params
     */
    static final String SDPE1 = "EXEC sp_describe_parameter_encryption ?,?";

    /**
     * sp_describe_parameter_encryption stored procedure with 3 params
     */
    static final String SDPE2 = "EXEC sp_describe_parameter_encryption ?,?,?";

    /**
     * Get the Enclave package
     * 
     * @param userSQL
     *        user sql
     * @param enclaveCEKs
     *        enclave CEKs
     * @return the enclave package
     * @throws SQLServerException
     *         if error
     */
    default byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs) throws SQLServerException {
        EnclaveSession enclaveSession = getEnclaveSession();
        if (null != enclaveSession) {
            try {
                ByteArrayOutputStream enclavePackage = new ByteArrayOutputStream();
                enclavePackage.write(enclaveSession.getSessionID());
                ByteArrayOutputStream keys = new ByteArrayOutputStream();
                byte[] randomGUID = new byte[16];
                SecureRandom.getInstanceStrong().nextBytes(randomGUID);
                keys.write(randomGUID);
                keys.write(ByteBuffer.allocate(8).order(ByteOrder.LITTLE_ENDIAN).putLong(enclaveSession.getCounter())
                        .array());
                keys.write(MessageDigest.getInstance("SHA-256").digest((userSQL).getBytes(UTF_16LE)));
                for (byte[] b : enclaveCEKs) {
                    keys.write(b);
                }
                enclaveCEKs.clear();
                SQLServerAeadAes256CbcHmac256EncryptionKey encryptedKey = new SQLServerAeadAes256CbcHmac256EncryptionKey(
                        enclaveSession.getSessionSecret(), SQLServerAeadAes256CbcHmac256Algorithm.algorithmName);
                SQLServerAeadAes256CbcHmac256Algorithm algo = new SQLServerAeadAes256CbcHmac256Algorithm(encryptedKey,
                        SQLServerEncryptionType.Randomized, (byte) 0x1);
                enclavePackage.write(algo.encryptData(keys.toByteArray()));
                return enclavePackage.toByteArray();
            } catch (GeneralSecurityException | SQLServerException | IOException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
            }
        }
        return null;
    }

    /**
     * Execute sp_describe_parameter_encryption for AEv2
     * 
     * @param stmt
     *        statement
     * @param userSql
     *        user sql
     * @param preparedTypeDefinitions
     *        prepared type definitions
     * @param req
     *        request
     * @return result set
     * @throws SQLException
     *         if error
     * @throws IOException
     *         if IO exception
     */
    default ResultSet executeSDPEv2(PreparedStatement stmt, String userSql, String preparedTypeDefinitions,
            BaseAttestationRequest req) throws SQLException, IOException {
        ((SQLServerPreparedStatement) stmt).isInternalEncryptionQuery = true;
        stmt.setNString(1, userSql);
        if (preparedTypeDefinitions != null && preparedTypeDefinitions.length() != 0) {
            stmt.setNString(2, preparedTypeDefinitions);
        } else {
            stmt.setNString(2, "");
        }
        stmt.setBytes(3, req.getBytes());
        return ((SQLServerPreparedStatement) stmt).executeQueryInternal();
    }

    /**
     * Execute sp_describe_parameter_encryption
     * 
     * @param stmt
     *        stmt
     * @param userSql
     *        user sql
     * @param preparedTypeDefinitions
     *        prepared type definitions
     * @return result set
     * @throws SQLException
     *         if error
     */
    default ResultSet executeSDPEv1(PreparedStatement stmt, String userSql,
            String preparedTypeDefinitions) throws SQLException {
        ((SQLServerPreparedStatement) stmt).isInternalEncryptionQuery = true;
        stmt.setNString(1, userSql);
        if (preparedTypeDefinitions != null && preparedTypeDefinitions.length() != 0) {
            stmt.setNString(2, preparedTypeDefinitions);
        } else {
            stmt.setNString(2, "");
        }
        return ((SQLServerPreparedStatement) stmt).executeQueryInternal();
    }

    /**
     * Process result from sp_describe_parameter_encryption
     * 
     * @param userSql
     *        user sql
     * @param preparedTypeDefinitions
     *        prepared type definitions
     * @param params
     *        params
     * @param parameterNames
     *        param names
     * @param connection
     *        connection
     * @param stmt
     *        statement
     * @param rs
     *        result set
     * @param enclaveRequestedCEKs
     *        enclave requested CEKs
     * @throws SQLException
     *         if error
     */
    default void processSDPEv1(String userSql, String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames, SQLServerConnection connection, PreparedStatement stmt, ResultSet rs,
            ArrayList<byte[]> enclaveRequestedCEKs) throws SQLException {
        Map<Integer, CekTableEntry> cekList = new HashMap<>();
        CekTableEntry cekEntry = null;
        boolean isRequestedByEnclave = false;
        while (rs.next()) {
            int currentOrdinal = rs.getInt(DescribeParameterEncryptionResultSet1.KeyOrdinal.value());
            if (!cekList.containsKey(currentOrdinal)) {
                cekEntry = new CekTableEntry(currentOrdinal);
                cekList.put(cekEntry.ordinal, cekEntry);
            } else {
                cekEntry = cekList.get(currentOrdinal);
            }

            String keyStoreName = rs.getString(DescribeParameterEncryptionResultSet1.ProviderName.value());
            String algo = rs.getString(DescribeParameterEncryptionResultSet1.KeyEncryptionAlgorithm.value());
            String keyPath = rs.getString(DescribeParameterEncryptionResultSet1.KeyPath.value());

            int dbID = rs.getInt(DescribeParameterEncryptionResultSet1.DbId.value());
            byte[] mdVer = rs.getBytes(DescribeParameterEncryptionResultSet1.KeyMdVersion.value());
            int keyID = rs.getInt(DescribeParameterEncryptionResultSet1.KeyId.value());
            byte[] encryptedKey = rs.getBytes(DescribeParameterEncryptionResultSet1.EncryptedKey.value());

            cekEntry.add(encryptedKey, dbID, keyID, rs.getInt(DescribeParameterEncryptionResultSet1.KeyVersion.value()),
                    mdVer, keyPath, keyStoreName, algo);

            // servers supporting enclave computations should always return a boolean indicating whether the key
            // is
            // required by enclave or not.
            if (ColumnEncryptionVersion.AE_v2.value() <= connection.getServerColumnEncryptionVersion().value()) {
                isRequestedByEnclave = rs
                        .getBoolean(DescribeParameterEncryptionResultSet1.IsRequestedByEnclave.value());
            }

            if (isRequestedByEnclave) {
                byte[] keySignature = rs.getBytes(DescribeParameterEncryptionResultSet1.EnclaveCMKSignature.value());
                String serverName = connection.getTrustedServerNameAE();
                SQLServerSecurityUtility.verifyColumnMasterKeyMetadata(connection, keyStoreName, keyPath, serverName,
                        isRequestedByEnclave, keySignature);

                // DBID(4) + MDVER(8) + KEYID(2) + CEK(32) = 46
                ByteBuffer aev2CekEntry = ByteBuffer.allocate(46);
                aev2CekEntry.order(ByteOrder.LITTLE_ENDIAN).putInt(dbID);
                aev2CekEntry.put(mdVer);
                aev2CekEntry.putShort((short) keyID);
                aev2CekEntry.put(connection.getColumnEncryptionKeyStoreProvider(keyStoreName)
                        .decryptColumnEncryptionKey(keyPath, algo, encryptedKey));
                enclaveRequestedCEKs.add(aev2CekEntry.array());
            }
        }

        // Process the second resultset.
        if (!stmt.getMoreResults()) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_UnexpectedDescribeParamFormat"), null,
                    0, false);
        }

        rs = (SQLServerResultSet) stmt.getResultSet();
        while (rs.next() && null != params) {
            String paramName = rs.getString(DescribeParameterEncryptionResultSet2.ParameterName.value());
            int paramIndex = parameterNames.indexOf(paramName);
            int cekOrdinal = rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncryptionKeyOrdinal.value());
            cekEntry = cekList.get(cekOrdinal);

            // cekEntry will be null if none of the parameters are encrypted.
            if ((null != cekEntry) && (cekList.size() < cekOrdinal)) {
                MessageFormat form = new MessageFormat(
                        SQLServerException.getErrString("R_InvalidEncryptionKeyOrdinal"));
                Object[] msgArgs = {cekOrdinal, cekEntry.getSize()};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
            SQLServerEncryptionType encType = SQLServerEncryptionType
                    .of((byte) rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncrytionType.value()));
            if (SQLServerEncryptionType.PlainText != encType) {
                params[paramIndex].cryptoMeta = new CryptoMetadata(cekEntry, (short) cekOrdinal,
                        (byte) rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncryptionAlgorithm.value()), null,
                        encType.value,
                        (byte) rs.getInt(DescribeParameterEncryptionResultSet2.NormalizationRuleVersion.value()));
                // Decrypt the symmetric key.(This will also validate and throw if needed).
                SQLServerSecurityUtility.decryptSymmetricKey(params[paramIndex].cryptoMeta, connection);
            } else {
                if (params[paramIndex].getForceEncryption()) {
                    MessageFormat form = new MessageFormat(
                            SQLServerException.getErrString("R_ForceEncryptionTrue_HonorAETrue_UnencryptedColumn"));
                    Object[] msgArgs = {userSql, paramIndex + 1};
                    SQLServerException.makeFromDriverError(null, connection, form.format(msgArgs), "0", true);
                }
            }
        }
    }

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
    protected static final int BIG_INTEGER_SIZE = 48;

    protected PrivateKey privateKey;
    protected byte[] enclaveChallenge;
    protected byte[] x;
    protected byte[] y;

    byte[] getBytes() throws IOException {
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
        byte[] x = new byte[BIG_INTEGER_SIZE];
        byte[] y = new byte[BIG_INTEGER_SIZE];
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
        try {
            KeyPairGenerator kpg = KeyPairGenerator.getInstance("EC");
            kpg.initialize(new ECGenParameterSpec("secp384r1"));
            KeyPair kp = kpg.generateKeyPair();
            ECPublicKey publicKey = (ECPublicKey) kp.getPublic();
            privateKey = kp.getPrivate();
            ECPoint w = publicKey.getW();
            x = adjustBigInt(w.getAffineX().toByteArray());
            y = adjustBigInt(w.getAffineY().toByteArray());
        } catch (GeneralSecurityException | IOException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
    }

    /*
     * We need our BigInts to be 48 bytes
     */
    private byte[] adjustBigInt(byte[] b) throws IOException {
        if (0 == b[0] && BIG_INTEGER_SIZE < b.length) {
            b = Arrays.copyOfRange(b, 1, b.length);
        }

        if (b.length < BIG_INTEGER_SIZE) {
            ByteArrayOutputStream output = new ByteArrayOutputStream();
            for (int i = 0; i < BIG_INTEGER_SIZE - b.length; i++) {
                output.write(0);
            }
            output.write(b);
            b = output.toByteArray();
        }
        return b;
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
    private AtomicLong counter;
    private byte[] sessionSecret;

    EnclaveSession(byte[] cs, byte[] b) {
        sessionID = cs;
        sessionSecret = b;
        counter = new AtomicLong(0);
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

    void addEntry(String servername, String catalog, String attestationUrl, BaseAttestationRequest b,
            EnclaveSession e) {
        StringBuilder sb = new StringBuilder(servername).append(catalog).append(attestationUrl);
        sessionCache.put(sb.toString(), new EnclaveCacheEntry(b, e));
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
