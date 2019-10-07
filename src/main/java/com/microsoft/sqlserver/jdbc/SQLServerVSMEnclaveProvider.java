package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import static java.nio.charset.StandardCharsets.UTF_16LE;
import java.security.InvalidAlgorithmParameterException;
import java.security.InvalidKeyException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.InvalidKeySpecException;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import javax.crypto.KeyAgreement;


public class SQLServerVSMEnclaveProvider implements ISQLServerEnclaveProvider {

    private VSMAttestationParameters vsmParams = null;
    private AttestationResponse hgsResponse = null;
    private String attestationURL = null;
    EnclaveSession enclaveSession = null;

    @Override
    public void getAttestationParamters(boolean createNewParameters,
            String url) throws NoSuchAlgorithmException, InvalidAlgorithmParameterException {
        if (null == vsmParams || createNewParameters) {
            attestationURL = url;
            vsmParams = new VSMAttestationParameters();
        }
    }

    @Override
    public void createEnclaveSession(SQLServerConnection connection, String userSql, String preparedTypeDefinitions,
            Parameter[] params, ArrayList<String> parameterNames) throws SQLServerException {
        describeParameterEncryption(connection, userSql, preparedTypeDefinitions, params, parameterNames);
        if (null != hgsResponse) {
            try {
                enclaveSession = new EnclaveSession(hgsResponse.getSessionID(),
                        vsmParams.createSessionSecret(hgsResponse.DHpublicKey));
            } catch (InvalidKeyException | NoSuchAlgorithmException | InvalidKeySpecException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
    }

    @Override
    public void invalidateEnclaveSession() {
        enclaveSession = null;
        vsmParams = null;
        attestationURL = null;
    }

    @Override
    public EnclaveSession getEnclaveSession() {
        return enclaveSession;
    }

    private AttestationResponse validateAttestationResponse(byte[] b) throws SQLServerException {
        AttestationResponse ar = new AttestationResponse(b);
        try {
            byte[] attestationCerts = getAttestationCertificates();
            ar.validateCert(attestationCerts);
        } catch (IOException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
        return ar;
    }

    @Override
    public byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs) {
        if (null != enclaveSession) {
            try {
                ByteArrayOutputStream enclavePackage = new ByteArrayOutputStream();
                enclavePackage.writeBytes(enclaveSession.getSessionID());
                ByteArrayOutputStream keys = new ByteArrayOutputStream();
                byte[] randomGUID = new byte[16];
                SecureRandom.getInstanceStrong().nextBytes(randomGUID);
                keys.writeBytes(randomGUID);
                keys.writeBytes(ByteBuffer.allocate(8).putLong(enclaveSession.getCounter()).array());
                keys.writeBytes(MessageDigest.getInstance("SHA-256").digest((userSQL).getBytes(UTF_16LE)));
                for (byte[] b : enclaveCEKs) {
                    keys.writeBytes(b);
                }
                SQLServerAeadAes256CbcHmac256EncryptionKey encryptedKey = new SQLServerAeadAes256CbcHmac256EncryptionKey(
                        enclaveSession.getSessionSecret());
                SQLServerAeadAes256CbcHmac256Algorithm algo = new SQLServerAeadAes256CbcHmac256Algorithm(encryptedKey,
                        SQLServerEncryptionType.Randomized, (byte) 0x1);
                enclavePackage.writeBytes(algo.encryptData(keys.toByteArray()));
                return enclavePackage.toByteArray();
            } catch (NoSuchAlgorithmException | SQLServerException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
        }
        return null;
    }

    private byte[] getAttestationCertificates() throws IOException {
        java.net.URL url = new java.net.URL(attestationURL + "/attestationservice.svc/v2.0/signingCertificates/");
        java.net.URLConnection con = url.openConnection();
        String s = new String(con.getInputStream().readAllBytes());
        // omit the square brackets that come with the JSON
        String[] bytesString = s.substring(1, s.length() - 1).split(",");
        byte[] certData = new byte[bytesString.length];
        for (int i = 0; i < certData.length; i++) {
            certData[i] = (byte) (Integer.parseInt(bytesString[i]));
        }
        return certData;
    }

    private void describeParameterEncryption(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        // if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
        // getStatementLogger().fine(
        // "Calling stored procedure sp_describe_parameter_encryption to get parameter encryption information.");
        // }
        connection.enclaveCEKs.clear();
        ResultSet rs = null;
        try (PreparedStatement stmt = connection.prepareStatement("EXEC sp_describe_parameter_encryption ?,?,?")) {
            ((SQLServerPreparedStatement) stmt).isInternalEncryptionQuery = true;
            stmt.setNString(1, userSql);
            if (preparedTypeDefinitions != null && preparedTypeDefinitions.length() != 0) {
                stmt.setNString(2, preparedTypeDefinitions);
            } else {
                stmt.setNString(2, "");
            }
            stmt.setBytes(3, vsmParams.getBytes());
            rs = ((SQLServerPreparedStatement) stmt).executeQueryInternal();

            if (null == rs) {
                // No results. Meaning no parameter.
                // Should never happen.
                return;
            }

            Map<Integer, CekTableEntry> cekList = new HashMap<>();
            CekTableEntry cekEntry = null;
            boolean isRequestedByEnclave = false;
            try {
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

                    cekEntry.add(encryptedKey, dbID, keyID,
                            rs.getInt(DescribeParameterEncryptionResultSet1.KeyVersion.value()), mdVer, keyPath,
                            keyStoreName, algo);

                    // servers supporting enclave computations should always return a boolean indicating whether the key
                    // is
                    // required by enclave or not.
                    if (ColumnEncryptionVersion.AE_v2.value() <= connection.getServerColumnEncryptionVersion()
                            .value()) {
                        isRequestedByEnclave = rs
                                .getBoolean(DescribeParameterEncryptionResultSet1.IsRequestedByEnclave.value());
                    }

                    if (isRequestedByEnclave) {
                        byte[] keySignature = rs
                                .getBytes(DescribeParameterEncryptionResultSet1.EnclaveCMKSignature.value());
                        String serverName = connection.getTrustedServerNameAE();
                        SQLServerSecurityUtility.verifyColumnMasterKeyMetadata(connection, keyStoreName, keyPath,
                                serverName, isRequestedByEnclave, keySignature);

                        // Check for the connection provider first.
                        SQLServerColumnEncryptionKeyStoreProvider provider = connection
                                .getSystemColumnEncryptionKeyStoreProvider(keyStoreName);

                        // There is no connection provider of this name, check for the global system providers.
                        if (null == provider) {
                            provider = SQLServerConnection
                                    .getGlobalSystemColumnEncryptionKeyStoreProvider(keyStoreName);
                        }

                        // There is no global system provider of this name, check for the global custom providers.
                        if (null == provider) {
                            provider = SQLServerConnection
                                    .getGlobalCustomColumnEncryptionKeyStoreProvider(keyStoreName);
                        }

                        // DBID(4) + MDVER(8) + KEYID(2) + CEK(32) = 46
                        ByteBuffer aev2CekEntry = ByteBuffer.allocate(46);
                        aev2CekEntry.order(ByteOrder.LITTLE_ENDIAN).putInt(dbID);
                        aev2CekEntry.put(mdVer);
                        aev2CekEntry.putShort((short) keyID);
                        aev2CekEntry.put(provider.decryptColumnEncryptionKey(keyPath, algo, encryptedKey));
                        connection.enclaveCEKs.add(aev2CekEntry.array());
                    }
                }
                // if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                // getStatementLogger().fine("Matadata of CEKs is retrieved.");
                // }
            } catch (SQLException e) {
                if (e instanceof SQLServerException) {
                    throw (SQLServerException) e;
                } else {
                    throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"),
                            null, 0, e);
                }
            }

            // Process the second resultset.
            if (!stmt.getMoreResults()) {
                throw new SQLServerException(this, SQLServerException.getErrString("R_UnexpectedDescribeParamFormat"),
                        null, 0, false);
            }

            try {
                rs = (SQLServerResultSet) stmt.getResultSet();
                while (rs.next() && null != params) {
                    String paramName = rs.getString(DescribeParameterEncryptionResultSet2.ParameterName.value());
                    int paramIndex = parameterNames.indexOf(paramName);
                    int cekOrdinal = rs
                            .getInt(DescribeParameterEncryptionResultSet2.ColumnEncryptionKeyOrdinal.value());
                    cekEntry = cekList.get(cekOrdinal);

                    // cekEntry will be null if none of the parameters are encrypted.
                    if ((null != cekEntry) && (cekList.size() < cekOrdinal)) {
                        MessageFormat form = new MessageFormat(
                                SQLServerException.getErrString("R_InvalidEncryptionKeyOridnal"));
                        Object[] msgArgs = {cekOrdinal, cekEntry.getSize()};
                        throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
                    }
                    SQLServerEncryptionType encType = SQLServerEncryptionType
                            .of((byte) rs.getInt(DescribeParameterEncryptionResultSet2.ColumnEncrytionType.value()));
                    if (SQLServerEncryptionType.PlainText != encType) {
                        params[paramIndex].cryptoMeta = new CryptoMetadata(cekEntry, (short) cekOrdinal,
                                (byte) rs.getInt(
                                        DescribeParameterEncryptionResultSet2.ColumnEncryptionAlgorithm.value()),
                                null, encType.value, (byte) rs.getInt(
                                        DescribeParameterEncryptionResultSet2.NormalizationRuleVersion.value()));
                        // Decrypt the symmetric key.(This will also validate and throw if needed).
                        SQLServerSecurityUtility.decryptSymmetricKey(params[paramIndex].cryptoMeta, connection);
                    } else {
                        if (params[paramIndex].getForceEncryption()) {
                            MessageFormat form = new MessageFormat(SQLServerException
                                    .getErrString("R_ForceEncryptionTrue_HonorAETrue_UnencryptedColumn"));
                            Object[] msgArgs = {userSql, paramIndex + 1};
                            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), null, true);
                        }
                    }
                }
                // if (getStatementLogger().isLoggable(java.util.logging.Level.FINE)) {
                // getStatementLogger().fine("Parameter encryption metadata is set.");
                // }
            } catch (SQLException e) {
                if (e instanceof SQLServerException) {
                    throw (SQLServerException) e;
                } else {
                    throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"),
                            null, 0, e);
                }
            }

            // Process the third resultset.
            if (connection.isAEv2() && stmt.getMoreResults()) {
                rs = (SQLServerResultSet) stmt.getResultSet();
                while (rs.next()) {
                    // This validates and establishes the enclave session if valid
                    hgsResponse = validateAttestationResponse(rs.getBytes(1));
                }
            }

            // Null check for rs is done already.
            rs.close();
        } catch (SQLException e) {
            if (e instanceof SQLServerException) {
                throw (SQLServerException) e;
            } else {
                throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), null,
                        0, e);
            }
        }
        // connection.resetCurrentCommand();
    }
}


class VSMAttestationParameters extends BaseAttestationRequest {

    // Static byte[] for VSM ECDH
    static byte ECDH_MAGIC[] = {0x45, 0x43, 0x4b, 0x33, 0x30, 0x00, 0x00, 0x00};
    // Type 3 is VSM, sent as Little Endian 0x30000000
    static byte ENCLAVE_TYPE[] = new byte[] {0x3, 0x0, 0x0, 0x0};
    // VSM doesn't have a challenge
    static byte ENCLAVE_CHALLENGE[] = new byte[] {0x0, 0x0, 0x0, 0x0};
    static int ENCLAVE_LENGTH = 104;
    byte[] x;
    byte[] y;

    public VSMAttestationParameters() throws NoSuchAlgorithmException, InvalidAlgorithmParameterException {
        KeyPairGenerator kpg;
        kpg = KeyPairGenerator.getInstance("EC");
        kpg.initialize(new ECGenParameterSpec("secp384r1"));
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
        if (x[0] == 0 && x.length != 48) {
            x = Arrays.copyOfRange(x, 1, x.length);
        }
        if (y[0] == 0 && y.length != 48) {
            y = Arrays.copyOfRange(y, 1, y.length);
        }
    }

    @Override
    byte[] getBytes() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.writeBytes(ENCLAVE_TYPE);
        os.writeBytes(ENCLAVE_CHALLENGE);
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(ENCLAVE_LENGTH).array());
        os.writeBytes(ECDH_MAGIC);
        os.writeBytes(x);
        os.writeBytes(y);
        return os.toByteArray();
    }

    byte[] createSessionSecret(
            byte[] serverResponse) throws NoSuchAlgorithmException, InvalidKeySpecException, InvalidKeyException, SQLServerException {
        if (serverResponse.length != ENCLAVE_LENGTH) {
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
}


class AttestationResponse {
    int totalSize;
    int identitySize;
    int healthReportSize;
    int enclaveReportSize;

    byte[] enclavePK;
    byte[] healthReportCertificate;
    byte[] enclaveReportPackage;

    int sessionInfoSize;
    byte[] sessionID = new byte[8];
    int DHPKsize;
    int DHPKSsize;
    byte[] DHpublicKey;
    byte[] publicKeySig;

    X509Certificate healthCert;

    AttestationResponse(byte[] b) throws SQLServerException {
        ByteBuffer response = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
        this.totalSize = response.getInt();
        this.identitySize = response.getInt();
        this.healthReportSize = response.getInt();
        this.enclaveReportSize = response.getInt();

        enclavePK = new byte[identitySize];
        healthReportCertificate = new byte[healthReportSize];
        enclaveReportPackage = new byte[enclaveReportSize];

        response.get(enclavePK, 0, identitySize);
        response.get(healthReportCertificate, 0, healthReportSize);
        response.get(enclaveReportPackage, 0, enclaveReportSize);

        this.sessionInfoSize = response.getInt();
        response.get(sessionID, 0, 8);
        this.DHPKsize = response.getInt();
        this.DHPKSsize = response.getInt();

        DHpublicKey = new byte[DHPKsize];
        publicKeySig = new byte[DHPKSsize];

        response.get(DHpublicKey, 0, DHPKsize);
        response.get(publicKeySig, 0, DHPKSsize);

        if (0 != response.remaining()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_EnclaveResponseLengthError"), "0", false);
        }

        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            healthCert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(healthReportCertificate));
        } catch (CertificateException ce) {
            SQLServerException.makeFromDriverError(null, this, ce.getLocalizedMessage(), "0", false);
        }
    }

    @SuppressWarnings("unchecked")
    boolean validateCert(byte[] b) throws SQLServerException {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<X509Certificate> certs = (Collection<X509Certificate>) cf
                    .generateCertificates(new ByteArrayInputStream(b));
            for (X509Certificate cert : certs) {
                try {
                    healthCert.verify(cert.getPublicKey());
                    return true;
                } catch (SignatureException e) {
                    // Doesn't match, but continue looping through the rest of the certificates
                }
            }
        } catch (NoSuchAlgorithmException | NoSuchProviderException | InvalidKeyException | CertificateException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
        return false;
    }

    byte[] getSessionID() {
        return sessionID;
    }
}
