/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.ByteArrayInputStream;
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
import java.security.PublicKey;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.interfaces.ECPrivateKey;
import java.security.interfaces.ECPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.security.spec.ECPoint;
import java.security.spec.ECPublicKeySpec;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PSSParameterSpec;
import java.security.spec.RSAPublicKeySpec;
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


/**
 * 
 * Provides the implementation of the VSM Enclave Provider. The enclave provider encapsulates the client-side
 * implementation details of the enclave attestation protocol.
 *
 */
public class SQLServerVSMEnclaveProvider implements ISQLServerEnclaveProvider {

    private VSMAttestationParameters vsmParams = null;
    private AttestationResponse hgsResponse = null;
    private String attestationURL = null;
    private EnclaveSession enclaveSession = null;

    @Override
    public void getAttestationParameters(boolean createNewParameters, String url) throws SQLServerException {
        if (null == vsmParams || createNewParameters) {
            attestationURL = url;
            vsmParams = new VSMAttestationParameters();
        }
    }

    @Override
    public ArrayList<byte[]> createEnclaveSession(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        ArrayList<byte[]> b = describeParameterEncryption(connection, userSql, preparedTypeDefinitions, params,
                parameterNames);
        if (null != hgsResponse && !connection.enclaveEstablished()) {
            try {
                enclaveSession = new EnclaveSession(hgsResponse.getSessionID(),
                        vsmParams.createSessionSecret(hgsResponse.getDHpublicKey()));
            } catch (GeneralSecurityException e) {
                SQLServerException.makeFromDriverError(connection, this, e.getLocalizedMessage(), "0", false);
            }
        }
        return b;
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

    @Override
    public byte[] getEnclavePackage(String userSQL, ArrayList<byte[]> enclaveCEKs) throws SQLServerException {
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

    private AttestationResponse validateAttestationResponse(AttestationResponse ar) throws SQLServerException {
        try {
            byte[] attestationCerts = getAttestationCertificates();
            ar.validateCert(attestationCerts);
            ar.validateStatementSignature();
            ar.validateDHPublicKey();
        } catch (IOException | GeneralSecurityException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
        return ar;
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

    private ArrayList<byte[]> describeParameterEncryption(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        ArrayList<byte[]> enclaveRequestedCEKs = new ArrayList<>();
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
                return enclaveRequestedCEKs;
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
                            SQLServerException.makeFromDriverError(connection, this, form.format(msgArgs), "0", true);
                        }
                    }
                }
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
                    hgsResponse = new AttestationResponse(rs.getBytes(1));
                    // This validates and establishes the enclave session if valid
                    if (!connection.enclaveEstablished()) {
                        hgsResponse = validateAttestationResponse(hgsResponse);
                    }
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
        return enclaveRequestedCEKs;
    }
}


class VSMAttestationParameters extends BaseAttestationRequest {

    // Static byte[] for VSM ECDH
    private static byte ECDH_MAGIC[] = {0x45, 0x43, 0x4b, 0x33, 0x30, 0x00, 0x00, 0x00};
    // Type 3 is VSM, sent as Little Endian 0x30000000
    private static byte ENCLAVE_TYPE[] = new byte[] {0x3, 0x0, 0x0, 0x0};
    // VSM doesn't have a challenge
    private static byte ENCLAVE_CHALLENGE[] = new byte[] {0x0, 0x0, 0x0, 0x0};
    private static int ENCLAVE_LENGTH = 104;
    private byte[] x;
    private byte[] y;

    VSMAttestationParameters() throws SQLServerException {
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

    byte[] createSessionSecret(byte[] serverResponse) throws GeneralSecurityException, SQLServerException {
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


@SuppressWarnings("unused")
class AttestationResponse {
    private int totalSize;
    private int identitySize;
    private int healthReportSize;
    private int enclaveReportSize;

    private byte[] enclavePK;
    private byte[] healthReportCertificate;
    private byte[] enclaveReportPackage;

    private int sessionInfoSize;
    private byte[] sessionID = new byte[8];
    private int DHPKsize;
    private int DHPKSsize;
    private byte[] DHpublicKey;
    private byte[] publicKeySig;

    private X509Certificate healthCert;

    AttestationResponse(byte[] b) throws SQLServerException {
        /*-
         * Parse the attestation response.
         * 
         * Total Size of the response - 4B
         * Size of the Identity - 4B 
         * Size of the HealthCert - 4B
         * Size of the EnclaveReport - 4B
         * Enclave PK - identitySize bytes
         * Health Certificate - healthReportSize bytes
         * Enclave Report Package - enclaveReportSize bytes
         * Session Info Size - 4B
         * Session ID - 8B
         * DH Public Key Size - 4B
         * DH Public Key Signature Size - 4B
         * DH Public Key - DHPKsize bytes
         * DH Public Key Signature - DHPKSsize bytes
         */
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
        // Create a X.509 certificate from the bytes
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            healthCert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(healthReportCertificate));
        } catch (CertificateException ce) {
            SQLServerException.makeFromDriverError(null, this, ce.getLocalizedMessage(), "0", false);
        }
    }

    @SuppressWarnings("unchecked")
    void validateCert(byte[] b) throws SQLServerException {
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            Collection<X509Certificate> certs = (Collection<X509Certificate>) cf
                    .generateCertificates(new ByteArrayInputStream(b));
            for (X509Certificate cert : certs) {
                try {
                    healthCert.verify(cert.getPublicKey());
                    return;
                } catch (SignatureException e) {
                    // Doesn't match, but continue looping through the rest of the certificates
                }
            }
        } catch (GeneralSecurityException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
        SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_InvalidHealthCert"), "0",
                false);
    }

    void validateStatementSignature() throws SQLServerException, GeneralSecurityException {
        /*-
         * Parse the Enclave Report Package fields.
         * 
         * Package Size - 4B
         * Version - 4B
         * Signature Scheme - 4B
         * Signed Statement Bytes Size - 4B 
         * Signature Size - 4B
         * Reserved - 4B
         * Signed Statement - signedStatementSize bytes contains:
         *      Report Size - 4B
         *      Report Version - 4B
         *      Enclave Data - 64B
         *      Enclave Identity - 152B
         *      ??? - 720B
         * Signature Blob - signatureSize bytes
         */
        ByteBuffer enclaveReportPackageBuffer = ByteBuffer.wrap(enclaveReportPackage).order(ByteOrder.LITTLE_ENDIAN);
        int packageSize = enclaveReportPackageBuffer.getInt();
        int version = enclaveReportPackageBuffer.getInt();
        int signatureScheme = enclaveReportPackageBuffer.getInt();
        int signedStatementSize = enclaveReportPackageBuffer.getInt();
        int signatureSize = enclaveReportPackageBuffer.getInt();
        int reserved = enclaveReportPackageBuffer.getInt();

        byte[] signedStatement = new byte[signedStatementSize];
        enclaveReportPackageBuffer.get(signedStatement, 0, signedStatementSize);
        byte[] signatureBlob = new byte[signatureSize];
        enclaveReportPackageBuffer.get(signatureBlob, 0, signatureSize);

        if (enclaveReportPackageBuffer.remaining() != 0) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_EnclavePackageLengthError"), "0", false);
        }

        Signature sig = Signature.getInstance("RSASSA-PSS");
        PSSParameterSpec pss = new PSSParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1);
        sig.setParameter(pss);
        sig.initVerify(healthCert);
        sig.update(signedStatement);
        if (!sig.verify(signatureBlob)) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_InvalidSignedStatement"), "0", false);
        }
    }

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
