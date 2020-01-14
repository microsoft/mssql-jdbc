/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.NoSuchAlgorithmException;
import java.security.Security;
import java.security.Signature;
import java.security.SignatureException;
import java.security.cert.CertificateException;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.security.spec.MGF1ParameterSpec;
import java.security.spec.PSSParameterSpec;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Hashtable;


/**
 * 
 * Provides the implementation of the VSM Enclave Provider. The enclave provider encapsulates the client-side
 * implementation details of the enclave attestation protocol.
 *
 */
public class SQLServerVSMEnclaveProvider implements ISQLServerEnclaveProvider {

    private static EnclaveSessionCache enclaveCache = new EnclaveSessionCache();

    private VSMAttestationParameters vsmParams = null;
    private VSMAttestationResponse hgsResponse = null;
    private String attestationUrl = null;
    private EnclaveSession enclaveSession = null;

    @Override
    public void getAttestationParameters(String url) throws SQLServerException {
        if (null == vsmParams) {
            attestationUrl = url;
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
            // Check if the session exists in our cache
            EnclaveCacheEntry entry = enclaveCache.getSession(connection.getServerName() + attestationUrl);
            if (null != entry) {
                this.enclaveSession = entry.getEnclaveSession();
                this.vsmParams = (VSMAttestationParameters) entry.getBaseAttestationRequest();
                return b;
            }
            // If not, set it up
            try {
                enclaveSession = new EnclaveSession(hgsResponse.getSessionID(),
                        vsmParams.createSessionSecret(hgsResponse.getDHpublicKey()));
                enclaveCache.addEntry(connection.getServerName(), connection.enclaveAttestationUrl, vsmParams,
                        enclaveSession);
            } catch (GeneralSecurityException e) {
                SQLServerException.makeFromDriverError(connection, this, e.getLocalizedMessage(), "0", false);
            }
        }
        return b;
    }

    @Override
    public void invalidateEnclaveSession() {
        if (null != enclaveSession) {
            enclaveCache.removeEntry(enclaveSession);
        }
        enclaveSession = null;
        vsmParams = null;
        attestationUrl = null;
    }

    @Override
    public EnclaveSession getEnclaveSession() {
        return enclaveSession;
    }

    private VSMAttestationResponse validateAttestationResponse(VSMAttestationResponse ar) throws SQLServerException {
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

    private static Hashtable<String, X509CertificateEntry> certificateCache = new Hashtable<>();

    private byte[] getAttestationCertificates() throws IOException {
        byte[] certData = null;
        X509CertificateEntry cacheEntry = certificateCache.get(attestationUrl);
        if (null != cacheEntry && !cacheEntry.expired()) {
            certData = cacheEntry.getCertificates();
        } else if (null != cacheEntry && cacheEntry.expired()) {
            certificateCache.remove(attestationUrl);
        }

        if (null == certData) {
            java.net.URL url = new java.net.URL(attestationUrl + "/attestationservice.svc/v2.0/signingCertificates/");
            java.net.URLConnection con = url.openConnection();
            byte[] buff = new byte[con.getInputStream().available()];
            con.getInputStream().read(buff, 0, buff.length);
            String s = new String(buff);
            // omit the square brackets that come with the JSON
            String[] bytesString = s.substring(1, s.length() - 1).split(",");
            certData = new byte[bytesString.length];
            for (int i = 0; i < certData.length; i++) {
                certData[i] = (byte) (Integer.parseInt(bytesString[i]));
            }
            certificateCache.put(attestationUrl, new X509CertificateEntry(certData));
        }
        return certData;
    }

    private ArrayList<byte[]> describeParameterEncryption(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        ArrayList<byte[]> enclaveRequestedCEKs = new ArrayList<>();
        ResultSet rs = null;
        try (PreparedStatement stmt = connection.prepareStatement(connection.enclaveEstablished() ? SDPE1 : SDPE2)) {
            if (connection.enclaveEstablished()) {
                rs = executeSDPEv1(stmt, userSql, preparedTypeDefinitions);
            } else {
                rs = executeSDPEv2(stmt, userSql, preparedTypeDefinitions, vsmParams);
            }
            if (null == rs) {
                // No results. Meaning no parameter.
                // Should never happen.
                return enclaveRequestedCEKs;
            }
            processSDPEv1(userSql, preparedTypeDefinitions, params, parameterNames, connection, stmt, rs,
                    enclaveRequestedCEKs);
            // Process the third resultset.
            if (connection.isAEv2() && stmt.getMoreResults()) {
                rs = (SQLServerResultSet) stmt.getResultSet();
                while (rs.next()) {
                    hgsResponse = new VSMAttestationResponse(rs.getBytes(1));
                    // This validates and establishes the enclave session if valid
                    if (!connection.enclaveEstablished()) {
                        hgsResponse = validateAttestationResponse(hgsResponse);
                    }
                }
            }
            // Null check for rs is done already.
            rs.close();
        } catch (SQLException | IOException e) {
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
    // Type 3 is VSM, sent as Little Endian 0x30000000
    private static byte ENCLAVE_TYPE[] = new byte[] {0x3, 0x0, 0x0, 0x0};

    VSMAttestationParameters() throws SQLServerException {
        enclaveChallenge = new byte[] {0x0, 0x0, 0x0, 0x0};
        initBcryptECDH();
    }

    @Override
    byte[] getBytes() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(ENCLAVE_TYPE);
        os.write(enclaveChallenge);
        os.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(ENCLAVE_LENGTH).array());
        os.write(ECDH_MAGIC);
        os.write(x);
        os.write(y);
        return os.toByteArray();
    }
}


@SuppressWarnings("unused")
class VSMAttestationResponse extends BaseAttestationResponse {
    private byte[] healthReportCertificate;
    private byte[] enclaveReportPackage;
    private X509Certificate healthCert;

    VSMAttestationResponse(byte[] b) throws SQLServerException {
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
        ByteBuffer response = (null != b) ? ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN) : null;
        if (null != response) {
            this.totalSize = response.getInt();
            this.identitySize = response.getInt();
            int healthReportSize = response.getInt();
            int enclaveReportSize = response.getInt();

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
        }

        if (null == response || 0 != response.remaining()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_EnclaveResponseLengthError"), "0", false);
        }
        // Create a X.509 certificate from the bytes
        try {
            CertificateFactory cf = CertificateFactory.getInstance("X.509");
            healthCert = (X509Certificate) cf.generateCertificate(new ByteArrayInputStream(healthReportCertificate));
        } catch (CertificateException ce) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_HealthCertError"));
            Object[] msgArgs = {ce.getLocalizedMessage()};
            SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);
        }
    }

    @SuppressWarnings("unchecked")
    void validateCert(byte[] b) throws SQLServerException {
        if (null != b) {
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

        Signature sig = null;
        try {
            sig = Signature.getInstance("RSASSA-PSS");
        } catch (NoSuchAlgorithmException e) {
            /*
             * RSASSA-PSS was added in JDK 11, the user might be using an older version of Java. Use BC as backup.
             * Remove this logic if JDK 8 stops being supported or backports RSASSA-PSS
             */
            Security.addProvider(new org.bouncycastle.jce.provider.BouncyCastleProvider());
            sig = Signature.getInstance("RSASSA-PSS");
        }
        PSSParameterSpec pss = new PSSParameterSpec("SHA-256", "MGF1", MGF1ParameterSpec.SHA256, 32, 1);
        sig.setParameter(pss);
        sig.initVerify(healthCert);
        sig.update(signedStatement);
        if (!sig.verify(signatureBlob)) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_InvalidSignedStatement"), "0", false);
        }
    }
}


class X509CertificateEntry {
    private static final long EIGHT_HOUR_IN_SECONDS = 28800;

    private byte[] certificates;
    private long timeCreatedInSeconds;

    X509CertificateEntry(byte[] b) {
        certificates = b;
        timeCreatedInSeconds = Instant.now().getEpochSecond();
    }

    boolean expired() {
        return (Instant.now().getEpochSecond() - timeCreatedInSeconds) > EIGHT_HOUR_IN_SECONDS;
    }

    byte[] getCertificates() {
        return certificates;
    }
}
