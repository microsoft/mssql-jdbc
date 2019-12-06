/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLConnection;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.cert.CertificateFactory;
import java.security.cert.X509Certificate;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Hashtable;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;


/**
 * 
 * Provides the implementation of the AAS Enclave Provider. The enclave provider encapsulates the client-side
 * implementation details of the enclave attestation protocol.
 *
 */
public class SQLServerAASEnclaveProvider implements ISQLServerEnclaveProvider {

    private static EnclaveSessionCache enclaveCache = new EnclaveSessionCache();

    private AASAttestationParameters aasParams = null;
    private AASAttestationResponse hgsResponse = null;
    private String attestationURL = null;
    private EnclaveSession enclaveSession = null;

    @Override
    public void getAttestationParameters(String url) throws SQLServerException {
        if (null == aasParams) {
            attestationURL = url;
            try {
                aasParams = new AASAttestationParameters(attestationURL);
            } catch (IOException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
            }
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
            EnclaveCacheEntry entry = enclaveCache.getSession(connection.getServerName() + attestationURL);
            if (null != entry) {
                this.enclaveSession = entry.getEnclaveSession();
                this.aasParams = (AASAttestationParameters) entry.getBaseAttestationRequest();
                return b;
            }
            try {
                enclaveSession = new EnclaveSession(hgsResponse.getSessionID(),
                        aasParams.createSessionSecret(hgsResponse.getDHpublicKey()));
                enclaveCache.addEntry(connection.getServerName(), connection.enclaveAttestationUrl, aasParams,
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
        aasParams = null;
        attestationURL = null;
    }

    @Override
    public EnclaveSession getEnclaveSession() {
        return enclaveSession;
    }

    private AASAttestationResponse validateAttestationResponse(AASAttestationResponse ar) throws SQLServerException {
        try {
            ar.validateToken(attestationURL, aasParams.getNonce());
            ar.validateDHPublicKey(aasParams.getNonce());
        } catch (GeneralSecurityException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
        }
        return ar;
    }

    private ArrayList<byte[]> describeParameterEncryption(SQLServerConnection connection, String userSql,
            String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        ArrayList<byte[]> enclaveRequestedCEKs = new ArrayList<>();
        ResultSet rs = null;
        try (PreparedStatement stmt = connection.prepareStatement(proc)) {
            rs = executeProc(stmt, userSql, preparedTypeDefinitions, aasParams);
            if (null == rs) {
                // No results. Meaning no parameter.
                // Should never happen.
                return enclaveRequestedCEKs;
            }
            processAev1SPDE(userSql, preparedTypeDefinitions, params, parameterNames, connection, stmt, rs,
                    enclaveRequestedCEKs);
            // Process the third resultset.
            if (connection.isAEv2() && stmt.getMoreResults()) {
                rs = (SQLServerResultSet) stmt.getResultSet();
                while (rs.next()) {
                    hgsResponse = new AASAttestationResponse(rs.getBytes(1));
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


class AASAttestationParameters extends BaseAttestationRequest {

    // Type 1 is AAS, sent as Little Endian 0x10000000
    private static final byte[] ENCLAVE_TYPE = new byte[] {0x1, 0x0, 0x0, 0x0};
    // Nonce length is always 256
    private static byte[] NONCE_LENGTH = new byte[] {0x0, 0x1, 0x0, 0x0};
    private byte[] nonce = new byte[256];

    AASAttestationParameters(String attestationUrl) throws SQLServerException, IOException {
        byte[] attestationUrlBytes = (attestationUrl + '\0').getBytes(UTF_16LE);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(attestationUrlBytes.length).array());
        os.writeBytes(attestationUrlBytes);
        os.writeBytes(NONCE_LENGTH);
        new SecureRandom().nextBytes(nonce);
        os.writeBytes(nonce);
        enclaveChallenge = os.toByteArray();

        initBcryptECDH();
    }

    @Override
    byte[] getBytes() {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.writeBytes(ENCLAVE_TYPE);
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(enclaveChallenge.length).array());
        os.writeBytes(enclaveChallenge);
        os.writeBytes(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(ENCLAVE_LENGTH).array());
        os.writeBytes(ECDH_MAGIC);
        os.writeBytes(x);
        os.writeBytes(y);
        return os.toByteArray();
    }

    byte[] getNonce() {
        return nonce;
    }
}


class JWTCertificateEntry {
    private static final long TWENTY_FOUR_HOUR_IN_SECONDS = 86400;

    private JsonArray certificates;
    private long timeCreatedInSeconds;

    JWTCertificateEntry(JsonArray j) {
        certificates = j;
        timeCreatedInSeconds = Instant.now().getEpochSecond();
    }

    boolean expired() {
        return (Instant.now().getEpochSecond() - timeCreatedInSeconds) > TWENTY_FOUR_HOUR_IN_SECONDS;
    }

    JsonArray getCertificates() {
        return certificates;
    }
}


@SuppressWarnings("unused")
class AASAttestationResponse extends BaseAttestationResponse {

    private byte[] attestationToken;
    private static Hashtable<String, JWTCertificateEntry> certificateCache = new Hashtable<>();

    AASAttestationResponse(byte[] b) throws SQLServerException {
        /*-
         * A model class representing the deserialization of the byte payload the client
         * receives from SQL Server while setting up a session.
         * Protocol format:
         * 1. Total Size of the attestation blob as UINT
         * 2. Size of Enclave RSA public key as UINT
         * 3. Size of Attestation token as UINT
         * 4. Enclave Type as UINT
         * 5. Enclave RSA public key (raw key, of length #2)
         * 6. Attestation token (of length #3)
         * 7. Size of Session Id was UINT
         * 8. Session id value
         * 9. Size of enclave ECDH public key
         * 10. Enclave ECDH public key (of length #9)
        */
        ByteBuffer response = ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN);
        this.totalSize = response.getInt();
        this.identitySize = response.getInt();
        this.attestationTokenSize = response.getInt();
        this.enclaveType = response.getInt(); // 1 for VBS, 2 for SGX

        enclavePK = new byte[identitySize];
        attestationToken = new byte[attestationTokenSize];

        response.get(enclavePK, 0, identitySize);
        response.get(attestationToken, 0, attestationTokenSize);

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
    }

    void validateToken(String attestationUrl, byte[] nonce) throws SQLServerException {
        try {
            /*
             * 3 parts of our JWT token: Header, Body, and Signature. Broken up via '.'
             */
            String jwtToken = (new String(attestationToken)).trim();
            if (jwtToken.startsWith("\"") && jwtToken.endsWith("\"")) {
                jwtToken = jwtToken.substring(1, jwtToken.length() - 1);
            }
            String[] splitString = jwtToken.split("\\.");
            java.util.Base64.Decoder decoder = Base64.getUrlDecoder();
            String header = new String(decoder.decode(splitString[0]));
            String body = new String(decoder.decode(splitString[1]));
            byte[] stmtSig = decoder.decode(splitString[2]);

            JsonArray keys = null;
            JWTCertificateEntry cacheEntry = certificateCache.get(attestationUrl);
            if (null != cacheEntry && !cacheEntry.expired()) {
                keys = cacheEntry.getCertificates();
            } else if (null != cacheEntry && cacheEntry.expired()) {
                certificateCache.remove(attestationUrl);
            }

            if (null == keys) {
                // Use the attestation URL to find where our keys are
                String authorityUrl = new URL(attestationUrl).getAuthority();
                URL wellKnownUrl = new URL("https://" + authorityUrl + "/.well-known/openid-configuration");
                URLConnection con = wellKnownUrl.openConnection();
                String wellKnownUrlJson = new String(con.getInputStream().readAllBytes());
                JsonObject attestationJson = JsonParser.parseString(wellKnownUrlJson).getAsJsonObject();
                // Get our Keys
                URL jwksUrl = new URL(attestationJson.get("jwks_uri").getAsString());
                URLConnection jwksCon = jwksUrl.openConnection();
                String jwksUrlJson = new String(jwksCon.getInputStream().readAllBytes());
                JsonObject jwksJson = JsonParser.parseString(jwksUrlJson).getAsJsonObject();
                keys = jwksJson.get("keys").getAsJsonArray();
                certificateCache.put(attestationUrl, new JWTCertificateEntry(keys));
            }
            // Find the specific keyID we need from our header

            JsonObject headerJsonObject = JsonParser.parseString(header).getAsJsonObject();
            String keyID = headerJsonObject.get("kid").getAsString();
            // Iterate through our list of keys and find the one with the same keyID
            for (JsonElement key : keys) {
                JsonObject keyObj = key.getAsJsonObject();
                String kId = keyObj.get("kid").getAsString();
                if (kId.equals(keyID)) {
                    JsonArray certsFromServer = keyObj.get("x5c").getAsJsonArray();
                    /*
                     * To create the signature part you have to take the encoded header, the encoded payload, a secret,
                     * the algorithm specified in the header, and sign that.
                     */
                    byte[] signatureBytes = (splitString[0] + "." + splitString[1]).getBytes();
                    for (JsonElement jsonCert : certsFromServer) {
                        CertificateFactory cf = CertificateFactory.getInstance("X.509");
                        X509Certificate cert = (X509Certificate) cf.generateCertificate(
                                new ByteArrayInputStream(java.util.Base64.getDecoder().decode(jsonCert.getAsString())));
                        Signature sig = Signature.getInstance("SHA256withRSA");
                        sig.initVerify(cert.getPublicKey());
                        sig.update(signatureBytes);
                        if (sig.verify(stmtSig)) {
                            // Token is verified, now check the aas-ehd
                            JsonObject bodyJsonObject = JsonParser.parseString(body).getAsJsonObject();
                            String aasEhd = bodyJsonObject.get("aas-ehd").getAsString();
                            if (!Arrays.equals(Base64.getUrlDecoder().decode(aasEhd), enclavePK)) {
                                SQLServerException.makeFromDriverError(null, this,
                                        SQLServerResource.getResource("R_AasEhdError"), "0", false);
                            }
                            if (this.enclaveType == 1) {
                                // Verify rp_data claim as well if VBS
                                String rpData = bodyJsonObject.get("rp_data").getAsString();
                                if (!Arrays.equals(Base64.getUrlDecoder().decode(rpData), nonce)) {
                                    SQLServerException.makeFromDriverError(null, this,
                                            SQLServerResource.getResource("R_VbsRpDataError"), "0", false);
                                }
                            }
                            return;
                        }
                    }
                }
            }
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_AasJWTError"), "0",
                    false);
        } catch (IOException | GeneralSecurityException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "", false);
        }
    }

    void validateDHPublicKey(byte[] nonce) throws SQLServerException, GeneralSecurityException {
        if (this.enclaveType == 2) {
            for (int i = 0; i < enclavePK.length; i++) {
                enclavePK[i] = (byte) (enclavePK[i] ^ nonce[i % nonce.length]);
            }
        }
        validateDHPublicKey();
    }
}
