/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;
import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
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
import java.util.concurrent.ConcurrentHashMap;

import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParseException;
import com.google.gson.JsonParser;
import com.google.gson.JsonPrimitive;


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
    private String attestationUrl = null;
    private EnclaveSession enclaveSession = null;

    /**
     * default constructor
     */
    public SQLServerAASEnclaveProvider() {
        // default constructor
    }

    @Override
    public void getAttestationParameters(String url) throws SQLServerException {
        if (null == aasParams) {
            attestationUrl = url;
            try {
                aasParams = new AASAttestationParameters(attestationUrl);
            } catch (IOException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false, e);
            }
        }
    }

    @Override
    public ArrayList<byte[]> createEnclaveSession(SQLServerConnection connection, SQLServerStatement statement,
            String userSql, String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        // Check if the session exists in our cache
        StringBuilder keyLookup = new StringBuilder(connection.getServerName()).append(connection.getCatalog())
                .append(attestationUrl);
        EnclaveCacheEntry entry = enclaveCache.getSession(keyLookup.toString());
        if (null != entry) {
            this.enclaveSession = entry.getEnclaveSession();
            this.aasParams = (AASAttestationParameters) entry.getBaseAttestationRequest();
        }
        ArrayList<byte[]> b = describeParameterEncryption(connection, statement, userSql, preparedTypeDefinitions,
                params, parameterNames);
        if (connection.enclaveEstablished()) {
            return b;
        } else if (null != hgsResponse && !connection.enclaveEstablished()) {
            try {
                enclaveSession = new EnclaveSession(hgsResponse.getSessionID(),
                        aasParams.createSessionSecret(hgsResponse.getDHpublicKey()));
                enclaveCache.addEntry(connection.getServerName(), connection.getCatalog(),
                        connection.enclaveAttestationUrl, aasParams, enclaveSession);
            } catch (GeneralSecurityException e) {
                SQLServerException.makeFromDriverError(connection, this, e.getLocalizedMessage(), "0", false, e);
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
        attestationUrl = null;
    }

    @Override
    public EnclaveSession getEnclaveSession() {
        return enclaveSession;
    }

    private void validateAttestationResponse() throws SQLServerException {
        if (null != hgsResponse) {
            try {
                hgsResponse.validateToken(attestationUrl, aasParams.getNonce());
                hgsResponse.validateDHPublicKey(aasParams.getNonce());
            } catch (GeneralSecurityException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false, e);
            }
        }
    }

    private ArrayList<byte[]> describeParameterEncryption(SQLServerConnection connection, SQLServerStatement statement,
            String userSql, String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {

        // sp_describe_parameter_encryption stored procedure with 2 params
        final String SDPE1 = "EXEC sp_describe_parameter_encryption ?,?";

        // sp_describe_parameter_encryption stored procedure with 3 params
        final String SDPE2 = "EXEC sp_describe_parameter_encryption ?,?,?";

        ArrayList<byte[]> enclaveRequestedCEKs = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(connection.enclaveEstablished() ? SDPE1 : SDPE2)) {
            // Check the cache for metadata for Always Encrypted versions 1 and 3, when there are parameters to check.
            if (connection.getServerColumnEncryptionVersion() == ColumnEncryptionVersion.AE_V2 || params == null
                    || params.length == 0 || !ParameterMetaDataCache.getQueryMetadata(params, parameterNames,
                            connection, statement, userSql)) {
                try (ResultSet rs = connection.enclaveEstablished() ? executeSDPEv1(stmt, userSql,
                        preparedTypeDefinitions) : executeSDPEv2(stmt, userSql, preparedTypeDefinitions, aasParams)) {
                    if (null == rs) {
                        // No results. Meaning no parameter.
                        // Should never happen.
                        return enclaveRequestedCEKs;
                    }
                    processSDPEv1(userSql, preparedTypeDefinitions, params, parameterNames, connection, statement, stmt,
                            rs, enclaveRequestedCEKs);
                    // Process the third resultset.
                    if (connection.isAEv2() && stmt.getMoreResults()) {
                        try (ResultSet hgsRs = (SQLServerResultSet) stmt.getResultSet()) {
                            if (hgsRs.next()) {
                                hgsResponse = new AASAttestationResponse(hgsRs.getBytes(1));
                                // This validates and establishes the enclave session if valid
                                validateAttestationResponse();
                            } else {
                                SQLServerException.makeFromDriverError(null, this,
                                        SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), "0",
                                        false);
                            }
                        }
                    }
                }
            }
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


class AASAttestationParameters extends BaseAttestationRequest {

    // Type 1 is AAS, sent as Little Endian 0x10000000
    private static final byte[] ENCLAVE_TYPE = new byte[] {0x1, 0x0, 0x0, 0x0};
    // Nonce length is always 256
    private static final byte[] NONCE_LENGTH = new byte[] {0x0, 0x1, 0x0, 0x0};
    private byte[] nonce = new byte[256];

    AASAttestationParameters(String attestationUrl) throws SQLServerException, IOException {
        byte[] attestationUrlBytes = (attestationUrl + '\0').getBytes(UTF_16LE);

        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(attestationUrlBytes.length).array());
        os.write(attestationUrlBytes);
        os.write(NONCE_LENGTH);
        new SecureRandom().nextBytes(nonce);
        os.write(nonce);
        enclaveChallenge = os.toByteArray();

        initBcryptECDH();
    }

    @Override
    byte[] getBytes() throws IOException {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        os.write(ENCLAVE_TYPE);
        os.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(enclaveChallenge.length).array());
        os.write(enclaveChallenge);
        os.write(ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN).putInt(ENCLAVE_LENGTH).array());
        os.write(ECDH_MAGIC);
        os.write(x);
        os.write(y);
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

    private static final String EXPECTED_JWT_ALGORITHM = "RS256";
    private static final long TOKEN_CLOCK_SKEW_SECONDS = 300;

    private byte[] attestationToken;
    private static ConcurrentHashMap<String, JWTCertificateEntry> certificateCache = new ConcurrentHashMap<>();

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
        this.dhpkSize = response.getInt();
        this.dhpkSsize = response.getInt();

        dhPublicKey = new byte[dhpkSize];
        publicKeySig = new byte[dhpkSsize];

        response.get(dhPublicKey, 0, dhpkSize);
        response.get(publicKeySig, 0, dhpkSsize);

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
            String jwtToken = (new String(attestationToken, UTF_8)).trim();
            if (jwtToken.startsWith("\"") && jwtToken.endsWith("\"")) {
                jwtToken = jwtToken.substring(1, jwtToken.length() - 1);
            }
            String[] splitString = jwtToken.split("\\.", -1);
            if (3 != splitString.length) {
                SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_AasJWTError"),
                        "0", false);
            }

            java.util.Base64.Decoder decoder = Base64.getUrlDecoder();
            String header = new String(decoder.decode(splitString[0]), UTF_8);
            String body = new String(decoder.decode(splitString[1]), UTF_8);
            byte[] stmtSig = decoder.decode(splitString[2]);

            JsonObject headerJsonObject = JsonParser.parseString(header).getAsJsonObject();
            validateTokenAlgorithm(headerJsonObject);
            String keyID = getRequiredStringClaim(headerJsonObject, "kid", "R_AasJWTError");

            URI attestationUri = new URI(attestationUrl);
            if (!isValidHttpsAuthority(attestationUri)) {
                SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_AasJWTError"),
                        "0", false);
            }

            JsonArray keys = null;
            JWTCertificateEntry cacheEntry = certificateCache.get(attestationUrl);
            if (null != cacheEntry && !cacheEntry.expired()) {
                keys = cacheEntry.getCertificates();
            } else if (null != cacheEntry && cacheEntry.expired()) {
                certificateCache.remove(attestationUrl);
            }

            if (null == keys) {
                // Use the attestation URL to find where our keys are
                URL wellKnownUrl = new URL("https", attestationUri.getHost(), attestationUri.getPort(),
                        "/.well-known/openid-configuration");
                URLConnection con = wellKnownUrl.openConnection();
                String wellKnownUrlJson = Util.convertInputStreamToString(con.getInputStream());
                JsonObject attestationJson = JsonParser.parseString(wellKnownUrlJson).getAsJsonObject();
                // Get our Keys
                URL jwksUrl = new URL(attestationJson.get("jwks_uri").getAsString());
                if (!"https".equalsIgnoreCase(jwksUrl.getProtocol())) {
                    SQLServerException.makeFromDriverError(null, this,
                            SQLServerResource.getResource("R_AasJWTError"), "0", false);
                }
                URLConnection jwksCon = jwksUrl.openConnection();
                String jwksUrlJson = Util.convertInputStreamToString(jwksCon.getInputStream());
                JsonObject jwksJson = JsonParser.parseString(jwksUrlJson).getAsJsonObject();
                keys = jwksJson.get("keys").getAsJsonArray();
                certificateCache.put(attestationUrl, new JWTCertificateEntry(keys));
            }
            // Find the specific keyID we need from our header

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
                    byte[] signatureBytes = (splitString[0] + "." + splitString[1]).getBytes(UTF_8);
                    for (JsonElement jsonCert : certsFromServer) {
                        CertificateFactory cf = CertificateFactory.getInstance("X.509");
                        X509Certificate cert = (X509Certificate) cf.generateCertificate(
                                new ByteArrayInputStream(java.util.Base64.getDecoder().decode(jsonCert.getAsString())));
                        Signature sig = Signature.getInstance("SHA256withRSA"); // CodeQL [SM05136] Required for an external standard: Azure Attestation Service JWT attestation tokens are expected to use RS256, which maps to SHA256withRSA in Java; the driver intentionally verifies using this fixed algorithm for AAS tokens (https://learn.microsoft.com/azure/attestation/overview)
                        sig.initVerify(cert.getPublicKey());
                        sig.update(signatureBytes);
                        if (sig.verify(stmtSig)) {
                            JsonObject bodyJsonObject = JsonParser.parseString(body).getAsJsonObject();
                            validateTokenClaims(bodyJsonObject, attestationUrl, nonce);
                            return;
                        }
                    }
                }
            }
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_AasJWTError"), "0",
                    false);
        } catch (IOException | GeneralSecurityException | URISyntaxException | JsonParseException
                | IllegalArgumentException | IllegalStateException e) {
            SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "", false, e);
        }
    }

    /*
     * Validates claims after the token signature has been verified. In addition to binding the token to the enclave
     * key, the claims must bind it to the configured attestation provider and its validity window.
     */
    void validateTokenClaims(JsonObject bodyJsonObject, String attestationUrl,
            byte[] nonce) throws SQLServerException {
        String issuer = getRequiredStringClaim(bodyJsonObject, "iss", "R_AasTokenIssuerError");
        if (!issuerMatchesConfiguredUrl(issuer, attestationUrl)) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_AasTokenIssuerError"), "0", false);
        }

        validateTokenLifetime(bodyJsonObject);

        String aasEhd = getRequiredStringClaim(bodyJsonObject, "aas-ehd", "R_AasEhdError");
        try {
            if (!Arrays.equals(Base64.getUrlDecoder().decode(aasEhd), enclavePK)) {
                SQLServerException.makeFromDriverError(null, this,
                        SQLServerResource.getResource("R_AasEhdError"), "0", false);
            }

            if (this.enclaveType == 1) {
                // VBS tokens bind the attestation response to the nonce generated for this request.
                String rpData = getRequiredStringClaim(bodyJsonObject, "rp_data", "R_VbsRpDataError");
                if (!Arrays.equals(Base64.getUrlDecoder().decode(rpData), nonce)) {
                    SQLServerException.makeFromDriverError(null, this,
                            SQLServerResource.getResource("R_VbsRpDataError"), "0", false);
                }
            }
        } catch (IllegalArgumentException e) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_AasJWTError"), "0",
                    false, e);
        }
    }

    /*
     * MAA token issuers contain only an authority. Paths, queries, fragments, and user information are rejected rather
     * than discarded so that every accepted issuer has one unambiguous representation.
     */
    private static boolean issuerMatchesConfiguredUrl(String issuer, String attestationUrl) {
        try {
            URI issuerUri = new URI(issuer);
            URI configuredUri = new URI(attestationUrl);
            if (!isValidHttpsAuthority(issuerUri) || !isValidHttpsAuthority(configuredUri)
                    || null != issuerUri.getQuery() || null != issuerUri.getFragment()
                    || (null != issuerUri.getPath() && !issuerUri.getPath().isEmpty())) {
                return false;
            }

            return issuerUri.getHost().equalsIgnoreCase(configuredUri.getHost())
                    && effectivePort(issuerUri) == effectivePort(configuredUri);
        } catch (URISyntaxException e) {
            return false;
        }
    }

    private static boolean isValidHttpsAuthority(URI uri) {
        return "https".equalsIgnoreCase(uri.getScheme()) && null != uri.getHost() && null == uri.getUserInfo();
    }

    private static int effectivePort(URI uri) {
        return -1 == uri.getPort() ? 443 : uri.getPort();
    }

    /*
     * MAA tokens require an expiration time. The optional not-before time is also enforced when present. The skew
     * allowance matches the common JWT validation default and prevents minor client/service clock differences from
     * rejecting an otherwise current token.
     */
    private void validateTokenLifetime(JsonObject bodyJsonObject) throws SQLServerException {
        long expiration = getRequiredNumericDate(bodyJsonObject, "exp");
        long now = Instant.now().getEpochSecond();
        if (expiration <= now - TOKEN_CLOCK_SKEW_SECONDS) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_AasTokenLifetimeError"), "0", false);
        }

        JsonElement notBeforeElement = bodyJsonObject.get("nbf");
        if (null != notBeforeElement) {
            long notBefore = getNumericDate(notBeforeElement);
            if (notBefore >= expiration || notBefore > now + TOKEN_CLOCK_SKEW_SECONDS) {
                SQLServerException.makeFromDriverError(null, this,
                        SQLServerResource.getResource("R_AasTokenLifetimeError"), "0", false);
            }
        }
    }

    private long getRequiredNumericDate(JsonObject claims, String claimName) throws SQLServerException {
        JsonElement claim = claims.get(claimName);
        if (null == claim) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_AasTokenLifetimeError"), "0", false);
        }
        return getNumericDate(claim);
    }

    private long getNumericDate(JsonElement claim) throws SQLServerException {
        if (!claim.isJsonPrimitive() || !claim.getAsJsonPrimitive().isNumber()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_AasTokenLifetimeError"), "0", false);
        }
        try {
            return Long.parseLong(claim.getAsString());
        } catch (NumberFormatException e) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_AasTokenLifetimeError"), "0", false, e);
            return 0;
        }
    }

    private String getRequiredStringClaim(JsonObject claims, String claimName,
            String resourceKey) throws SQLServerException {
        JsonElement claim = claims.get(claimName);
        if (null == claim || !claim.isJsonPrimitive()) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource(resourceKey), "0",
                    false);
        }

        JsonPrimitive claimValue = claim.getAsJsonPrimitive();
        if (!claimValue.isString() || claimValue.getAsString().isEmpty()) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource(resourceKey), "0",
                    false);
        }
        return claimValue.getAsString();
    }

    private void validateTokenAlgorithm(JsonObject headerJsonObject) throws SQLServerException {
        String algorithm = getRequiredStringClaim(headerJsonObject, "alg", "R_AasJWTError");
        if (!EXPECTED_JWT_ALGORITHM.equals(algorithm)) {
            SQLServerException.makeFromDriverError(null, this, SQLServerResource.getResource("R_AasJWTError"), "0",
                    false);
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
