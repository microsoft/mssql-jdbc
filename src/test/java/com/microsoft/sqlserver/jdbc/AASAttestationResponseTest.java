/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.cert.X509Certificate;
import java.time.Instant;
import java.util.Base64;
import java.util.Date;
import java.util.concurrent.ConcurrentHashMap;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.X509CertificateHolder;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.ContentSigner;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


class AASAttestationResponseTest {
    private static final String ATTESTATION_URL
            = "https://customer.weu.attest.azure.net/attest/SgxEnclave?api-version=2022-08-01";
    private static final String KEY_ID = "aas-test-key";
    private static final byte[] ENCLAVE_PUBLIC_KEY = {1, 2, 3, 4};
    private static final byte[] NONCE = {5, 6, 7, 8};

    private static PrivateKey signingKey;
    private static X509Certificate signingCertificate;

    @BeforeAll
    static void createSigningCertificate() throws Exception {
        KeyPairGenerator keyPairGenerator = KeyPairGenerator.getInstance("RSA");
        keyPairGenerator.initialize(2048);
        KeyPair keyPair = keyPairGenerator.generateKeyPair();
        signingKey = keyPair.getPrivate();

        Instant now = Instant.now();
        X500Name subject = new X500Name("CN=AAS Attestation Response Test");
        JcaX509v3CertificateBuilder certificateBuilder = new JcaX509v3CertificateBuilder(subject,
                BigInteger.valueOf(now.toEpochMilli()), Date.from(now.minusSeconds(60)),
                Date.from(now.plusSeconds(3600)), subject, keyPair.getPublic());
        ContentSigner contentSigner = new JcaContentSignerBuilder("SHA256withRSA").build(signingKey);
        X509CertificateHolder certificateHolder = certificateBuilder.build(contentSigner);
        signingCertificate = new JcaX509CertificateConverter().getCertificate(certificateHolder);
    }

    @Test
    void validSignedTokenIsAccepted() throws Exception {
        JsonObject claims = validClaims();
        cacheSigningCertificate();
        AASAttestationResponse response = createResponse(2, createSignedToken(claims));

        assertDoesNotThrow(() -> response.validateToken(ATTESTATION_URL, NONCE));
    }

    @Test
    void explicitDefaultPortIssuerIsAccepted() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("iss", "https://customer.weu.attest.azure.net:443");
        AASAttestationResponse response = createResponse(2);

        assertDoesNotThrow(() -> response.validateTokenClaims(claims, ATTESTATION_URL, NONCE));
    }

    @Test
    void signedTokenWithForeignIssuerIsRejected() throws Exception {
        JsonObject claims = validClaims();
        claims.addProperty("iss", "https://sharedweu.weu.attest.azure.net");
        cacheSigningCertificate();
        AASAttestationResponse response = createResponse(2, createSignedToken(claims));

        SQLServerException exception = assertThrows(SQLServerException.class,
                () -> response.validateToken(ATTESTATION_URL, NONCE));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_AasTokenIssuerError")));
    }

    @Test
    void issuerWithPathIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("iss", "https://customer.weu.attest.azure.net/another-provider");
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenIssuerError");
    }

    @Test
    void missingIssuerIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.remove("iss");
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenIssuerError");
    }

    @Test
    void nonHttpsIssuerIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("iss", "http://customer.weu.attest.azure.net");
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenIssuerError");
    }

    @Test
    void expiredTokenIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("exp", Instant.now().getEpochSecond() - 3600);
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenLifetimeError");
    }

    @Test
    void notYetValidTokenIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("nbf", Instant.now().getEpochSecond() + 3600);
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenLifetimeError");
    }

    @Test
    void missingExpirationIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.remove("exp");
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenLifetimeError");
    }

    @Test
    void nonNumericExpirationIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("exp", "not-a-number");
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenLifetimeError");
    }

    @Test
    void invalidValidityWindowIsRejected() throws SQLServerException {
        JsonObject claims = validClaims();
        long expiration = Instant.now().getEpochSecond() + 3600;
        claims.addProperty("exp", expiration);
        claims.addProperty("nbf", expiration);
        AASAttestationResponse response = createResponse(2);

        assertClaimError(response, claims, "R_AasTokenLifetimeError");
    }

    @Test
    void validVbsClaimsAreAccepted() throws SQLServerException {
        JsonObject claims = validClaims();
        claims.addProperty("rp_data", Base64.getUrlEncoder().withoutPadding().encodeToString(NONCE));
        AASAttestationResponse response = createResponse(1);

        assertDoesNotThrow(() -> response.validateTokenClaims(claims, ATTESTATION_URL, NONCE));
    }

    @Test
    void malformedTokenIsRejected() throws SQLServerException {
        AASAttestationResponse response = createResponse(2, "only.two");

        assertTokenError(response);
    }

    @Test
    void unexpectedAlgorithmIsRejected() throws SQLServerException {
        String header = Base64.getUrlEncoder().withoutPadding()
                .encodeToString("{\"alg\":\"HS256\",\"kid\":\"key-id\"}".getBytes(StandardCharsets.UTF_8));
        String body = Base64.getUrlEncoder().withoutPadding().encodeToString("{}".getBytes(StandardCharsets.UTF_8));
        AASAttestationResponse response = createResponse(2, header + "." + body + ".AA");

        assertTokenError(response);
    }

    private static JsonObject validClaims() {
        JsonObject claims = new JsonObject();
        claims.addProperty("iss", "https://customer.weu.attest.azure.net");
        claims.addProperty("nbf", Instant.now().getEpochSecond() - 60);
        claims.addProperty("exp", Instant.now().getEpochSecond() + 3600);
        claims.addProperty("aas-ehd", Base64.getUrlEncoder().withoutPadding().encodeToString(ENCLAVE_PUBLIC_KEY));
        return claims;
    }

    @SuppressWarnings("unchecked")
    private static void cacheSigningCertificate() throws Exception {
        JsonObject key = new JsonObject();
        key.addProperty("kid", KEY_ID);
        JsonArray certificates = new JsonArray();
        certificates.add(Base64.getEncoder().encodeToString(signingCertificate.getEncoded()));
        key.add("x5c", certificates);
        JsonArray keys = new JsonArray();
        keys.add(key);

        Field cacheField = AASAttestationResponse.class.getDeclaredField("certificateCache");
        cacheField.setAccessible(true);
        ConcurrentHashMap<String, JWTCertificateEntry> cache
                = (ConcurrentHashMap<String, JWTCertificateEntry>) cacheField.get(null);
        cache.put(ATTESTATION_URL, new JWTCertificateEntry(keys));
    }

    private static String createSignedToken(JsonObject claims) throws Exception {
        JsonObject header = new JsonObject();
        header.addProperty("alg", "RS256");
        header.addProperty("kid", KEY_ID);
        String encodedHeader = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(header.toString().getBytes(StandardCharsets.UTF_8));
        String encodedClaims = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(claims.toString().getBytes(StandardCharsets.UTF_8));
        String signingInput = encodedHeader + "." + encodedClaims;

        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(signingKey);
        signature.update(signingInput.getBytes(StandardCharsets.UTF_8));
        return signingInput + "." + Base64.getUrlEncoder().withoutPadding().encodeToString(signature.sign());
    }

    private static AASAttestationResponse createResponse(int enclaveType) throws SQLServerException {
        return createResponse(enclaveType, "unused");
    }

    private static AASAttestationResponse createResponse(int enclaveType, String tokenValue) throws SQLServerException {
        byte[] token = tokenValue.getBytes(StandardCharsets.UTF_8);
        int responseLength = 36 + ENCLAVE_PUBLIC_KEY.length + token.length;
        ByteBuffer response = ByteBuffer.allocate(responseLength).order(ByteOrder.LITTLE_ENDIAN);
        response.putInt(responseLength);
        response.putInt(ENCLAVE_PUBLIC_KEY.length);
        response.putInt(token.length);
        response.putInt(enclaveType);
        response.put(ENCLAVE_PUBLIC_KEY);
        response.put(token);
        response.putInt(8);
        response.put(new byte[8]);
        response.putInt(0);
        response.putInt(0);
        return new AASAttestationResponse(response.array());
    }

    private static void assertClaimError(AASAttestationResponse response, JsonObject claims,
            String resourceKey) {
        SQLServerException exception = assertThrows(SQLServerException.class,
                () -> response.validateTokenClaims(claims, ATTESTATION_URL, NONCE));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg(resourceKey)));
    }

    private static void assertTokenError(AASAttestationResponse response) {
        SQLServerException exception = assertThrows(SQLServerException.class,
                () -> response.validateToken(ATTESTATION_URL, NONCE));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_AasJWTError")));
    }
}
