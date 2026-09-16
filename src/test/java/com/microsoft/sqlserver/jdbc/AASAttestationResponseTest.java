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
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


class AASAttestationResponseTest {
    private static final String ATTESTATION_URL = "https://customer.weu.attest.azure.net/attest/SgxEnclave?api-version=2022-08-01";
    private static final String KEY_ID = "aas-test-key";
    private static final byte[] ENCLAVE_PUBLIC_KEY = {1, 2, 3, 4};
    private static final byte[] NONCE = {5, 6, 7, 8};

    private static PrivateKey signingKey;
    private static X509Certificate signingCertificate;
    private static ConcurrentHashMap<String, JWTCertificateEntry> certificateCache;
    private static JWTCertificateEntry previousCertificateEntry;

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
        cacheSigningCertificate();
    }

    @AfterAll
    static void restoreCertificateCache() {
        if (null != certificateCache) {
            if (null == previousCertificateEntry) {
                certificateCache.remove(ATTESTATION_URL);
            } else {
                certificateCache.put(ATTESTATION_URL, previousCertificateEntry);
            }
        }
    }

    @Test
    void validSignedTokenIsAccepted() throws Exception {
        JsonObject claims = validClaims();
        AASAttestationResponse response = createResponse(2, createSignedToken(claims));

        assertDoesNotThrow(() -> response.validateToken(ATTESTATION_URL, NONCE));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://customer.weu.attest.azure.net", "https://CUSTOMER.WEU.ATTEST.AZURE.NET",
            "https://customer.weu.attest.azure.net:443"})
    void equivalentIssuerAuthoritiesAreAccepted(String issuer) {
        JsonObject claims = validClaims();
        claims.addProperty("iss", issuer);

        assertDoesNotThrow(() -> validateSignedClaims(claims, 2));
    }

    @Test
    void signedTokenWithForeignIssuerIsRejected() throws Exception {
        JsonObject claims = validClaims();
        claims.addProperty("iss", "https://sharedweu.weu.attest.azure.net");
        // Both providers use the same signing key; only the issuer distinguishes their approvals.
        AASAttestationResponse response = createResponse(2, createSignedToken(claims));

        SQLServerException exception = assertThrows(SQLServerException.class,
                () -> response.validateToken(ATTESTATION_URL, NONCE));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_AasTokenIssuerError")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"https://customer.weu.attest.azure.net/another-provider",
            "https://customer.weu.attest.azure.net/", "https://customer.weu.attest.azure.net?provider=other",
            "https://customer.weu.attest.azure.net#other", "https://user@customer.weu.attest.azure.net",
            "https://customer.weu.attest.azure.net:444", "http://customer.weu.attest.azure.net",
            "https://customer.weu.attest.azure.net.example.com", "not a URI"})
    void invalidIssuerAuthoritiesAreRejected(String issuer) {
        JsonObject claims = validClaims();
        claims.addProperty("iss", issuer);

        assertClaimError(claims, "R_AasTokenIssuerError");
    }

    @Test
    void missingIssuerIsRejected() {
        JsonObject claims = validClaims();
        claims.remove("iss");

        assertClaimError(claims, "R_AasTokenIssuerError");
    }

    @Test
    void nonStringIssuerIsRejected() {
        JsonObject claims = validClaims();
        claims.addProperty("iss", 123);

        assertClaimError(claims, "R_AasTokenIssuerError");
    }

    @Test
    void expiredTokenIsRejected() {
        JsonObject claims = validClaims();
        claims.addProperty("exp", Instant.now().getEpochSecond() - 3600);
        assertClaimError(claims, "R_AasTokenLifetimeError");
    }

    @Test
    void notYetValidTokenIsRejected() {
        JsonObject claims = validClaims();
        claims.addProperty("nbf", Instant.now().getEpochSecond() + 3600);
        assertClaimError(claims, "R_AasTokenLifetimeError");
    }

    @Test
    void missingExpirationIsRejected() {
        JsonObject claims = validClaims();
        claims.remove("exp");
        assertClaimError(claims, "R_AasTokenLifetimeError");
    }

    @Test
    void nonNumericExpirationIsRejected() {
        JsonObject claims = validClaims();
        claims.addProperty("exp", "not-a-number");
        assertClaimError(claims, "R_AasTokenLifetimeError");
    }

    @Test
    void invalidValidityWindowIsRejected() {
        JsonObject claims = validClaims();
        long expiration = Instant.now().getEpochSecond() + 3600;
        claims.addProperty("exp", expiration);
        claims.addProperty("nbf", expiration);
        assertClaimError(claims, "R_AasTokenLifetimeError");
    }

    @Test
    void validVbsClaimsAreAccepted() {
        JsonObject claims = validClaims();
        claims.addProperty("rp_data", Base64.getUrlEncoder().withoutPadding().encodeToString(NONCE));
        assertDoesNotThrow(() -> validateSignedClaims(claims, 1));
    }

    @Test
    void missingNotBeforeIsAccepted() {
        JsonObject claims = validClaims();
        claims.remove("nbf");

        assertDoesNotThrow(() -> validateSignedClaims(claims, 2));
    }

    @Test
    void clockSkewWithinAllowanceIsAccepted() {
        JsonObject recentlyExpiredClaims = validClaims();
        recentlyExpiredClaims.addProperty("exp", Instant.now().getEpochSecond() - 60);
        recentlyExpiredClaims.remove("nbf");
        assertDoesNotThrow(() -> validateSignedClaims(recentlyExpiredClaims, 2));

        JsonObject notYetValidClaims = validClaims();
        notYetValidClaims.addProperty("nbf", Instant.now().getEpochSecond() + 60);
        assertDoesNotThrow(() -> validateSignedClaims(notYetValidClaims, 2));
    }

    @Test
    void mismatchedEnclaveKeyIsRejected() {
        JsonObject claims = validClaims();
        claims.addProperty("aas-ehd", Base64.getUrlEncoder().encodeToString(NONCE));

        assertClaimError(claims, "R_AasEhdError");
    }

    @Test
    void mismatchedVbsNonceIsRejected() {
        JsonObject claims = validClaims();
        claims.addProperty("rp_data", Base64.getUrlEncoder().encodeToString(ENCLAVE_PUBLIC_KEY));

        SQLServerException exception = assertThrows(SQLServerException.class, () -> validateSignedClaims(claims, 1));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_VbsRpDataError")));
    }

    @ParameterizedTest
    @ValueSource(strings = {"aas-ehd", "rp_data"})
    void malformedBindingClaimIsRejected(String claimName) {
        JsonObject claims = validClaims();
        claims.addProperty("rp_data", Base64.getUrlEncoder().encodeToString(NONCE));
        claims.addProperty(claimName, "not base64!");

        SQLServerException exception = assertThrows(SQLServerException.class, () -> validateSignedClaims(claims, 1));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_AasJWTError")));
    }

    @Test
    void modifiedSignedPayloadIsRejected() throws Exception {
        String token = createSignedToken(validClaims());
        JsonObject modifiedClaims = validClaims();
        modifiedClaims.addProperty("jti", "modified");
        String encodedClaims = Base64.getUrlEncoder().withoutPadding()
                .encodeToString(modifiedClaims.toString().getBytes(StandardCharsets.UTF_8));
        String modifiedToken = token.substring(0, token.indexOf('.') + 1) + encodedClaims
                + token.substring(token.lastIndexOf('.'));

        assertTokenError(createResponse(2, modifiedToken));
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
        certificateCache = (ConcurrentHashMap<String, JWTCertificateEntry>) cacheField.get(null);
        previousCertificateEntry = certificateCache.put(ATTESTATION_URL, new JWTCertificateEntry(keys));
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

    private static void validateSignedClaims(JsonObject claims, int enclaveType) throws Exception {
        AASAttestationResponse response = createResponse(enclaveType, createSignedToken(claims));
        response.validateToken(ATTESTATION_URL, NONCE);
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

    private static void assertClaimError(JsonObject claims, String resourceKey) {
        SQLServerException exception = assertThrows(SQLServerException.class, () -> validateSignedClaims(claims, 2));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg(resourceKey)));
    }

    private static void assertTokenError(AASAttestationResponse response) {
        SQLServerException exception = assertThrows(SQLServerException.class,
                () -> response.validateToken(ATTESTATION_URL, NONCE));
        assertTrue(exception.getMessage().matches(TestUtils.formatErrorMsg("R_AasJWTError")));
    }
}
