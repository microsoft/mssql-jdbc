/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.lang.reflect.Field;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.PrivateKey;
import java.security.Signature;
import java.security.interfaces.ECPublicKey;
import java.security.interfaces.RSAPublicKey;
import java.security.spec.ECGenParameterSpec;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.Collections;
import java.util.Date;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.bouncycastle.asn1.x500.X500Name;
import org.bouncycastle.cert.jcajce.JcaX509CertificateConverter;
import org.bouncycastle.cert.jcajce.JcaX509v3CertificateBuilder;
import org.bouncycastle.operator.jcajce.JcaContentSignerBuilder;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

import com.google.gson.JsonArray;
import com.google.gson.JsonObject;


/**
 * Exercises real attestation validation, session derivation and caches with mocked connection properties and SQL transport.
 * Unique database names isolate cache entries without clearing other tests' sessions or metadata.
 */
class AASAttestationRetryTest {
    private static final String ATTESTATION_URL = "https://retry-test.attest.azure.net";
    private static final String SERVER = "aas-retry-test-server";
    private static final String SQL = "SELECT ?";
    private static final String KEY_ID = "aas-retry-test-key";
    private static KeyPair signer;
    private static KeyPair enclaveRsa;
    private static ConcurrentHashMap<String, JWTCertificateEntry> certificateCache;
    private static JWTCertificateEntry previousCertificate;

    private enum ResponseKind {
        VALID(null),
        FOREIGN_ISSUER("R_AasTokenIssuerError"),
        EXPIRED("R_AasTokenLifetimeError"),
        BAD_TOKEN_SIGNATURE("R_AasJWTError"),
        BAD_DH_SIGNATURE("R_InvalidDHKeySignature"),
        BAD_DH_HEADER("R_MalformedECDHHeader");

        final String errorResource;

        ResponseKind(String errorResource) {
            this.errorResource = errorResource;
        }
    }

    @BeforeAll
    @SuppressWarnings("unchecked")
    static void createSigningCertificate() throws Exception {
        KeyPairGenerator generator = KeyPairGenerator.getInstance("RSA");
        generator.initialize(2048);
        signer = generator.generateKeyPair();
        enclaveRsa = generator.generateKeyPair();
        Instant now = Instant.now();
        X500Name subject = new X500Name("CN=AAS Attestation Retry Test");
        byte[] certificate = new JcaX509CertificateConverter()
                .getCertificate(new JcaX509v3CertificateBuilder(subject, BigInteger.ONE,
                        Date.from(now.minusSeconds(60)), Date.from(now.plusSeconds(3600)), subject, signer.getPublic())
                        .build(new JcaContentSignerBuilder("SHA256withRSA").build(signer.getPrivate())))
                .getEncoded();
        JsonObject key = new JsonObject();
        key.addProperty("kid", KEY_ID);
        JsonArray chain = new JsonArray();
        chain.add(Base64.getEncoder().encodeToString(certificate));
        key.add("x5c", chain);
        JsonArray keys = new JsonArray();
        keys.add(key);
        certificateCache = (ConcurrentHashMap<String, JWTCertificateEntry>) field(AASAttestationResponse.class,
                "certificateCache").get(null);
        previousCertificate = certificateCache.put(ATTESTATION_URL, new JWTCertificateEntry(keys));
    }

    @AfterAll
    static void restoreCertificateCache() {
        if (null != certificateCache) {
            if (null == previousCertificate) {
                certificateCache.remove(ATTESTATION_URL);
            } else {
                certificateCache.put(ATTESTATION_URL, previousCertificate);
            }
        }
    }

    static Stream<Arguments> invalidResponses() {
        return Stream.of(ColumnEncryptionVersion.AE_V3, ColumnEncryptionVersion.AE_V2)
                .flatMap(version -> Arrays.stream(ResponseKind.values()).filter(kind -> kind != ResponseKind.VALID)
                        .flatMap(kind -> Stream.of(1, 2).map(type -> Arguments.of(version, kind, type))));
    }

    static Stream<Arguments> versionsAndEnclaveTypes() {
        return Stream.of(ColumnEncryptionVersion.AE_V3, ColumnEncryptionVersion.AE_V2)
                .flatMap(version -> Stream.of(1, 2).map(type -> Arguments.of(version, type)));
    }

    @ParameterizedTest(name = "{0}/{1}/enclaveType={2}")
    @MethodSource("invalidResponses")
    void rejectedResponsesRequireFreshAttestationAndCanRecover(ColumnEncryptionVersion version, ResponseKind kind,
            int enclaveType) throws Exception {
        try (Fixture fixture = new Fixture(version, enclaveType)) {
            fixture.respondWith(kind);
            for (int attempt = 1; attempt <= 2; attempt++) {
                SQLServerException error = assertThrows(SQLServerException.class, fixture::execute);
                assertEquals(SQLServerResource.getResource(kind.errorResource), error.getMessage());
                fixture.assertNoSession();
                assertEquals(attempt, fixture.executions.get());
                assertEquals(version == ColumnEncryptionVersion.AE_V3, fixture.metadataCached());
            }
            fixture.assertNoPendingResponse();

            fixture.respondWith(ResponseKind.VALID);
            fixture.execute();
            assertEquals(3, fixture.executions.get());
            fixture.assertEstablishedSession();
            fixture.assertNoPendingResponse();
        }
    }

    @ParameterizedTest(name = "{0}/enclaveType={1}")
    @MethodSource("versionsAndEnclaveTypes")
    void establishedSessionIsReused(ColumnEncryptionVersion version, int enclaveType) throws Exception {
        try (Fixture fixture = new Fixture(version, enclaveType)) {
            fixture.execute();
            EnclaveSession session = fixture.assertEstablishedSession();
            fixture.assertNoPendingResponse();

            fixture.execute();
            assertSame(session, fixture.assertEstablishedSession());
            assertEquals(version == ColumnEncryptionVersion.AE_V3 ? 1 : 2, fixture.executions.get());
            fixture.assertNoPendingResponse();
        }
    }

    @ParameterizedTest(name = "{0}/enclaveType={1}")
    @MethodSource("versionsAndEnclaveTypes")
    void cachedSessionIsReusedByAnotherProvider(ColumnEncryptionVersion version, int enclaveType) throws Exception {
        try (Fixture fixture = new Fixture(version, enclaveType)) {
            fixture.execute();
            EnclaveSession session = fixture.assertEstablishedSession();

            SQLServerAASEnclaveProvider otherProvider = new SQLServerAASEnclaveProvider();
            otherProvider.getAttestationParameters(ATTESTATION_URL);
            SQLServerConnection otherConnection = fixture.connectionFor(otherProvider);
            otherProvider.createEnclaveSession(otherConnection, fixture.statement, SQL, "@p0 int", fixture.parameters,
                    fixture.names);

            assertSame(session, otherProvider.getEnclaveSession());
            assertEquals(version == ColumnEncryptionVersion.AE_V3 ? 1 : 2, fixture.executions.get());
        }
    }

    @ParameterizedTest
    @ValueSource(ints = {1, 2})
    void rejectedResponseDoesNotInvalidateUnrelatedSession(int enclaveType) throws Exception {
        try (Fixture valid = new Fixture(ColumnEncryptionVersion.AE_V3, enclaveType);
                Fixture rejected = new Fixture(ColumnEncryptionVersion.AE_V3, enclaveType)) {
            valid.execute();
            EnclaveSession session = valid.assertEstablishedSession();
            rejected.respondWith(ResponseKind.FOREIGN_ISSUER);

            for (int attempt = 0; attempt < 2; attempt++) {
                assertThrows(SQLServerException.class, rejected::execute);
                rejected.assertNoSession();
                assertSame(session, valid.assertEstablishedSession());
            }
            valid.execute();
            assertEquals(1, valid.executions.get());
            assertSame(session, valid.assertEstablishedSession());
        }
    }

    @ParameterizedTest(name = "{0}/enclaveType={1}")
    @MethodSource("versionsAndEnclaveTypes")
    void transportFailureAfterValidationDoesNotRetainResponse(ColumnEncryptionVersion version,
            int enclaveType) throws Exception {
        try (Fixture fixture = new Fixture(version, enclaveType)) {
            // Statement close happens after the token and DH signatures have been validated.
            fixture.failOnClose = true;
            SQLServerException error = assertThrows(SQLServerException.class, fixture::execute);
            assertEquals(SQLServerResource.getResource("R_UnableRetrieveParameterMetadata"), error.getMessage());
            assertTrue(error.getCause() instanceof SQLException);
            fixture.assertNoSession();
            fixture.assertNoPendingResponse();

            fixture.failOnClose = false;
            fixture.respondWith(ResponseKind.EXPIRED);
            SQLServerException retryError = assertThrows(SQLServerException.class, fixture::execute);
            assertEquals(SQLServerResource.getResource("R_AasTokenLifetimeError"), retryError.getMessage());
            fixture.assertNoSession();

            fixture.respondWith(ResponseKind.VALID);
            fixture.execute();
            assertEquals(3, fixture.executions.get());
            fixture.assertEstablishedSession();
            fixture.assertNoPendingResponse();
        }
    }

    private static final class Fixture implements AutoCloseable {
        final SQLServerAASEnclaveProvider provider = new SQLServerAASEnclaveProvider();
        final SQLServerConnection connection;
        final SQLServerStatement statement = mock(SQLServerStatement.class);
        final Parameter[] parameters = {new Parameter(false)};
        final ArrayList<String> names = new ArrayList<>(Collections.singletonList("@p0"));
        final AtomicInteger executions = new AtomicInteger();
        final String database = "aas-retry-" + UUID.randomUUID();
        final ColumnEncryptionVersion version;
        final int enclaveType;
        final AASAttestationParameters request;
        final EnclaveSessionCache sessions;
        byte[] wireResponse;
        boolean failOnClose;

        Fixture(ColumnEncryptionVersion version, int enclaveType) throws Exception {
            this.version = version;
            this.enclaveType = enclaveType;
            provider.getAttestationParameters(ATTESTATION_URL);
            request = (AASAttestationParameters) field(SQLServerAASEnclaveProvider.class, "aasParams").get(provider);
            sessions = (EnclaveSessionCache) field(SQLServerAASEnclaveProvider.class, "enclaveCache").get(null);
            connection = connectionFor(provider);
            respondWith(ResponseKind.VALID);
            assertNoSession();
            assertFalse(metadataCached());
        }

        SQLServerConnection connectionFor(SQLServerAASEnclaveProvider enclaveProvider) throws Exception {
            SQLServerConnection conn = mock(SQLServerConnection.class);
            conn.activeConnectionProperties = new Properties();
            conn.activeConnectionProperties.setProperty("databaseName", database);
            field(SQLServerConnection.class, "enclaveAttestationUrl").set(conn, ATTESTATION_URL);
            field(SQLServerConnection.class, "enclaveProvider").set(conn, enclaveProvider);
            when(conn.enclaveEstablished()).thenCallRealMethod();
            when(conn.getServerName()).thenReturn(SERVER);
            when(conn.getCatalog()).thenReturn(database);
            when(conn.getServerColumnEncryptionVersion()).thenReturn(version);
            when(conn.isAEv2()).thenReturn(true);
            when(conn.prepareStatement(anyString())).thenAnswer(invocation -> transport(invocation.getArgument(0)));
            return conn;
        }

        void respondWith(ResponseKind kind) throws Exception {
            wireResponse = response(request, kind, enclaveType);
            if (kind != ResponseKind.BAD_DH_SIGNATURE) {
                // Token rejection fixtures have valid signed DH material, not a second reason to reject.
                new AASAttestationResponse(wireResponse).validateDHPublicKey(request.getNonce());
            }
        }

        void execute() throws SQLServerException {
            provider.createEnclaveSession(connection, statement, SQL, "@p0 int", parameters, names);
        }

        boolean metadataCached() throws SQLServerException {
            return ParameterMetaDataCache.getQueryMetadata(parameters, names, connection, statement, SQL);
        }

        void assertNoSession() {
            assertNull(provider.getEnclaveSession());
            assertNull(sessions.getSession(SERVER + database + ATTESTATION_URL));
        }

        void assertNoPendingResponse() throws Exception {
            assertNull(field(SQLServerAASEnclaveProvider.class, "hgsResponse").get(provider));
        }

        EnclaveSession assertEstablishedSession() {
            EnclaveSession session = provider.getEnclaveSession();
            assertNotNull(session);
            assertEquals(32, session.getSessionSecret().length);
            EnclaveCacheEntry entry = sessions.getSession(SERVER + database + ATTESTATION_URL);
            assertNotNull(entry);
            assertSame(session, entry.getEnclaveSession());
            return session;
        }

        private SQLServerPreparedStatement transport(String query) throws Exception {
            SQLServerPreparedStatement stmt = mock(SQLServerPreparedStatement.class);
            ResultSet keys = mock(ResultSet.class);
            ResultSet metadata = mock(ResultSet.class);
            when(metadata.next()).thenReturn(true, false);
            when(metadata.getString(DescribeParameterEncryptionResultSet2.PARAMETERNAME.value())).thenReturn("@p0");
            when(metadata.getInt(DescribeParameterEncryptionResultSet2.COLUMNENCRYPTIONTYPE.value()))
                    .thenReturn((int) SQLServerEncryptionType.PLAINTEXT.value);
            SQLServerResultSet attestation = mock(SQLServerResultSet.class);
            when(attestation.next()).thenReturn(true, false);
            when(attestation.getBytes(1)).thenReturn(wireResponse);
            when(stmt.executeQueryInternal()).thenAnswer(invocation -> {
                executions.incrementAndGet();
                return keys;
            });
            when(stmt.getMoreResults()).thenReturn(true, query.endsWith("?,?,?"), false);
            when(stmt.getResultSet()).thenReturn(metadata, attestation);
            doAnswer(invocation -> {
                if (failOnClose) {
                    throw new SQLException("Simulated SDPE statement close failure");
                }
                return null;
            }).when(stmt).close();
            return stmt;
        }

        @Override
        public void close() {
            ParameterMetaDataCache.removeCacheEntry(connection, SQL);
            provider.invalidateEnclaveSession();
        }
    }

    private static byte[] response(AASAttestationParameters request, ResponseKind kind,
            int enclaveType) throws Exception {
        RSAPublicKey rsa = (RSAPublicKey) enclaveRsa.getPublic();
        byte[] exponent = unsigned(rsa.getPublicExponent());
        byte[] modulus = unsigned(rsa.getModulus());
        byte[] identity = ByteBuffer.allocate(24 + exponent.length + modulus.length).order(ByteOrder.LITTLE_ENDIAN)
                .putInt(0x31415352).putInt(2048).putInt(exponent.length).putInt(modulus.length).putInt(0).putInt(0)
                .put(exponent).put(modulus).array();
        if (enclaveType == 2) {
            for (int i = 0; i < identity.length; i++) {
                identity[i] ^= request.getNonce()[i % request.getNonce().length];
            }
        }
        KeyPairGenerator ec = KeyPairGenerator.getInstance("EC");
        ec.initialize(new ECGenParameterSpec("secp384r1"));
        ECPublicKey dh = (ECPublicKey) ec.generateKeyPair().getPublic();
        byte[] dhBlob = ByteBuffer.allocate(104).put(BaseAttestationRequest.ECDH_MAGIC)
                .put(coordinate(dh.getW().getAffineX())).put(coordinate(dh.getW().getAffineY())).array();
        if (kind == ResponseKind.BAD_DH_HEADER) {
            dhBlob[0] ^= 1;
        }
        byte[] dhSignature = sign(dhBlob, enclaveRsa.getPrivate());
        if (kind == ResponseKind.BAD_DH_SIGNATURE) {
            dhSignature[0] ^= 1;
        }

        long now = Instant.now().getEpochSecond();
        JsonObject claims = new JsonObject();
        claims.addProperty("iss",
                kind == ResponseKind.FOREIGN_ISSUER ? "https://different.attest.azure.net" : ATTESTATION_URL);
        claims.addProperty("nbf", now - 7200);
        claims.addProperty("exp", now + (kind == ResponseKind.EXPIRED ? -3600 : 3600));
        claims.addProperty("aas-ehd", encode(identity));
        claims.addProperty("rp_data", encode(request.getNonce()));
        JsonObject header = new JsonObject();
        header.addProperty("alg", "RS256");
        header.addProperty("kid", KEY_ID);
        String input = encode(header.toString().getBytes(StandardCharsets.UTF_8)) + "."
                + encode(claims.toString().getBytes(StandardCharsets.UTF_8));
        byte[] tokenSignature = sign(input.getBytes(StandardCharsets.UTF_8), signer.getPrivate());
        if (kind == ResponseKind.BAD_TOKEN_SIGNATURE) {
            tokenSignature[0] ^= 1;
        }
        byte[] token = (input + "." + encode(tokenSignature)).getBytes(StandardCharsets.UTF_8);
        byte[] sessionId = ByteBuffer.allocate(8).putLong(UUID.randomUUID().getLeastSignificantBits()).array();
        int length = 36 + identity.length + token.length + dhBlob.length + dhSignature.length;
        return ByteBuffer.allocate(length).order(ByteOrder.LITTLE_ENDIAN).putInt(length).putInt(identity.length)
                .putInt(token.length).putInt(enclaveType).put(identity).put(token)
                .putInt(16 + dhBlob.length + dhSignature.length).put(sessionId).putInt(dhBlob.length)
                .putInt(dhSignature.length).put(dhBlob).put(dhSignature).array();
    }

    private static byte[] sign(byte[] input, PrivateKey key) throws Exception {
        Signature signature = Signature.getInstance("SHA256withRSA");
        signature.initSign(key);
        signature.update(input);
        return signature.sign();
    }

    private static byte[] unsigned(BigInteger integer) {
        byte[] bytes = integer.toByteArray();
        return bytes[0] == 0 ? Arrays.copyOfRange(bytes, 1, bytes.length) : bytes;
    }

    private static byte[] coordinate(BigInteger integer) {
        byte[] bytes = unsigned(integer);
        byte[] result = new byte[48];
        System.arraycopy(bytes, 0, result, 48 - bytes.length, bytes.length);
        return result;
    }

    private static String encode(byte[] bytes) {
        return Base64.getUrlEncoder().withoutPadding().encodeToString(bytes);
    }

    private static Field field(Class<?> type, String name) throws Exception {
        Field field = type.getDeclaredField(name);
        field.setAccessible(true);
        return field;
    }
}
