/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
import java.security.SecureRandom;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;


/**
 * 
 * Provides the implementation of the NONE Enclave Provider. This enclave provider does not use attestation.
 *
 */
public class SQLServerNoneEnclaveProvider implements ISQLServerEnclaveProvider {

    private static final EnclaveSessionCache enclaveCache = new EnclaveSessionCache();

    private NoneAttestationParameters noneParams = null;
    private NoneAttestationResponse noneResponse = null;
    private String attestationUrl = null;
    private EnclaveSession enclaveSession = null;
    
    @Override
    public void getAttestationParameters(String url) throws SQLServerException {
        if (null == noneParams) {
            attestationUrl = url;
            try {
                noneParams = new NoneAttestationParameters(attestationUrl);
            } catch (IOException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
            }
        }
    }
    
    @Override
    public ArrayList<byte[]> createEnclaveSession(SQLServerConnection connection, SQLServerStatement statement,
            String userSql, String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {

        /*
         * for None attestation: enclave does not send public key, and sends an empty attestation info. The only
         * non-trivial content it sends is the session setup info (DH pubkey of enclave).
         */

        // Check if the session exists in our cache
        StringBuilder keyLookup = new StringBuilder(connection.getServerName()).append(connection.getCatalog())
                .append(attestationUrl);
        EnclaveCacheEntry entry = enclaveCache.getSession(keyLookup.toString());
        if (null != entry) {
            this.enclaveSession = entry.getEnclaveSession();
            this.noneParams = (NoneAttestationParameters) entry.getBaseAttestationRequest();
        }
        ArrayList<byte[]> b = describeParameterEncryption(connection, statement, userSql, preparedTypeDefinitions,
                params, parameterNames);
        if (connection.enclaveEstablished()) {
            return b;
        } else if (null != noneResponse && !connection.enclaveEstablished()) {
            try {
                enclaveSession = new EnclaveSession(noneResponse.getSessionID(),
                        noneParams.createSessionSecret(noneResponse.getDHpublicKey()));
                enclaveCache.addEntry(connection.getServerName(), connection.getCatalog(),
                        connection.enclaveAttestationUrl, noneParams, enclaveSession);
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
        noneParams = null;
        attestationUrl = null;
    }

    @Override
    public EnclaveSession getEnclaveSession() {
        return enclaveSession;
    }

    private void validateAttestationResponse() throws SQLServerException {
        if (null != noneResponse) {
            try {
                noneResponse.validateDHPublicKey(noneParams.getNonce());
            } catch (GeneralSecurityException e) {
                SQLServerException.makeFromDriverError(null, this, e.getLocalizedMessage(), "0", false);
            }
        }
    }

    private ArrayList<byte[]> describeParameterEncryption(SQLServerConnection connection, SQLServerStatement statement,
            String userSql, String preparedTypeDefinitions, Parameter[] params,
            ArrayList<String> parameterNames) throws SQLServerException {
        ArrayList<byte[]> enclaveRequestedCEKs = new ArrayList<>();
        try (PreparedStatement stmt = connection.prepareStatement(connection.enclaveEstablished() ? SDPE1 : SDPE2)) {
            try (ResultSet rs = connection.enclaveEstablished() ? executeSDPEv1(stmt, userSql,
                    preparedTypeDefinitions) : executeSDPEv2(stmt, userSql, preparedTypeDefinitions, noneParams)) {
                if (null == rs) {
                    // No results. Meaning no parameter.
                    // Should never happen.
                    return enclaveRequestedCEKs;
                }
                processSDPEv1(userSql, preparedTypeDefinitions, params, parameterNames, connection, statement, stmt, rs,
                        enclaveRequestedCEKs);
                // Process the third result set.
                if (connection.isAEv2() && stmt.getMoreResults()) {
                    try (ResultSet hgsRs = stmt.getResultSet()) {
                        if (hgsRs.next()) {
                            noneResponse = new NoneAttestationResponse(hgsRs.getBytes(1));
                            // This validates and establishes the enclave session if valid
                            validateAttestationResponse();
                        } else {
                            SQLServerException.makeFromDriverError(null, this,
                                    SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), "0", false);
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


/**
 * 
 * Represents the serialization of the request the client sends to the 
 * SQL Server while setting up a session.
 *
 */
class NoneAttestationParameters extends BaseAttestationRequest {

    // Type 2 is NONE, sent as Little Endian 0x20000000
    private static final byte[] ENCLAVE_TYPE = new byte[] {0x2, 0x0, 0x0, 0x0};
    // Nonce length is always 256
    private static final byte[] NONCE_LENGTH = new byte[] {0x0, 0x1, 0x0, 0x0};
    private final byte[] nonce = new byte[256];

    NoneAttestationParameters(String attestationUrl) throws SQLServerException, IOException {
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


/**
 * 
 * Represents the deserialization of the byte payload the client receives from the
 * SQL Server while setting up a session.
 *
 */
class NoneAttestationResponse extends BaseAttestationResponse {
    
    NoneAttestationResponse(byte[] b) throws SQLServerException {
        /*-
         * Protocol format:
         * 1. Total Size of the attestation blob as UINT
         * 2. Size of Enclave RSA public key as UINT
         * 3. Size of Attestation token as UINT
         * 4. Enclave Type as UINT
         * 5. Enclave RSA public key (raw key, of length #2)
         * 6. Attestation token (of length #3)
         * 7. Size of Session ID was UINT
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
        byte[] attestationToken = new byte[attestationTokenSize];

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

    void validateDHPublicKey(byte[] nonce) throws SQLServerException, GeneralSecurityException {
        if (this.enclaveType == EnclaveType.SGX.getValue()) {
            for (int i = 0; i < enclavePK.length; i++) {
                enclavePK[i] = (byte) (enclavePK[i] ^ nonce[i % nonce.length]);
            }
        }
        validateDHPublicKey();
    }
}
