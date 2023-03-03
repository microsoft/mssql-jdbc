/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.GeneralSecurityException;
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

    /**
     * default constructor
     */
    public SQLServerNoneEnclaveProvider() {
        // default constructor
    }

    @Override
    public void getAttestationParameters(String url) throws SQLServerException {
        if (null == noneParams) {
            attestationUrl = url;
            noneParams = new NoneAttestationParameters();
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
                        preparedTypeDefinitions) : executeSDPEv2(stmt, userSql, preparedTypeDefinitions, noneParams)) {
                    if (null == rs) {
                        // No results. Meaning no parameter.
                        // Should never happen.
                        return enclaveRequestedCEKs;
                    }
                    processSDPEv1(userSql, preparedTypeDefinitions, params, parameterNames, connection, statement, stmt,
                            rs, enclaveRequestedCEKs);
                    // Process the third result set.
                    if (connection.isAEv2() && stmt.getMoreResults()) {
                        try (ResultSet noneRs = stmt.getResultSet()) {
                            if (noneRs.next()) {
                                noneResponse = new NoneAttestationResponse(noneRs.getBytes(1));
                            } else {
                                SQLServerException.makeFromDriverError(null, this,
                                        SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), "0",
                                        false);
                            }
                        }
                    }
                }
            }
        } catch (SQLServerException e) {
            throw (SQLServerException) e;
        } catch (SQLException | IOException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_UnableRetrieveParameterMetadata"), null, 0,
                    e);
        }
        return enclaveRequestedCEKs;
    }
}


/**
 * 
 * Represents the serialization of the request the client sends to the SQL Server while setting up a session.
 *
 */
class NoneAttestationParameters extends BaseAttestationRequest {

    // Type 2 is NONE, sent as Little Endian 0x20000000
    private static final byte[] ENCLAVE_TYPE = new byte[] {0x2, 0x0, 0x0, 0x0};

    NoneAttestationParameters() throws SQLServerException {
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


/**
 * 
 * Represents the deserialization of the byte payload the client receives from the SQL Server while setting up a
 * session.
 *
 */
class NoneAttestationResponse extends BaseAttestationResponse {

    NoneAttestationResponse(byte[] b) throws SQLServerException {
        /*-
         * Parse the attestation response.
         * 
         * Total Size of the response - 4B
         * Session Info Size - 4B
         * Session ID - 8B
         * DH Public Key Size - 4B
         * DH Public Key Signature Size - 4B
         * DH Public Key - dhpkSize bytes
         * DH Public Key Signature - dhpkSsize bytes
         */
        ByteBuffer response = (null != b) ? ByteBuffer.wrap(b).order(ByteOrder.LITTLE_ENDIAN) : null;
        if (null != response) {
            this.totalSize = response.getInt();
            this.sessionInfoSize = response.getInt();
            response.get(sessionID, 0, 8);
            this.dhpkSize = response.getInt();
            this.dhpkSsize = response.getInt();

            dhPublicKey = new byte[dhpkSize];
            publicKeySig = new byte[dhpkSsize];

            response.get(dhPublicKey, 0, dhpkSize);
            response.get(publicKeySig, 0, dhpkSsize);
        }

        if (null == response || 0 != response.remaining()) {
            SQLServerException.makeFromDriverError(null, this,
                    SQLServerResource.getResource("R_EnclaveResponseLengthError"), "0", false);
        }
    }
}
