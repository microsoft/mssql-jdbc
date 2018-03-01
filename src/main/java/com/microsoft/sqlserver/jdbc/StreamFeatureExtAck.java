/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;

/**
 * StreamLoginExtAck represents a TDS login feature extension ack.
 * 
 */

final class StreamFeatureExtAck extends StreamPacket {
    FedAuthOptions fedAuth = null;
    FeatureAckOptSessionRecovery sessionRecovery = null;
    boolean serverSupportsColumnEncryption = false;
    boolean feature_ext_terminator = false;
    boolean invalid_token = false;

    StreamFeatureExtAck() {
        super(TDS.TDS_FEATURE_EXTENSION_ACK);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_FEATURE_EXTENSION_ACK != tdsReader.readUnsignedByte())
            assert false : "FeatureExtAck not received when expected.";

        boolean moreFeatureExtension = true;
        while (moreFeatureExtension) {
            byte token = (byte) tdsReader.readUnsignedByte();
            switch (token) {
                case FeatureExt.SESSION_RECOVERY: // session recovery
                    sessionRecovery = new FeatureAckOptSessionRecovery();
                    sessionRecovery.featureAckDataLen = tdsReader.readUnsignedInt();
                    parseInitialAllSessionStateData(tdsReader, sessionRecovery.sessionStateInitial);
                    break;

                case FeatureExt.FEDAUTH: {
                    int dataLen;
                    dataLen = tdsReader.readInt();
                    byte[] data = new byte[dataLen];
                    if (dataLen > 0) {
                        tdsReader.readBytes(data, 0, dataLen);
                    }

                    if (!fedAuth.isRequested) {
                        MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnrequestedFeatureAckReceived"));
                        Object[] msgArgs = {token}; // featureId can be replaced with token
                        throw new SQLServerException(form.format(msgArgs), null);
                    }

                    // _fedAuthFeatureExtensionData must not be null when _federatedAuthenticatonRequested == true
                    assert null != fedAuth.fedAuthFeatureExtensionData;

                    switch (fedAuth.fedAuthFeatureExtensionData.libraryType) {
                        case TDS.TDS_FEDAUTH_LIBRARY_ADAL:
                        case TDS.TDS_FEDAUTH_LIBRARY_SECURITYTOKEN:
                            // The server shouldn't have sent any additional data with the ack (like a nonce)
                            if (0 != data.length) {
                                throw new SQLServerException(SQLServerException.getErrString("R_FedAuthFeatureAckContainsExtraData"), null);
                            }
                            break;

                        default:
                            assert false;   // Unknown _fedAuthLibrary type
                            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_FedAuthFeatureAckUnknownLibraryType"));
                            Object[] msgArgs = {fedAuth.fedAuthFeatureExtensionData.libraryType};
                            throw new SQLServerException(form.format(msgArgs), null);
                    }
                    fedAuth.isAcknowledged = true;

                    break;
                }
                case FeatureExt.ALWAYS_ENCRYPTED: {
                    int dataLen;
                    dataLen = tdsReader.readInt();
                    byte[] data = new byte[dataLen];
                    if (dataLen > 0) {
                        tdsReader.readBytes(data, 0, dataLen);
                    }

                    if (1 > data.length) {
                        throw new SQLServerException(SQLServerException.getErrString("R_InvalidAEVersionNumber"), null);
                    }

                    byte supportedTceVersion = data[0];
                    if (0 == supportedTceVersion || supportedTceVersion > TDS.MAX_SUPPORTED_TCE_VERSION) {
                        throw new SQLServerException(SQLServerException.getErrString("R_InvalidAEVersionNumber"), null);
                    }

                    assert supportedTceVersion == TDS.MAX_SUPPORTED_TCE_VERSION; // Client support TCE version 1
                    serverSupportsColumnEncryption = true;
                    break;
                }
                case FeatureExt.FEATURE_EXT_TERMINATOR: // End of feature extension ack
                    feature_ext_terminator = true;
                    moreFeatureExtension = false;
                    break;
                default:
                    invalid_token = true;
                    break;
            }
        }
    }

    void parseInitialAllSessionStateData(TDSReader tdsReader,
            byte[][] sessionStateInitial) throws SQLServerException {
        long bytesRead = 0;
        // This is AllSessionStateData (no database, no language)
        // Should contain StateId, StateLen, StateValue
        while (bytesRead < sessionRecovery.featureAckDataLen) {
            short sessionStateId = (short) tdsReader.readUnsignedByte(); // unsigned byte
            short sessionStateLength = (short) tdsReader.readUnsignedByte(); // unsigned byte
            bytesRead += 2;
            if (sessionStateLength == 0xFF) {
                sessionStateLength = tdsReader.readShort();
                bytesRead += 2;
            }
            sessionStateInitial[sessionStateId] = new byte[sessionStateLength];
            tdsReader.readBytes(sessionStateInitial[sessionStateId], 0, sessionStateLength);
            bytesRead += sessionStateLength;
        }
    }
}

class FeatureAckOptSessionRecovery extends FeatureAckOption {
    byte[][] sessionStateInitial = null;

    FeatureAckOptSessionRecovery() {
        super();
        featureId = FeatureExt.SESSION_RECOVERY;
        sessionStateInitial = new byte[SessionStateTable.SESSION_STATE_ID_MAX][];
    }
}

class FeatureAckOption {
    int featureId;
    long featureAckDataLen; // this value is 4 bytes however it should be unsigned hence using 8 byte long.

    FeatureAckOption() {
        featureId = 0;
        featureAckDataLen = 0;
    }
}

class FedAuthOptions {
    boolean requiredByUser = false;
    boolean requiredPreLoginResponse = false;
    boolean isAcknowledged = false;
    boolean isRequested = false;
    // Keep this distinct from isRequested, since some fedauth library types may not need more info
    boolean infoRequested = false; 
    FederatedAuthenticationFeatureExtensionData fedAuthFeatureExtensionData = null;
}