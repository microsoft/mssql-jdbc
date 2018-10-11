/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

class SessionRecoveryFeature {
    private boolean connectionRecoveryNegotiated;
    private int connectRetryCount;
    private SQLServerConnection connection;
    private SessionStateTable sessionStateTable;

    SessionRecoveryFeature(SQLServerConnection connection) {
        this.connection = connection;
    }

    boolean isConnectionRecoveryNegotiated() {
        return connectionRecoveryNegotiated;
    }

    void setConnectionRecoveryNegotiated(boolean connectionRecoveryNegotiated) {
        this.connectionRecoveryNegotiated = connectionRecoveryNegotiated;
    }

    int getConnectRetryCount() {
        return connectRetryCount;
    }

    void setConnectRetryCount(int connectRetryCount) {
        this.connectRetryCount = connectRetryCount;
    }

    SQLServerConnection getConnection() {
        return connection;
    }

    void setConnection(SQLServerConnection connection) {
        this.connection = connection;
    }

    SessionStateTable getSessionStateTable() {
        return sessionStateTable;
    }

    void setSessionStateTable(SessionStateTable sessionStateTable) {
        this.sessionStateTable = sessionStateTable;
    }

    void parseInitialSessionStateData(TDSReader tdsReader, byte[][] sessionStateInitial) throws SQLServerException {
        int bytesRead = 0;
        int dataLength = tdsReader.readInt();
        
        // Contains StateId, StateLen, StateValue
        while (bytesRead < dataLength) {
            short sessionStateId = (short) tdsReader.readUnsignedByte();
            short sessionStateLength = (short) tdsReader.readUnsignedByte();
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


class SessionStateValue {
    private boolean isRecoverable;
    private boolean sequenceNumberUnsignedCarryover;
    private int sequenceNumber;
    private int dataLengh;
    private byte[] data;

    boolean isRecoverable() {
        return isRecoverable;
    }

    void setRecoverable(boolean isRecoverable) {
        this.isRecoverable = isRecoverable;
    }

    boolean isSequenceNumberUnsignedCarryover() {
        return sequenceNumberUnsignedCarryover;
    }

    void setSequenceNumberUnsignedCarryover(boolean sequenceNumberUnsignedCarryover) {
        this.sequenceNumberUnsignedCarryover = sequenceNumberUnsignedCarryover;
    }

    int getSequenceNumber() {
        return sequenceNumber;
    }

    void setSequenceNumber(int sequenceNumber) {
        this.sequenceNumber = sequenceNumber;
    }

    int getDataLengh() {
        return dataLengh;
    }

    void setDataLengh(int dataLengh) {
        this.dataLengh = dataLengh;
    }

    byte[] getData() {
        return data;
    }

    void setData(byte[] data) {
        this.data = data;
    }
}


class SessionStateTable {
    private static final int SESSION_STATE_ID_MAX = 256;
    private byte[][] sessionStateInitial;
    private SessionStateValue sessionStateDelta[];

    SessionStateTable() {
        this.sessionStateDelta = new SessionStateValue[SESSION_STATE_ID_MAX];
        this.sessionStateInitial = new byte[SessionStateTable.SESSION_STATE_ID_MAX][];
    }

    byte[][] getSessionStateInitial() {
        return sessionStateInitial;
    }

    void setSessionStateInitial(byte[][] sessionStateInitial) {
        this.sessionStateInitial = sessionStateInitial;
    }

    SessionStateValue[] getSessionStateDelta() {
        return sessionStateDelta;
    }

    void setSessionStateDelta(SessionStateValue[] sessionStateDelta) {
        this.sessionStateDelta = sessionStateDelta;
    }
}
