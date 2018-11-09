/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.concurrent.atomic.AtomicInteger;


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
            int sessionStateLength = (int) tdsReader.readUnsignedByte();
            bytesRead += 2;
            if (sessionStateLength >= 0xFF) {
                sessionStateLength = (int) tdsReader.readUnsignedInt(); // xFF - xFFFF
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
    private int sequenceNumber;
    private int dataLengh;
    private byte[] data;

    boolean isSequenceNumberGreater(int sequenceNumberToBeCompared) {
        // Illustration using 8 bit number

        // Initial assignment takes care of following scenarios:
        // toBeCompared= 2 benchmark = 1 (both positive) ..true
        // toBeCompared=-1(255) benchmark = -2(254) (both negative) ..true
        // toBeCompared=-1(255) benchmark = 1 ..true
        // toBeCompared=-1(255) benchmark = 0 ..true
        boolean greater = true;

        if (sequenceNumberToBeCompared > sequenceNumber) {
            // Following if condition takes care of these scenarios:
            // toBeCompared = 0 benchmark = -1(255)
            // toBeCompared = 1 benchmark = -1(255)
            if ((sequenceNumberToBeCompared >= 0) && (sequenceNumber < 0))
                greater = false;
        }
        // This else takes care of these scenarios where result is false:
        // toBeCompared= 1 benchmark = 2 (both positive) ..false
        // toBeCompared=-2(254) benchmark = -1(255) (both negative) ..false
        else
        // Following if condition to not set return to false for these scenarios:
        // toBeCompared=-1(255) benchmark = 1 ..true
        // toBeCompared=-1(255) benchmark = 0 ..true
        if ((sequenceNumberToBeCompared > 0) || (sequenceNumber < 0))
            greater = false;

        return greater;
    }

    boolean isRecoverable() {
        return isRecoverable;
    }

    void setRecoverable(boolean isRecoverable) {
        this.isRecoverable = isRecoverable;
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
    static final long MASTER_RECOVERY_DISABLE_SEQ_NUMBER = 0XFFFFFFFF;
    private boolean masterRecoveryDisabled;
    private byte[][] sessionStateInitial;
    private SessionStateValue sessionStateDelta[];
    private AtomicInteger unRecoverableSessionStateCount = new AtomicInteger(0);

    SessionStateTable() {
        this.sessionStateDelta = new SessionStateValue[SESSION_STATE_ID_MAX];
        this.sessionStateInitial = new byte[SessionStateTable.SESSION_STATE_ID_MAX][];
    }

    void updateSessionState(TDSReader tdsReader, short sessionStateId, int sessionStateLength, int sequenceNumber,
            boolean fRecoverable) throws SQLServerException {
        sessionStateDelta[sessionStateId].setSequenceNumber(sequenceNumber);
        sessionStateDelta[sessionStateId].setDataLengh(sessionStateLength);

        if ((sessionStateDelta[sessionStateId].getData() == null)
                || (sessionStateDelta[sessionStateId].getData().length < sessionStateLength)) {
            sessionStateDelta[sessionStateId].setData(new byte[sessionStateLength]);

            // First time state update and value is not recoverable, hence count is incremented.
            if (!fRecoverable) {
                unRecoverableSessionStateCount.incrementAndGet();
            }
        } else {
            int count;
            // @TODO Where is count supposed to be used?
            // Not a first time state update hence if only there is a transition in state do we update the count.
            if (fRecoverable != sessionStateDelta[sessionStateId].isRecoverable()) {
                count = fRecoverable ? unRecoverableSessionStateCount.decrementAndGet()
                                     : unRecoverableSessionStateCount.incrementAndGet();
            }
        }
        tdsReader.readBytes(sessionStateDelta[sessionStateId].getData(), 0, sessionStateLength);
        sessionStateDelta[sessionStateId].setRecoverable(fRecoverable);
    }

    boolean isMasterRecoveryDisabled() {
        return masterRecoveryDisabled;
    }

    void setMasterRecoveryDisabled(boolean masterRecoveryDisabled) {
        this.masterRecoveryDisabled = masterRecoveryDisabled;
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

final class ReconnectThread extends Thread {
    private SQLServerConnection con = null;
    private SQLServerException eReceived = null;
    
    private volatile boolean stopRequested = false;
    private int connectRetryCount = 0;

    /*
     * This class is only meant to be used by a Connection object to reconnect in the background.
     * Don't allow default instantiation as it doesn't make sense.
     */
    private ReconnectThread() {};

    ReconnectThread(SQLServerConnection sqlC) {
        this.con = sqlC;
    }

    private void reset() {
        connectRetryCount = con.getRetryCount();
        eReceived = null;
        stopRequested = false;
    }
    
    public void run() {
        reset();
        boolean keepRetrying = true;
        
        while ((connectRetryCount != 0) && (!stopRequested) && keepRetrying) {
            try {
                eReceived = null;
                con.connect(null, con.getPooledConnectionParent());
                keepRetrying = false;
            }
            catch (SQLServerException e) {
                if (!stopRequested) {
                    eReceived = e;
                    if (con.isFatalError(e)) {
                        keepRetrying = false;
                    }
                    else {
                        try {
                            if (connectRetryCount > 1)
                                Thread.sleep(con.getRetryInterval() * 1000);
                        }
                        catch (InterruptedException e1) {
                            this.eReceived = new SQLServerException(e1.getMessage(), null, DriverError.NOT_SET, null);
                            keepRetrying = false;
                        }
                    }
                }
            }
            finally {
                connectRetryCount--;
            }
        }

        if ((connectRetryCount == 0) && (keepRetrying)) {
            eReceived = new SQLServerException(SQLServerException.getErrString("R_crClientAllRecoveryAttemptsFailed"), eReceived);
        }

        return;
    }
    
    void stop(boolean blocking) {
        stopRequested = true;
        if (blocking && this.isAlive()) {
            while (this.getState() != State.TERMINATED) {
                //wait until thread terminates
            }
        }
    }

    /* Run method can not be implemented to return an exception hence statement execution thread that called
     * reconnection will get exception
     * through this function as soon as reconnection thread execution is over.*/
    SQLServerException getException() {
        return eReceived;
    }
}
