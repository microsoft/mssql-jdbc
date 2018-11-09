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
    
    long featureDataLen;

    SessionRecoveryFeature(SQLServerConnection connection) {
        this.connection = connection;
    }
    
    //refer to TDS documentation
    byte getFeatureID() {
        return 0x01;
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
    private int dataLength;
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

    int getDataLength() {
        return dataLength;
    }

    void setDataLengh(int dataLength) {
        this.dataLength = dataLength;
    }

    byte[] getData() {
        return data;
    }

    void setData(byte[] data) {
        this.data = data;
    }
}

class SessionStateTable {
    static final int SESSION_STATE_ID_MAX = 256;
    static final long MASTER_RECOVERY_DISABLE_SEQ_NUMBER = 0XFFFFFFFF;
    private boolean masterRecoveryDisabled;
    private byte[][] sessionStateInitial;
    private SessionStateValue sessionStateDelta[];
    private AtomicInteger unRecoverableSessionStateCount = new AtomicInteger(0);
    byte initialNegotiatedEncryptionLevel = TDS.ENCRYPT_INVALID;
    
    private String sOriginalCatalog;
    private SQLCollation sOriginalCollation;
    private String sOriginalLanguage;

    SessionStateTable(byte negotiatedEncryptionLevel) {
        this.sessionStateDelta = new SessionStateValue[SESSION_STATE_ID_MAX];
        this.sessionStateInitial = new byte[SessionStateTable.SESSION_STATE_ID_MAX][];
        initialNegotiatedEncryptionLevel = negotiatedEncryptionLevel;
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
    
    void setOriginalCatalog(String cat) {
        this.sOriginalCatalog = cat;
    }
    
    void setOriginalCollation(SQLCollation col) {
        this.sOriginalCollation = col;
    }
    
    void setOriginalLanguage(String lang) {
        this.sOriginalLanguage = lang;
    }
    
    String getOriginalCatalog() {
        return sOriginalCatalog;
    }
    
    SQLCollation getOriginalCollation() {
        return sOriginalCollation;
    }
    
    String getOriginalLanaguage() {
        return sOriginalLanguage;
    }

    // Length of initial session state data
    // State id
    // State length
    // State value
    long getInitialLength() {
        long length = 0; // bytes
        for (int i = 0; i < SESSION_STATE_ID_MAX; i++) {
            if (sessionStateInitial[i] != null) {
                length += (1/* state id */ + (sessionStateInitial[i].length < 0xFF ? 1 : 3)/* Data length */ + sessionStateInitial[i].length);
            }
        }
        return length;
    }    
    
    // Length of delta session state data
    // State id
    // State length
    // State value
    long getDeltaLength() {
        long length = 0; // bytes
        for (int i = 0; i < SESSION_STATE_ID_MAX; i++) {
            if (sessionStateDelta[i] != null && sessionStateDelta[i].getData() != null) {
                length += (1/* state id */ + (sessionStateDelta[i].getDataLength() < 0xFF ? 1 : 3)/* Data length */ + sessionStateDelta[i].getDataLength());
            }
        }
        return length;
    }
}

final class ReconnectThread implements Runnable {
    private SQLServerConnection con = null;
    Object reconnectStateSynchronizer = new Object();
    Object stopReconnectionSynchronizer = new Object();
    private SQLServerException eReceived = null;

    private boolean reconnecting = false;
    private volatile boolean stopRequest = false;
    private int connectRetryCount = 0;

    private ReconnectThread() {};

    ReconnectThread(SQLServerConnection sqlC) {
        this.con = sqlC;
        reset();
    }

    void reset() {
        connectRetryCount = con.getRetryCount();
        eReceived = null;
        stopRequest = false;
    }
    
    public void run() {
        reconnecting = true;

        while ((connectRetryCount != 0) && (!stopRequest) && (reconnecting == true)) {
            try {
                eReceived = null;
                con.connect(null, con.getPooledConnectionParent());  // exception caught here but should be thrown appropriately. Add exception
                                                                // variable and CheckException() API
//                if (connectionlogger.isLoggable(Level.FINER)) {
//                    connectionlogger.finer(this.toString() + "Reconnection successful.");
//                }
                reconnecting = false;
            }
            catch (SQLServerException e) {
                if (!stopRequest) {
                    eReceived = e;
                    if (isFatalError(e)) {
                        reconnecting = false;   // We don't want to retry connection if it failed because of non-retryable reasons.
                    }
                    else {
                        try {
                            synchronized (reconnectStateSynchronizer) {
                                reconnectStateSynchronizer.notifyAll(); // this will unblock thread waiting for reconnection (statement execution
                                                                        // thread).
                            }

//                            if (connectionlogger.isLoggable(Level.FINER)) {
//                                connectionlogger.finer(this.toString() + "Sleeping before next reconnection..");
//                            }
                            if (connectRetryCount > 1)
                                Thread.sleep(con.getRetryInterval() * 1000 /* milliseconds */);
                        }
                        catch (InterruptedException e1) {
                            // Exception is generated only if the thread is interrupted by another thread.
                            // Currently we don't have anything interrupting reconnection thread hence ignore this sleep exception.
//                            if (connectionlogger.isLoggable(Level.FINER)) {
//                                connectionlogger.finer(this.toString() + "Interrupt during sleep is unexpected.");
//                            }
                        }
                    }
                }
            }// connection state is set to Opened at the end of connect()
            finally {
                connectRetryCount--;
            }
        }

        if ((connectRetryCount == 0) && (reconnecting))    // reconnection could not happen while all reconnection attempts are exhausted
        {
//            if (connectionlogger.isLoggable(Level.FINER)) {
//                connectionlogger.finer(this.toString() + "Connection retry attempts exhausted.");
//            }
            eReceived = new SQLServerException(SQLServerException.getErrString("R_crClientAllRecoveryAttemptsFailed"), eReceived);
        }

        reconnecting = false;
        if (stopRequest)
            synchronized (stopReconnectionSynchronizer) {
                stopReconnectionSynchronizer.notify(); // this will unblock thread invoking reconnection stop
            }
        synchronized (reconnectStateSynchronizer) {
            reconnectStateSynchronizer.notify();    // There could at the most be only 1 thread waiting on reconnectStateSynchronizer. NotifyAll
                                                    // will unblock the thread waiting for reconnection (statement execution thread).
        }
        return;
    }

    boolean isRunning() {
        return reconnecting;
    }

    private boolean isFatalError(SQLServerException e) {
        // NOTE: If these conditions are modified, consider modification to conditions in SQLServerConnection::login()
        // and
        // Reconnect::run()
        if ((SQLServerException.LOGON_FAILED == e.getErrorCode()) // actual logon failed, i.e. bad password
                || (SQLServerException.PASSWORD_EXPIRED == e.getErrorCode()) // actual logon failed, i.e. password
                                                                             // isExpired
                || (SQLServerException.DRIVER_ERROR_INVALID_TDS == e.getDriverErrorCode()) // invalid TDS received from
                                                                                           // server
                || (SQLServerException.DRIVER_ERROR_SSL_FAILED == e.getDriverErrorCode()) // failure negotiating SSL
                || (SQLServerException.DRIVER_ERROR_INTERMITTENT_TLS_FAILED == e.getDriverErrorCode()) // failure TLS1.2
                || (SQLServerException.ERROR_SOCKET_TIMEOUT == e.getDriverErrorCode()) // socket timeout ocurred
                || (SQLServerException.DRIVER_ERROR_UNSUPPORTED_CONFIG == e.getDriverErrorCode())) // unsupported
                                                                                                   // configuration
                                                                                                   // (e.g. Sphinx,
                                                                                                   // invalid
                                                                                                   // packet size, etc.)
            return true;
        else
            return false;
    }

    Object getLock() {
        return reconnectStateSynchronizer;
    }

    void stop(boolean blocking) {
        // if (connectionlogger.isLoggable(Level.FINER)) {
        // connectionlogger.finer(this.toString() + "Reconnection stopping");
        // }
        stopRequest = true;

        if (blocking && reconnecting) {
            // If stopRequest is received while reconnecting is true, only then can we receive notify on
            // stopReconnectionObject
            try {
                synchronized (stopReconnectionSynchronizer) {
                    if (reconnecting)
                        stopReconnectionSynchronizer.wait(); // Wait only if reconnecting is still true. This is to
                                                             // avoid a race condition where
                                                             // reconnecting set to false and
                                                             // stopReconnectionSynchronizer has already notified
                                                             // even before following wait() is called.
                }
            } catch (InterruptedException e) {
                // Driver does not generate any interrupts that will generate this exception hence ignoring. This
                // exception should not break
                // current flow of execution hence catching it.
                // if (connectionlogger.isLoggable(Level.FINER)) {
                // connectionlogger.finer(this.toString() + "Interrupt in reconnection stop() is unexpected.");
                // }
            }
        }
    }

    // Run method can not be implemented to return an exception hence statement execution thread that called
    // reconnection will get exception
    // through this function as soon as reconnection thread execution is over.
    SQLServerException getException() {
        return eReceived;
    }
}
