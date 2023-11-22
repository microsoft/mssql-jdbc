/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.lang.Thread.State;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Level;

import static com.microsoft.sqlserver.jdbc.SQLServerConnection.loggerResiliency;


class IdleConnectionResiliency {
    private static final java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.IdleConnectionResiliency");
    private boolean connectionRecoveryNegotiated;
    private int connectRetryCount;
    private SQLServerConnection connection;
    private SessionStateTable sessionStateTable;
    private ReconnectThread reconnectThread;
    private AtomicInteger unprocessedResponseCount = new AtomicInteger();
    private boolean connectionRecoveryPossible;
    private SQLServerException reconnectErrorReceived = null;

    /*
     * Variables needed to perform a reconnect, these are not necessarily determined from just the connection string
     */
    private String loginInstanceValue;
    private int loginNPort;
    private FailoverInfo loginFailoverInfo;
    private int loginLoginTimeoutSeconds;

    IdleConnectionResiliency(SQLServerConnection connection) {
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

    boolean isReconnectRunning() {
        return reconnectThread != null && (reconnectThread.getState() != State.TERMINATED);
    }

    SessionStateTable getSessionStateTable() {
        return sessionStateTable;
    }

    void setSessionStateTable(SessionStateTable sessionStateTable) {
        this.sessionStateTable = sessionStateTable;
    }

    boolean isConnectionRecoveryPossible() {
        return connectionRecoveryPossible;
    }

    void setConnectionRecoveryPossible(boolean connectionRecoveryPossible) {
        this.connectionRecoveryPossible = connectionRecoveryPossible;
    }

    int getUnprocessedResponseCount() {
        return unprocessedResponseCount.get();
    }

    void resetUnprocessedResponseCount() {
        this.unprocessedResponseCount.set(0);
    }

    void parseInitialSessionStateData(byte[] data, byte[][] sessionStateInitial) {
        int bytesRead = 0;

        // Contains StateId, StateLen, StateValue
        while (bytesRead < data.length) {
            short sessionStateId = (short) (data[bytesRead] & 0xFF);
            bytesRead++;
            int sessionStateLength;
            int byteLength = data[bytesRead] & 0xFF;
            bytesRead++;
            if (byteLength == 0xFF) {
                sessionStateLength = (int) (Util.readInt(data, bytesRead) & 0xFFFFFFFFL);
                bytesRead += 4;
            } else {
                sessionStateLength = byteLength;
            }
            sessionStateInitial[sessionStateId] = new byte[sessionStateLength];
            System.arraycopy(data, bytesRead, sessionStateInitial[sessionStateId], 0, sessionStateLength);
            bytesRead += sessionStateLength;
        }
    }

    void incrementUnprocessedResponseCount() {
        if ((connection.getRetryCount() > 0 && !isReconnectRunning())
                && (unprocessedResponseCount.incrementAndGet() < 0)) {
            /*
             * When this number rolls over, connection recovery is disabled for the rest of the life of the
             * connection.
             */
            if (loggerExternal.isLoggable(Level.FINER)) {
                loggerExternal.finer("unprocessedResponseCount < 0 on increment. Disabling connection resiliency.");
            }

            setConnectionRecoveryPossible(false);
        }
    }

    void decrementUnprocessedResponseCount() {
        if ((connection.getRetryCount() > 0 && !isReconnectRunning())
                && (unprocessedResponseCount.decrementAndGet() < 0)) {
            /*
             * When this number rolls over, connection recovery is disabled for the rest of the life of the
             * connection.
             */
            if (loggerExternal.isLoggable(Level.FINER)) {
                loggerExternal.finer("unprocessedResponseCount < 0 on decrement. Disabling connection resiliency.");
            }

            setConnectionRecoveryPossible(false);
        }
    }

    void setLoginParameters(String instanceValue, int nPort, FailoverInfo fo, int loginTimeoutSeconds) {
        this.loginInstanceValue = instanceValue;
        this.loginNPort = nPort;
        this.loginFailoverInfo = fo;
        this.loginLoginTimeoutSeconds = loginTimeoutSeconds;
    }

    String getInstanceValue() {
        return loginInstanceValue;
    }

    int getNPort() {
        return loginNPort;
    }

    FailoverInfo getFailoverInfo() {
        return loginFailoverInfo;
    }

    int getLoginTimeoutSeconds() {
        return loginLoginTimeoutSeconds;
    }

    void reconnect(TDSCommand cmd) throws InterruptedException {
        reconnectErrorReceived = null;
        connectRetryCount = this.connection.getRetryCount();
        if (connectRetryCount > 0) {
            reconnectThread = new ReconnectThread(this.connection, cmd);
        }
        reconnectThread.start();
        reconnectThread.join();
        reconnectErrorReceived = reconnectThread.getException();
        // Remove reference so GC can clean it up
        reconnectThread = null;
    }

    SQLServerException getReconnectException() {
        return reconnectErrorReceived;
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
    private SessionStateValue[] sessionStateDelta;
    private AtomicInteger unRecoverableSessionStateCount = new AtomicInteger(0);
    private String originalCatalog;
    private String originalLanguage;
    private SQLCollation originalCollation;
    private byte originalNegotiatedEncryptionLevel = TDS.ENCRYPT_INVALID;
    private boolean resetCalled = false;

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
            // Not a first time state update hence if only there is a transition in state do we update the count.
            if (fRecoverable != sessionStateDelta[sessionStateId].isRecoverable()) {
                if (fRecoverable)
                    unRecoverableSessionStateCount.decrementAndGet();
                else
                    unRecoverableSessionStateCount.incrementAndGet();
            }
        }
        tdsReader.readBytes(sessionStateDelta[sessionStateId].getData(), 0, sessionStateLength);
        sessionStateDelta[sessionStateId].setRecoverable(fRecoverable);
    }

    /**
     * @return length of initial session state data.
     */
    int getInitialLength() {
        int length = 0;
        for (int i = 0; i < SESSION_STATE_ID_MAX; i++) {
            if (sessionStateInitial[i] != null) {
                length += (1/* state id */ + (sessionStateInitial[i].length < 0xFF ? 1 : 5)/* Data length */
                        + sessionStateInitial[i].length);
            }
        }
        return length;
    }

    /**
     * @return length of delta session state data.
     */
    int getDeltaLength() {
        int length = 0;
        for (int i = 0; i < SESSION_STATE_ID_MAX; i++) {
            if (sessionStateDelta[i] != null && sessionStateDelta[i].getData() != null) {
                length += (1/* state id */ + (sessionStateDelta[i].getDataLength() < 0xFF ? 1 : 5)/* Data length */
                        + sessionStateDelta[i].getDataLength());
            }
        }
        return length;
    }

    boolean isSessionRecoverable() {
        return (!isMasterRecoveryDisabled() && (0 == unRecoverableSessionStateCount.get()));
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

    String getOriginalCatalog() {
        return originalCatalog;
    }

    void setOriginalCatalog(String catalog) {
        this.originalCatalog = catalog;
    }

    String getOriginalLanguage() {
        return originalLanguage;
    }

    void setOriginalLanguage(String language) {
        this.originalLanguage = language;
    }

    SQLCollation getOriginalCollation() {
        return originalCollation;
    }

    void setOriginalCollation(SQLCollation collation) {
        this.originalCollation = collation;
    }

    byte getOriginalNegotiatedEncryptionLevel() {
        return originalNegotiatedEncryptionLevel;
    }

    void setOriginalNegotiatedEncryptionLevel(byte originalNegotiatedEncryptionLevel) {
        this.originalNegotiatedEncryptionLevel = originalNegotiatedEncryptionLevel;
    }

    boolean spResetCalled() {
        return resetCalled;
    }

    void setspResetCalled(boolean status) {
        this.resetCalled = status;
    }

    public void reset() {
        resetCalled = true;
        sessionStateDelta = new SessionStateValue[SESSION_STATE_ID_MAX];
        unRecoverableSessionStateCount = new AtomicInteger(0);
    }
}


final class ReconnectThread extends Thread {
    static final java.util.logging.Logger loggerExternal = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ReconnectThread");
    private SQLServerConnection con = null;
    private SQLServerException eReceived = null;
    private TDSCommand command = null;

    private volatile boolean stopRequested = false;
    private int connectRetryCount = 0;

    /*
     * This class is only meant to be used by a Connection object to reconnect in the background. Don't allow default
     * instantiation as it doesn't make sense.
     */
    @SuppressWarnings("unused")
    private ReconnectThread() {}

    ReconnectThread(SQLServerConnection sqlC, TDSCommand cmd) {
        this.con = sqlC;
        this.command = cmd;
        connectRetryCount = con.getRetryCount();
        eReceived = null;
        stopRequested = false;
        if (loggerResiliency.isLoggable(Level.FINER)) {
            loggerResiliency.finer("Idle connection resiliency - ReconnectThread initialized. Connection retry count = "
                    + connectRetryCount + "; Command = " + cmd.toString());
        }

    }

    @Override
    public void run() {
        if (loggerResiliency.isLoggable(Level.FINER)) {
            loggerResiliency
                    .finer("Idle connection resiliency - starting ReconnectThread for command: " + command.toString());
        }
        boolean interruptsEnabled = command.getInterruptsEnabled();
        /*
         * All TDSCommands are not interruptible before execution, and all the commands passed to here won't have been
         * executed. We need to be able to interrupt these commands so the TimeoutPoller can tell us when a query has
         * timed out.
         */
        command.setInterruptsEnabled(true);
        command.attachThread(this);

        // We need a reference to the SharedTimer outside of the context of the connection
        SharedTimer timer = null;
        ScheduledFuture<?> timeout = null;

        if (command.getQueryTimeoutSeconds() > 0) {
            timer = SharedTimer.getTimer();
            timeout = timer.schedule(new TDSTimeoutTask(command, null), command.getQueryTimeoutSeconds());
        }

        boolean keepRetrying = true;
        long connectRetryInterval = TimeUnit.SECONDS.toMillis(con.getRetryInterval());

        while ((connectRetryCount >= 0) && (!stopRequested) && keepRetrying) {
            if (loggerResiliency.isLoggable(Level.FINER)) {
                loggerResiliency.finer("Idle connection resiliency - running reconnect for command: "
                        + command.toString() + " ; connectRetryCount = " + connectRetryCount);
            }

            try {
                eReceived = null;
                con.connect(null, con.getPooledConnectionParent());
                keepRetrying = false;

                if (loggerResiliency.isLoggable(Level.FINE)) {
                    loggerResiliency
                            .fine("Idle connection resiliency - reconnect attempt succeeded ; connectRetryCount = "
                                    + connectRetryCount);
                }

            } catch (SQLServerException e) {

                if (loggerResiliency.isLoggable(Level.FINE)) {
                    loggerResiliency.fine("Idle connection resiliency - reconnect attempt failed ; connectRetryCount = "
                            + connectRetryCount);
                }

                if (!stopRequested) {
                    eReceived = e;
                    if (con.isFatalError(e)) {

                        if (loggerResiliency.isLoggable(Level.FINER)) {
                            loggerResiliency.finer("Idle connection resiliency - reconnect for command: "
                                    + command.toString() + " encountered fatal error: " + e.getMessage()
                                    + " - stopping reconnect attempt.");
                        }

                        keepRetrying = false;
                    } else {
                        try {
                            if (connectRetryCount > 1) {
                                Thread.sleep(connectRetryInterval);
                            }
                        } catch (InterruptedException ie) {
                            if (loggerResiliency.isLoggable(Level.FINER)) {
                                loggerResiliency.finer("Idle connection resiliency - query timed out for command: "
                                        + command.toString() + ". Stopping reconnect attempt.");
                            }

                            // re-interrupt thread
                            Thread.currentThread().interrupt();

                            this.eReceived = new SQLServerException(SQLServerException.getErrString("R_queryTimedOut"),
                                    SQLState.STATEMENT_CANCELED, DriverError.NOT_SET, null);
                            keepRetrying = false;
                        }
                    }
                }
            } finally {
                connectRetryCount--;
                try {
                    command.checkForInterrupt();
                } catch (SQLServerException e) {
                    if (loggerResiliency.isLoggable(Level.FINER)) {
                        loggerResiliency.finer("Idle connection resiliency - timeout occurred on reconnect: "
                                + command.toString() + ". Stopping reconnect attempt.");
                    }
                    // Interrupted, timeout occurred. Stop retrying.
                    keepRetrying = false;
                    eReceived = e;
                }
            }
        }

        if ((connectRetryCount <= 0) && (keepRetrying)) {
            eReceived = new SQLServerException(SQLServerException.getErrString("R_crClientAllRecoveryAttemptsFailed"),
                    eReceived);
        }

        command.setInterruptsEnabled(interruptsEnabled);

        if (loggerResiliency.isLoggable(Level.FINER)) {
            loggerResiliency
                    .finer("Idle connection resiliency - ReconnectThread exiting for command: " + command.toString());
        }

        if (timeout != null) {
            timeout.cancel(false);
            timeout = null;
        }

        if (timer != null) {
            timer.removeRef();
            timer = null;
        }
    }

    void stop(boolean blocking) {
        if (loggerResiliency.isLoggable(Level.FINER)) {
            loggerResiliency.finer("ReconnectThread stop requested for command: " + command.toString());
        }
        stopRequested = true;
        if (blocking && this.isAlive()) {
            while (this.getState() != State.TERMINATED) {
                // wait until thread terminates
            }
        }
    }

    /*
     * Run method can not be implemented to return an exception hence statement execution thread that called
     * reconnection will get exception through this function as soon as reconnection thread execution is over.
     */
    SQLServerException getException() {
        return eReceived;
    }
}
