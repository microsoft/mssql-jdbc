/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Enum representing different performance activities.
 */
public enum PerformanceActivity {
    CONNECTION("Connection"),
    TOKEN_ACQUISITION("Token acquisition"),
    LOGIN("Login"),
    PRELOGIN("Prelogin"),

    // Statement-level activities
    STATEMENT_CREATION_TO_FIRST_PACKET("Statement creation to first packet"),
    /*
     * Called endCreationToFirstPacketTracking() in:
     * doExecuteStatement() - for regular statements
     * doExecuteCursored() - for cursor-based execution
     * doExecuteStatementBatch() - for batch execution
     * SQLServerPreparedStatement.java
     * Called endCreationToFirstPacketTracking() in doExecutePreparedStatement() just before sending the packet
     * Also restarts tracking on retry with startCreationToFirstPacketTracking()
     * How it works:
     * Start: When executeStatement() is called, we create a PerformanceLog.Scope
     * that starts timing
     * End: Just before startResponse() is called (which sends the TDS packet to
     * server), we close the scope to record the duration
     * The duration is published via the PerformanceLogCallback (if registered)
     * and/or logged via Java logging
     * 
     * Start the response - this sends the packet and reads response
            ensureExecuteResultsReader(execCmd.startResponse(isResponseBufferingAdaptive));
     */

    STATEMENT_FIRST_PACKET_TO_FIRST_RESPONSE("First packet to first response"),
    /*
     * startResponse() {
     * endMessage() <-- packet sent to server
     * readPacket() <-- wait for & read response
     * }
     * 
     * It measures the time from when we initiate sending the packet until 
     * we have received the first response from the server.
     */

    STATEMENT_PREPARE("Statement prepare");
    /*
     * Tracks the time to prepare a statement using sp_prepare.
     * This is ONLY triggered when using prepareMethod=prepare (not the default sp_prepexec).
     * 
     * Measured in doPrep() method:
     * - Start: Before startResponse() sends sp_prepare to server
     * - End: After processResponse() receives the prepared handle
     * 
     * Note: For sp_prepexec (default), prepare+execute are combined and cannot be separated.
     */

    private final String activity;

    PerformanceActivity(String activity) {
        this.activity = activity;
    }

    public String activity() {
        return activity;
    }

    @Override
    public String toString() {
        return activity;
    }
}
