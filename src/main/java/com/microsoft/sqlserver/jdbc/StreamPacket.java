/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Common interface for all TDS packet types
 *
 */
abstract class StreamPacket {
    int packetType;

    final int getTokenType() {
        return packetType;
    }

    StreamPacket() {
        this.packetType = 0;
    }

    StreamPacket(int packetType) {
        this.packetType = packetType;
    }

    abstract void setFromTDS(TDSReader tdsReader) throws SQLServerException;
}
