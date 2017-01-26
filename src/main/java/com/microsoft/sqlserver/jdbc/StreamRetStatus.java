/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamRetStatus represents a TDS return status.
 *
 */
final class StreamRetStatus extends StreamPacket {
    /** the returned status */
    private int status;

    final int getStatus() {
        return status;
    }

    StreamRetStatus() {
        super(TDS.TDS_RET_STAT);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_RET_STAT != tdsReader.readUnsignedByte())
            assert false;
        status = tdsReader.readInt();
    }
}
