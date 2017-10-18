/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamColInfo interprets the data stream from a COLINFO TDS token
 */

final class StreamColInfo extends StreamPacket {
    private TDSReader tdsReader;
    private TDSReaderMark colInfoMark;

    StreamColInfo() {
        super(TDS.TDS_COLINFO);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_COLINFO != tdsReader.readUnsignedByte())
            assert false : "Not a COLINFO token";

        this.tdsReader = tdsReader;
        int tokenLength = tdsReader.readUnsignedShort();
        colInfoMark = tdsReader.mark();
        tdsReader.skip(tokenLength);
    }

    int applyTo(Column[] columns) throws SQLServerException {
        int numTables = 0;

        // Read and apply the column info for each column
        TDSReaderMark currentMark = tdsReader.mark();
        tdsReader.reset(colInfoMark);
        for (Column col : columns) {
            // Ignore the column number, per TDS spec.
            // Column info is returned for each column, ascending by column index,
            // so iterating through the column info is sufficient.
            tdsReader.readUnsignedByte();

            // Set the column's table number, keeping track of the maximum table number
            // representing the number of tables encountered.
            col.setTableNum(tdsReader.readUnsignedByte());
            if (col.getTableNum() > numTables)
                numTables = col.getTableNum();

            // Set the other column info
            col.setInfoStatus(tdsReader.readUnsignedByte());
            if (col.hasDifferentName())
                col.setBaseColumnName(tdsReader.readUnicodeString(tdsReader.readUnsignedByte()));
        }

        tdsReader.reset(currentMark);
        return numTables;
    }
}
