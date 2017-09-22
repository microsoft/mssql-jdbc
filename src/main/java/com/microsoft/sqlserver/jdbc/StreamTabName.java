/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * StreamTabName interprets the data stream from a TABNAME TDS token
 *
 */

final class StreamTabName extends StreamPacket {
    private TDSReader tdsReader;
    private TDSReaderMark tableNamesMark;

    StreamTabName() {
        super(TDS.TDS_TABNAME);
    }

    void setFromTDS(TDSReader tdsReader) throws SQLServerException {
        if (TDS.TDS_TABNAME != tdsReader.readUnsignedByte())
            assert false : "Not a TABNAME token";

        this.tdsReader = tdsReader;
        int tokenLength = tdsReader.readUnsignedShort();
        tableNamesMark = tdsReader.mark();
        tdsReader.skip(tokenLength);
    }

    void applyTo(Column[] columns,
            int numTables) throws SQLServerException {
        TDSReaderMark currentMark = tdsReader.mark();
        tdsReader.reset(tableNamesMark);

        // Read in all of the multi-part table names. The number of table
        // names to expect is determined in advance. It is computed as a side
        // effect of processing the COLINFO token that preceeds this TABNAME token.
        SQLIdentifier[] tableNames = new SQLIdentifier[numTables];
        for (int i = 0; i < numTables; i++)
            tableNames[i] = tdsReader.readSQLIdentifier();

        // Apply the table names to their appropriate columns
        for (Column col : columns) {
            if (col.getTableNum() > 0)
                col.setTableName(tableNames[col.getTableNum() - 1]);
        }

        tdsReader.reset(currentMark);
    }
}
