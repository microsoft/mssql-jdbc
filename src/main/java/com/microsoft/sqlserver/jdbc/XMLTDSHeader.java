/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
* XMLTDSHeader is helper class used to read and write the XML TDS header from a TDS stream.
* 
* Typical XML headers ->
* 
* XML with schema.
*
* F1 
* 01 <- SCHEMA_PRESENT=1
* 03|54 00 44 00 53 00 <- DBNAME (1 byte length in UNICODE chars)
* 03|64 00 62 00 6F 00 <- OWNING_SCHEMA	(1 byte length in UNICODE chars)
* 09 00|53 00 68 00 69 00 70 00 4F 00 72 00 64 00 65 00 72 00  <- XML_SCHEMA_COLLECTION (2 byte length in UNICODE chars)
* 
* XML without any schema (this is common as well).
*
* F1 
* 00 <- SCHEMA_PRESENT=0
* 
*/

final class XMLTDSHeader {
    private final String databaseName;		// Database name where XML schema resides.
    private final String owningSchema;		// Owner of XML schema (like dbo for example).
    private final String xmlSchemaCollection;	// Name of XML schema collection.

    XMLTDSHeader(TDSReader tdsReader) throws SQLServerException {
        // Check schema present byte.
        if (0 != tdsReader.readUnsignedByte()) {
            // Ok, we have a schema present, process it.
            databaseName = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
            owningSchema = tdsReader.readUnicodeString(tdsReader.readUnsignedByte());
            xmlSchemaCollection = tdsReader.readUnicodeString(tdsReader.readUnsignedShort());
        }
        else {
            xmlSchemaCollection = null;
            owningSchema = null;
            databaseName = null;
        }
    }
}
