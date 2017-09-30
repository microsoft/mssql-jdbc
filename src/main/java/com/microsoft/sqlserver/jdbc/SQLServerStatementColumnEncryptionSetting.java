/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Specifies how data will be sent and received when reading and writing encrypted columns. Depending on your specific query, performance impact may
 * be reduced by bypassing the Always Encrypted driver processing when non-encrypted columns are being used. Note that these settings cannot be used
 * to bypass encryption and gain access to plaintext data.
 */
public enum SQLServerStatementColumnEncryptionSetting {
    /**
     * if "Column Encryption Setting=Enabled" in the connection string, use Enabled. Otherwise, maps to Disabled.
     */
    UseConnectionSetting,

    /**
     * Enables TCE for the command. Overrides the connection level setting for this command.
     */
    Enabled,

    /**
     * Parameters will not be encrypted, only the ResultSet will be decrypted. This is an optimization for queries that do not pass any encrypted
     * input parameters. Overrides the connection level setting for this command.
     */
    ResultSetOnly,

    /**
     * Disables TCE for the command.Overrides the connection level setting for this command.
     */
    Disabled,
}
