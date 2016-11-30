//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerStatementColumnEncryptionSetting.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;

/**
 * Specifies how data will be sent and received when reading and writing encrypted columns. Depending on your specific query, performance impact
 * may be reduced by bypassing the Always Encrypted driver processing when non-encrypted columns are being used. Note that these settings cannot
 * be used to bypass encryption and gain access to plaintext data.
 */
public enum SQLServerStatementColumnEncryptionSetting {
		/*
		 * if "Column Encryption Setting=Enabled" in the connection string, use Enabled. Otherwise, maps to Disabled. 
		 */
        UseConnectionSetting,

        /*
         * Enables TCE for the command. Overrides the connection level setting for this command. 
         */
        Enabled,

        /*
         * Parameters will not be encrypted, only the ResultSet will be decrypted. This is an optimization for queries that do not pass any encrypted input parameters.
         * Overrides the connection level setting for this command.
         */
        ResultSetOnly,

        /*
         * Disables TCE for the command.Overrides the connection level setting for this command.
         */
        Disabled,
}

