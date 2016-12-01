//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerConnection.java
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
import java.sql.SQLException;
import java.util.UUID;

/**
*
* This interface is implemented by SQLServerConnection Class. 
*/
public interface ISQLServerConnection extends java.sql.Connection
{
    // Transaction types.
	// TRANSACTION_SNAPSHOT corresponds to -> SET TRANSACTION ISOLATION LEVEL SNAPSHOT
	public final static int TRANSACTION_SNAPSHOT = 0x1000;
	
	/**
	 * Gets the connection ID of the most recent connection attempt, regardless of whether the attempt succeeded or failed.
	 * @return 16-byte GUID representing the connection ID of the most recent connection attempt. Or, NULL if there is a 
	 * failure after the connection request is initiated and the pre-login handshake.
	 * @throws SQLException If any errors occur.
	 */
	public UUID getClientConnectionId() throws SQLException;
}

