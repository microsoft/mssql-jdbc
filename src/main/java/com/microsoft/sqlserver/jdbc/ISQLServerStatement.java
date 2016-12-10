//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerStatement.java
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


public interface ISQLServerStatement extends java.sql.Statement
{
	/**
     * Sets the response buffering mode for this SQLServerStatement object to case-insensitive String full or adaptive.
     * <p>
     * Response buffering controls the driver's buffering of responses from SQL Server.
     * <p>
     * Possible values are:
     * <p>
     * "full" - Fully buffer the response at execution time.
     * <p>
     * "adaptive" - Data Pipe adaptive buffering
     * @param value A String that contains the response buffering mode. The valid mode can be one of the following case-insensitive Strings: full or adaptive.
     * @throws SQLServerException If there are any errors in setting the response buffering mode.
     */
    void setResponseBuffering(String value) throws SQLServerException;
	
	/**
     * Retrieves the response buffering mode for this SQLServerStatement object.
     * @return A String that contains a lower-case full or adaptive.
     * @throws SQLServerException If there are any errors in retrieving the response buffering mode.
     */
    String getResponseBuffering() throws SQLServerException;
}
