//---------------------------------------------------------------------------------------------------------------------------------
// File: Types.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the ""Software""), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 

package microsoft.sql;

/**
 * The class that defines the constants that are used to identify the SQL types
 * that are specific to Microsoft SQL Server.
 *
 * This class is never instantiated.
 */
public final class Types extends Object
{
    private Types()
    {
        // not reached
    }

    /*
     * The constant in the Java programming language, sometimes referred to as a type code,
     * that identifies the Microsoft SQL type DATETIMEOFFSET.
     */
    public static final int DATETIMEOFFSET = -155;
	public static final int STRUCTURED = -153;
	
	public static final int DATETIME = -151;
	public static final int SMALLDATETIME = -150;
	
	public static final int MONEY = -148;
	public static final int SMALLMONEY = -146;
	
	public static final int GUID = -145;
}
