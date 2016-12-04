//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerSymmetricKey.java
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
 * 
 * Base class which represents Symmetric key 
 *
 */
 class SQLServerSymmetricKey {
	private byte [] rootKey;
	
	
	 SQLServerSymmetricKey(byte [] rootKey) throws SQLServerException{
	     if(null == rootKey)
	     {
	    	 throw new SQLServerException(this, SQLServerException.getErrString("R_NullColumnEncryptionKey"), null, 0, false);
	     }
	     else if (0 == rootKey.length)
	     { 
	    	 throw new SQLServerException(this, SQLServerException.getErrString("R_EmptyColumnEncryptionKey"), null, 0, false);  
	     }
		this.rootKey=rootKey;
	}
	
	byte [] getRootKey(){
		return rootKey;
	}
	
	int length(){
		return rootKey.length;
	}
	
	void zeroOutKey()
	{
		for (int i = 0; i < rootKey.length; i++)
		{
			rootKey[i] = (byte) 0;
		}
	}
}
