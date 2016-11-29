//---------------------------------------------------------------------------------------------------------------------------------
// File: SQLServerEncryptionAlgorithmFactoryList.java
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
 
 
package com.microsoft.sqlserver.jdbc;


import java.text.MessageFormat;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Maintain list of all the encryption algorithm factory classes 
 */
 final class SQLServerEncryptionAlgorithmFactoryList {
	
	private ConcurrentHashMap<String,SQLServerEncryptionAlgorithmFactory> encryptionAlgoFactoryMap;
	
	private static final SQLServerEncryptionAlgorithmFactoryList instance = new SQLServerEncryptionAlgorithmFactoryList();
	
	private SQLServerEncryptionAlgorithmFactoryList(){
		encryptionAlgoFactoryMap = new ConcurrentHashMap<String, SQLServerEncryptionAlgorithmFactory>();
		encryptionAlgoFactoryMap.putIfAbsent(SQLServerAeadAes256CbcHmac256Algorithm.algorithmName, new SQLServerAeadAes256CbcHmac256Factory());		
	}
	
	static SQLServerEncryptionAlgorithmFactoryList getInstance(){
		return instance;
	}
	
	 /**
	  * @return list of registered algorithms separated by comma 
	  */
	 String getRegisteredCipherAlgorithmNames(){
		StringBuffer stringBuff = new StringBuffer();
		boolean first = true;
		for(String key:encryptionAlgoFactoryMap.keySet()){
		   if(first){
		       stringBuff.append("'");
		       first = false;
		   }else {
		       stringBuff.append(", '");
		   }
		   stringBuff.append(key);
		   stringBuff.append("'");
		    
		}
		return stringBuff.toString();		
	}
	
	 /**
	  * Return instance for given algorithm 
	  * @param key 
	  * @param encryptionType
	  * @param algorithmName
	  * @return instance for given algorithm 
	  * @throws SQLServerException
	  */
	 SQLServerEncryptionAlgorithm  getAlgorithm(SQLServerSymmetricKey key, SQLServerEncryptionType encryptionType, String algorithmName) throws SQLServerException{
		SQLServerEncryptionAlgorithm encryptionAlgorithm = null;
		SQLServerEncryptionAlgorithmFactory factory = null;
		if(!encryptionAlgoFactoryMap.containsKey(algorithmName)){
		    MessageFormat form =new MessageFormat(SQLServerException.getErrString("R_UnknownColumnEncryptionAlgorithm"));
            Object[] msgArgs={algorithmName, SQLServerEncryptionAlgorithmFactoryList.getInstance().getRegisteredCipherAlgorithmNames()};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
		}		
		
		factory = encryptionAlgoFactoryMap.get(algorithmName);
		assert null != factory : "Null Algorithm Factory class detected";
		encryptionAlgorithm = factory.create(key, encryptionType, algorithmName);
		return encryptionAlgorithm;		
	}

}
