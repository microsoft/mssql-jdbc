//---------------------------------------------------------------------------------------------------------------------------------
// File: FailOverMapSingleton.java
//
// Contents: This class keeps the failover map. The map is global
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
import java.util.*;

import java.util.logging.*;


final class FailoverMapSingleton  
{
	private static int INITIALHASHMAPSIZE = 5;
	private static HashMap<String, FailoverInfo> failoverMap = new HashMap<String, FailoverInfo>(INITIALHASHMAPSIZE);
	private FailoverMapSingleton(){/*hide the constructor to stop the instantiation of this class.*/}
	private static String concatPrimaryDatabase(String primary, String instance, String database)
	{
		StringBuilder buf = new StringBuilder();
		buf.append(primary);
		if(null != instance)
		{
			buf.append("\\");
			buf.append(instance);
		}
		buf.append(";");
		buf.append(database);
		return buf.toString();
	}
	static FailoverInfo getFailoverInfo(SQLServerConnection connection, String primaryServer, String instance, String database )
	{
		synchronized (FailoverMapSingleton.class) 
		{
			if(true == failoverMap.isEmpty())
			{
				return null;
			}
			else
			{
				String mapKey = concatPrimaryDatabase(primaryServer, instance, database);
				if(connection.getConnectionLogger().isLoggable(Level.FINER))
					connection.getConnectionLogger().finer (connection.toString() + " Looking up info in the map using key: " + mapKey);
				FailoverInfo fo = failoverMap.get(mapKey);
				if(null != fo)
					fo.log(connection);
				return fo;
			}
		}
	}

	// The map is populated with primary server, instance name and db as the key (always user info) value is the failover server name provided
	// by the server. The map is only populated if the server sends failover info.
	static void putFailoverInfo(SQLServerConnection connection,  String primaryServer, 
			String instance, String database, FailoverInfo actualFailoverInfo, boolean actualuseFailover, String failoverPartner) throws SQLServerException
	{
		FailoverInfo fo;

		synchronized (FailoverMapSingleton.class) 
		{
			// one more check to make sure someone already did not do  this 
			if(null == (fo=getFailoverInfo(connection,  primaryServer, instance, database) ))
			{
				if(connection.getConnectionLogger().isLoggable(Level.FINE))
					connection.getConnectionLogger().fine (connection.toString() +  " Failover map add server: " +primaryServer + "; database:" + database+
						"; Mirror:" + failoverPartner);
				failoverMap.put(concatPrimaryDatabase(primaryServer, instance, database), actualFailoverInfo);
			}
			else
				// if the class exists make sure the latest info is updated
				fo.failoverAdd(connection,  actualuseFailover, failoverPartner);
		}
	}
}
