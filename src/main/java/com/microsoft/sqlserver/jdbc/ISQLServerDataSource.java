//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerDataSource.java
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

import javax.sql.CommonDataSource;


/**
* This datasource lists properties specific for the SQLServerConnection class.
*/
public interface ISQLServerDataSource extends CommonDataSource
{
    public void setApplicationIntent(String applicationIntent);
    public String getApplicationIntent();
	public void setApplicationName(String applicationName);
    public String getApplicationName();
    public void setDatabaseName(String databaseName);
    public String getDatabaseName();
    public void setInstanceName(String instanceName);
    public String getInstanceName();
    public void setIntegratedSecurity(boolean enable);
    public void setLastUpdateCount(boolean lastUpdateCount);
    public boolean getLastUpdateCount();
    public void setEncrypt(boolean encrypt);
    public boolean getEncrypt();
    public void setTrustServerCertificate(boolean e);
    public boolean getTrustServerCertificate();
    public void setTrustStore(String st);
    public String getTrustStore();
    public void setTrustStorePassword(String p);
    public void setHostNameInCertificate(String host);
    public String getHostNameInCertificate();
    public void setLockTimeout(int lockTimeout);
    public int getLockTimeout();
    public void setPassword(String password);
    public void setPortNumber(int portNumber);
    public int getPortNumber();
    public void setSelectMethod(String selectMethod);
    public String getSelectMethod();
    public void setResponseBuffering(String respo);
    public String getResponseBuffering();
    public void setSendTimeAsDatetime(boolean sendTimeAsDatetime);
    public boolean getSendTimeAsDatetime();
    public void setSendStringParametersAsUnicode(boolean sendStringParametersAsUnicode);
    public boolean getSendStringParametersAsUnicode();
    public void setServerName(String serverName);
    public String getServerName();
    public void setFailoverPartner(String serverName);
    public String getFailoverPartner();
    public void setMultiSubnetFailover(boolean multiSubnetFailover);
    public boolean getMultiSubnetFailover();    
    public void setUser(String user);
    public String getUser();
    public void setWorkstationID(String workstationID);
    public String getWorkstationID();
    public void setXopenStates(boolean xopenStates);
    public boolean getXopenStates();
    public void setURL(String url);
    public String getURL();
    public void setDescription(String description);
    public String getDescription();
    public void setPacketSize(int packetSize);
    public int getPacketSize();
	public void setAuthenticationScheme(String authenticationScheme);
	public void setServerSpn(String serverSpn);
	public String getServerSpn();
}

