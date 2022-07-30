/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;

public class ActiveDirectoryPasswordTokenProvider implements SqlFedAuthTokenProvider {


    private static final String AUTH_TYPE = SqlAuthentication.ActiveDirectoryPassword.toString();
    private static final boolean MSAL_CONTEXT_EXISTS = msalContextExists();

	@Override
	public SqlFedAuthToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, Properties activeConnectionProperties) throws SQLServerException {
        String user = activeConnectionProperties.getProperty(SQLServerDriverStringProperty.USER.toString());
        return SQLServerMSAL4JUtils.getSqlFedAuthToken(fedAuthInfo, user,
                activeConnectionProperties.getProperty(SQLServerDriverStringProperty.PASSWORD.toString()),
                AUTH_TYPE);
	}

    private static boolean msalContextExists() {
        try {
            Class.forName("com.microsoft.aad.msal4j.PublicClientApplication");
        } catch (ClassNotFoundException e) {
            return false;
        }
        return true;
    }

	@Override
	public boolean supportAuthenticationType(String authenticationType) {
		return MSAL_CONTEXT_EXISTS && authenticationType !=null && AUTH_TYPE.equalsIgnoreCase(authenticationType);
	}
    
}
