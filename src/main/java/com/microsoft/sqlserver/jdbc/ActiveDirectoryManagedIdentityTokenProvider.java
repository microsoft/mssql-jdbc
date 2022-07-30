package com.microsoft.sqlserver.jdbc;

import java.util.Properties;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.SqlFedAuthInfo;

public class ActiveDirectoryManagedIdentityTokenProvider implements SqlFedAuthTokenProvider{

	@Override
	public SqlFedAuthToken getSqlFedAuthToken(SqlFedAuthInfo fedAuthInfo, Properties activeConnectionProperties)
			throws SQLServerException {
				return SQLServerSecurityUtility.getMSIAuthToken(fedAuthInfo.spn,
				activeConnectionProperties.getProperty(SQLServerDriverStringProperty.MSI_CLIENT_ID.toString()),
				cachedMsiTokenTtl);
	}

	@Override
	public boolean supportAuthenticationType(String authenticationType) {
		// TODO Auto-generated method stub
		return false;
	}
    
}
