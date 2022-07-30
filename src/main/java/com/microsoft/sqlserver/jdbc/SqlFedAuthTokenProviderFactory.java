package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.List;

public class SqlFedAuthTokenProviderFactory {
    
    public static final SqlFedAuthTokenProviderFactory INSTANCE = new SqlFedAuthTokenProviderFactory();
    private final List<String> providerClassNames = new ArrayList<>();

    private SqlFedAuthTokenProviderFactory(){
        initializeProviders();
    }

    

    private void initializeProviders() {
        providerClassNames.add(ActiveDirectoryPasswordTokenProvider.class.getName());
	}



    /**
     * return a valid {@code SqlFedAuthTokenProvider} or throw an exception if no one found.
     * @param authenticationType
     * @return
     */
	public SqlFedAuthTokenProvider getOrCreateAuthTokenProvider(String authenticationType){

    }
}
