package com.microsoft.sqlserver.jdbc;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SqlFedAuthTokenProviderFactory {

    public static final SqlFedAuthTokenProviderFactory INSTANCE = new SqlFedAuthTokenProviderFactory();
    private final List<Class> providerClassNames = new ArrayList<>();

    private SqlFedAuthTokenProviderFactory() {
        initializeProviders();
    }

    private void initializeProviders() {
        providerClassNames.add(ActiveDirectoryPasswordTokenProvider.class);
    }

    private final Map<String, SqlFedAuthTokenProvider> providers = new HashMap<>();

    /**
     * return a valid {@code SqlFedAuthTokenProvider} or throw an exception if no
     * one found.
     * 
     * @param authenticationType
     * @return
     */
    public SqlFedAuthTokenProvider getOrCreateAuthTokenProvider(String authenticationType) {

        if (authenticationType == null || authenticationType.length() == 0) {
            // throw sql server exception
            throw new IllegalArgumentException();
        }

        // get from cache

        SqlFedAuthTokenProvider provider = providers.get(authenticationType);

        if (provider != null) {
            return provider;
        }

        try {
            // load default providers
            for (Class providerClass : providerClassNames) {

                provider = getTokenProviderFromClass(providerClass, authenticationType);
                if (provider != null) {
                    return provider;
                }
            }
            //get from external configuration, which supports "authentication=com.example.custome.AuthTokenProvider"
            Class externalTokenProviderClass = Class.forName(authenticationType);

            provider = getTokenProviderFromClass(externalTokenProviderClass, authenticationType);
            if (provider != null) {
                return provider;
            }
        } catch (Exception e) {

            // TODO rethrow exception
            throw new RuntimeException(e);

        }

        throw new IllegalArgumentException("no token provider found for authentication type: " + authenticationType);

    }

    private SqlFedAuthTokenProvider getTokenProviderFromClass(Class providerClass, String authenticationType)
            throws InstantiationException, IllegalAccessException, IllegalArgumentException, InvocationTargetException,
            NoSuchMethodException, SecurityException {
        if (providerClass.isAssignableFrom(SqlFedAuthTokenProvider.class)) {
            SqlFedAuthTokenProvider provider = (SqlFedAuthTokenProvider) providerClass.getConstructor()
                    .newInstance();
            if (provider.supportAuthenticationType(authenticationType)) {

                if (provider.singleton()) {
                    providers.put(authenticationType, provider);
                }
                return provider;
            }
        }
        return null;
    }
}
