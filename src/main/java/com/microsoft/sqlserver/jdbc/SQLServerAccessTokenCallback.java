package com.microsoft.sqlserver.jdbc;

/**
 * Provides SqlAuthenticationToken callback to be implemented by client code.
 */
public interface SQLServerAccessTokenCallback {

    /**
     * For an example of callback usage, look under the project's code samples.
     *
     * Returns the access token for the authentication request
     *
     * @param stsurl
     *        - Security token service URL.
     * @param spn
     *        - Service principal name.
     *
     * @return Returns a {@link SqlAuthenticationToken}.
     */
    SqlAuthenticationToken getAccessToken(String spn, String stsurl);
}
