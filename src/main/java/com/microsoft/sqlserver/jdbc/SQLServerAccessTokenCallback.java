package com.microsoft.sqlserver.jdbc;

/**
 * Provides SqlAuthenticationToken callback to be implemented by client code.
 */
public interface SQLServerAccessTokenCallback {
    SqlAuthenticationToken getAccessToken(String stsurl, String spn);
}
