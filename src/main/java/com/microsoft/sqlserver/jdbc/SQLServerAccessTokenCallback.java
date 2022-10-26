package com.microsoft.sqlserver.jdbc;

/**
 * Provides an access token callback to be implemented by client code.
 */
public interface SQLServerAccessTokenCallback {
    String getAccessToken();
}
