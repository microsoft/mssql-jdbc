package com.microsoft.sqlserver.jdbc;

/**
 * Provides SqlAuthenticationToken callback to be implemented by client code.
 */
public interface SQLServerAccessTokenCallback {

    /**
     * For an example of callback usage, look under the project's code samples.
     *
     * Returns the access token for the authentication request
     * <p>
     * Runtime failures from this callback are reported as {@link SQLServerException} by the connection operation,
     * with the underlying cause preserved. Timeout and interruption causes receive token-acquisition-specific
     * messages. The callback is invoked synchronously and is responsible for enforcing its own timeouts and
     * preserving the calling thread's interrupt status when its own blocking work is interrupted.
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
