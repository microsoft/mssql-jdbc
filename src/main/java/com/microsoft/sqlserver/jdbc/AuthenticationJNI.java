/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.logging.Level;
import java.util.logging.Logger;


class FedAuthDllInfo {
    byte[] accessTokenBytes = null;
    long expiresIn = 0;

    FedAuthDllInfo(byte[] accessTokenBytes, long expiresIn) {
        this.accessTokenBytes = accessTokenBytes;
        this.expiresIn = expiresIn;
    }
}


/**
 * Encapsulation of the JNI native calls for trusted authentication.
 */
final class AuthenticationJNI extends SSPIAuthentication {

    private byte[] sniSec = new byte[MAXIMUM_POINTER_SIZE];
    private int[] sniSecLen =
    {0};
    private SQLServerConnection con;
    private static boolean enabled = false;
    private static Logger authLogger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.AuthenticationJNI");
    private static int sspiBlobMaxlen = 0;
    private static final UnsatisfiedLinkError LINK_ERROR;
    private static final int MAXIMUM_POINTER_SIZE = 128; // we keep the SNI_Sec pointer
    private final String dnsName;
    private final int port;

    static int getMaxSSPIBlobSize() {
        return sspiBlobMaxlen;
    }

    static boolean isDllLoaded() {
        return enabled;
    }

    static {
        UnsatisfiedLinkError temp = null;
        // Load the DLL
        try {
            String libName = "sqljdbc_auth";
            System.loadLibrary(libName);
            int[] pkg = new int[1];
            pkg[0] = 0;
            if (0 == SNISecInitPackage(pkg, authLogger)) {
                sspiBlobMaxlen = pkg[0];
            } else
                throw new UnsatisfiedLinkError();
            enabled = true;
        } catch (UnsatisfiedLinkError e) {
            temp = e;
            // This is not re-thrown on purpose - the constructor will terminate the properly with the appropriate error
            // string
        } finally {
            LINK_ERROR = temp;
        }

    }

    AuthenticationJNI(SQLServerConnection con, String address, int serverport) throws SQLServerException {
        if (!enabled)
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_notConfiguredForIntegrated"), LINK_ERROR);

        String[] dns = new String[1];
        if (GetDNSName(address, dns, authLogger) != 0) {
            // Simply initialize the DNS to address
            dns[0] = address;
        }

        this.con = con;
        dnsName = dns[0];
        port = serverport;
    }

    static FedAuthDllInfo getAccessTokenForWindowsIntegrated(String stsURL, String servicePrincipalName, String clientConnectionId, String clientId, long expirationFileTime) throws DLLException {
        FedAuthDllInfo dllInfo = ADALGetAccessTokenForWindowsIntegrated(stsURL, servicePrincipalName, clientConnectionId, clientId, expirationFileTime, authLogger);
        return dllInfo;
    }

    // InitDNSName should be called to initialize the DNSName before calling this function
    byte[] GenerateClientContext(byte[] pin, boolean[] done) throws SQLServerException {
        byte[] pOut;
        int[] outsize; // This is where the size of the filled data returned
        outsize = new int[1];
        outsize[0] = getMaxSSPIBlobSize();
        pOut = new byte[outsize[0]];

        // assert dnsName cant be null
        assert dnsName != null;

        int failure = SNISecGenClientContext(sniSec, sniSecLen, pin, pin.length, pOut, outsize, done, dnsName, port, null, null, authLogger);

        if (failure != 0) {
            if (authLogger.isLoggable(Level.WARNING)) {
                authLogger.warning(toString() + " Authentication failed code : " + failure);
            }
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"), LINK_ERROR);
        }
        // allocate space based on the size returned
        byte[] output = new byte[outsize[0]];

        System.arraycopy(pOut, 0, output, 0, outsize[0]);
        return output;
    }

    int releaseClientContext() {
        int success = 0;
        if (sniSecLen[0] > 0) {
            success = SNISecReleaseClientContext(sniSec, sniSecLen[0], authLogger);
            sniSecLen[0] = 0;
        }
        return success;
    }

    /*
     * we use arrays of size one in many places to retrieve output values Java Integer objects are immutable so we cant use them to get the output sizes. Same for String
     */
    native static byte[] DecryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm, byte[] encryptedColumnEncryptionKey) throws DLLException;

    private native static int SNISecGenClientContext(byte[] psec, int[] secptrsize, byte[] pin, int insize, byte[] pOut, int[] outsize, boolean[] done, String servername, int port, String username, String password, Logger log);

    private native static int SNISecReleaseClientContext(byte[] psec, int secptrsize, Logger log);

    private native static int SNISecInitPackage(int[] pcbMaxToken, Logger log);

    private native static int GetDNSName(String address, String[] DNSName, Logger log);

    private native static FedAuthDllInfo ADALGetAccessTokenForWindowsIntegrated(String stsURL, String servicePrincipalName, String clientConnectionId, String clientId, long expirationFileTime, Logger log);
}
