/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.util.logging.Level;

class FedAuthDllInfo {
    byte[] accessTokenBytes = null;
    long expiresIn = 0;

    FedAuthDllInfo(byte[] accessTokenBytes,
            long expiresIn) {
        this.accessTokenBytes = accessTokenBytes;
        this.expiresIn = expiresIn;
    }
}

/**
 * Encapsulation of the JNI native calls for trusted authentication.
 */
final class AuthenticationJNI extends SSPIAuthentication {
    private final static int maximumpointersize = 128; // we keep the SNI_Sec pointer
    private static boolean enabled = false;
    private static java.util.logging.Logger authLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.AuthenticationJNI");
    private static int sspiBlobMaxlen = 0;
    private byte[] sniSec = new byte[maximumpointersize];
    private int sniSecLen[] = {0};
    private final String DNSName;
    private final int port;
    private SQLServerConnection con;

    private static final UnsatisfiedLinkError linkError;

    static int GetMaxSSPIBlobSize() {
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
            }
            else
                throw new UnsatisfiedLinkError();
            enabled = true;
        }
        catch (UnsatisfiedLinkError e) {
            temp = e;
            authLogger.warning("Failed to load the sqljdbc_auth.dll cause : " + e.getMessage());
            // This is not re-thrown on purpose - the constructor will terminate the properly with the appropriate error string
        }
        finally {
            linkError = temp;
        }

    }

    AuthenticationJNI(SQLServerConnection con,
            String address,
            int serverport) throws SQLServerException {
        if (!enabled)
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_notConfiguredForIntegrated"), linkError);

        this.con = con;
        DNSName = GetDNSName(address);
        port = serverport;
    }

    static FedAuthDllInfo getAccessTokenForWindowsIntegrated(String stsURL,
            String servicePrincipalName,
            String clientConnectionId,
            String clientId,
            long expirationFileTime) throws DLLException {
        FedAuthDllInfo dllInfo = ADALGetAccessTokenForWindowsIntegrated(stsURL, servicePrincipalName, clientConnectionId, clientId,
                expirationFileTime, authLogger);
        return dllInfo;
    }

    // InitDNSName should be called to initialize the DNSName before calling this function
    byte[] GenerateClientContext(byte[] pin,
            boolean[] done) throws SQLServerException {
        byte[] pOut;
        int[] outsize; // This is where the size of the filled data returned
        outsize = new int[1];
        outsize[0] = GetMaxSSPIBlobSize();
        pOut = new byte[outsize[0]];

        // assert DNSName cant be null
        assert DNSName != null;

        int failure = SNISecGenClientContext(sniSec, sniSecLen, pin, pin.length, pOut, outsize, done, DNSName, port, null, null, authLogger);

        if (failure != 0) {
            if (authLogger.isLoggable(Level.WARNING)) {
                authLogger.warning(toString() + " Authentication failed code : " + failure);
            }
            con.terminate(SQLServerException.DRIVER_ERROR_NONE, SQLServerException.getErrString("R_integratedAuthenticationFailed"), linkError);
        }
        // allocate space based on the size returned
        byte output[] = new byte[outsize[0]];
        System.arraycopy(pOut, 0, output, 0, outsize[0]);
        return output;
    }

    /* L0 */ int ReleaseClientContext() {
        int success = 0;
        if (sniSecLen[0] > 0) {
            success = SNISecReleaseClientContext(sniSec, sniSecLen[0], authLogger);
            sniSecLen[0] = 0;
        }
        return success;
    }

    // note we handle the failures of the GetDNSName in this function, this function will return an empty string if the underlying call fails.
    private static String GetDNSName(String address) {
        String DNS[] = new String[1];
        if (GetDNSName(address, DNS, authLogger) != 0) {
            // Simply initialize the DNS to address
            DNS[0] = address;
        }
        return DNS[0];
    }

    // we use arrays of size one in many places to retrieve output values
    // Java Integer objects are immutable so we cant use them to get the output sizes.
    // Same for String
    /* L0 */private native static int SNISecGenClientContext(byte[] psec,
            int[] secptrsize,
            byte[] pin,
            int insize,
            byte[] pOut,
            int[] outsize,
            boolean[] done,
            String servername,
            int port,
            String username,
            String password,
            java.util.logging.Logger log);

    /* L0 */ private native static int SNISecReleaseClientContext(byte[] psec,
            int secptrsize,
            java.util.logging.Logger log);

    private native static int SNISecInitPackage(int[] pcbMaxToken,
            java.util.logging.Logger log);

    private native static int SNISecTerminatePackage(java.util.logging.Logger log);

    private native static int SNIGetSID(byte[] SID,
            java.util.logging.Logger log);

    private native static boolean SNIIsEqualToCurrentSID(byte[] SID,
            java.util.logging.Logger log);

    private native static int GetDNSName(String address,
            String[] DNSName,
            java.util.logging.Logger log);

    private native static FedAuthDllInfo ADALGetAccessTokenForWindowsIntegrated(String stsURL,
            String servicePrincipalName,
            String clientConnectionId,
            String clientId,
            long expirationFileTime,
            java.util.logging.Logger log);

    native static byte[] DecryptColumnEncryptionKey(String masterKeyPath,
            String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws DLLException;
}
