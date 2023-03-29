/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Date;
import java.util.UUID;
import java.util.logging.Level;


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
    private static final int MAXPOINTERSIZE = 128; // we keep the SNI_Sec pointer
    static boolean isDllLoaded = false;
    private static java.util.logging.Logger authLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.AuthenticationJNI");
    private static int sspiBlobMaxlen = 0;
    private byte[] sniSec = new byte[MAXPOINTERSIZE];
    private int[] sniSecLen = {0};
    private final String dnsName;
    private final int port;
    private SQLServerConnection con;

    private static final UnsatisfiedLinkError linkError;

    static int getMaxSSPIBlobSize() {
        return sspiBlobMaxlen;
    }

    static {
        UnsatisfiedLinkError temp = null;
        // Load the DLL
        try {
            System.loadLibrary(SQLServerDriver.AUTH_DLL_NAME);
            int[] pkg = new int[1];

            if (0 == SNISecInitPackage(pkg, authLogger)) {
                sspiBlobMaxlen = pkg[0];
            } else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnableToLoadAuthDllManually"));
                throw new UnsatisfiedLinkError(form.format(new Object[] {SQLServerDriver.AUTH_DLL_NAME}));
            }
            isDllLoaded = true;
        } catch (UnsatisfiedLinkError ue) {
            // If os is windows, attempt to extract and load the DLL packaged with the driver
            if (SQLServerConnection.isWindows) {
                String tempDirectory = System.getProperty("java.io.tmpdir") + SQLServerDriver.DLL_NAME + "\\";
                File outputDLL = new File(tempDirectory + UUID.randomUUID() + "-" + new Date().getTime() + ".dll");

                try {
                    Files.createDirectories(Paths.get(tempDirectory));

                    try (InputStream is = AuthenticationJNI.class.getResourceAsStream(
                            "/" + SQLServerDriver.DLL_NAME + "." + Util.getJVMArchOnWindows() + ".dll")) {
                        try (FileOutputStream fos = new FileOutputStream(outputDLL)) {
                            if (is != null) {
                                int length = is.available();
                                byte[] buffer = new byte[length];
                                if ((length = is.read(buffer)) != -1) {
                                    fos.write(buffer, 0, length);
                                }
                            }
                        }
                    }

                    System.load(outputDLL.getAbsolutePath());

                    int[] pkg = new int[1];

                    if (0 == SNISecInitPackage(pkg, authLogger)) {
                        sspiBlobMaxlen = pkg[0];
                    } else {
                        throw new UnsatisfiedLinkError(SQLServerException.getErrString("R_UnableToLoadPackagedAuthDll"));
                    }

                    isDllLoaded = true;

                } catch (UnsatisfiedLinkError e) {
                    temp = e; // R_UnableToLoadPackagedAuthDll
                } catch (IOException ioe) {
                    temp = new UnsatisfiedLinkError(ioe.getMessage());
                } finally {
                    Runtime.getRuntime().addShutdownHook(cleanup(tempDirectory));
                }
            }

            // If temp is still null and DLL is still not enabled, we attempted to load the DLL manually and failed
            if (null == temp && !isDllLoaded) {
                temp = ue; // R_UnableToLoadAuthDllManually
            }

            // The errors above are not re-thrown on purpose - the constructor will terminate properly
            // with the appropriate error string

        } finally {
            linkError = temp;
        }
    }

    AuthenticationJNI(SQLServerConnection con, String address, int serverport) throws SQLServerException {
        if (!isDllLoaded) {
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_notConfiguredForIntegrated"), linkError);
        }

        this.con = con;
        dnsName = initDNSArray(address);
        port = serverport;
    }

    static FedAuthDllInfo getAccessTokenForWindowsIntegrated(String stsURL, String servicePrincipalName,
            String clientConnectionId, String clientId, long expirationFileTime) throws DLLException {
        return ADALGetAccessTokenForWindowsIntegrated(stsURL, servicePrincipalName, clientConnectionId, clientId,
                expirationFileTime, authLogger);
    }

    // InitDNSName should be called to initialize the DNSName before calling this function
    byte[] generateClientContext(byte[] pin, boolean[] done) throws SQLServerException {
        byte[] pOut;
        int[] outsize; // This is where the size of the filled data returned
        outsize = new int[1];
        outsize[0] = getMaxSSPIBlobSize();
        pOut = new byte[outsize[0]];

        // assert DNSName cant be null
        assert dnsName != null;

        int failure = SNISecGenClientContext(sniSec, sniSecLen, pin, pin.length, pOut, outsize, done, dnsName, port,
                null, null, authLogger);

        if (failure != 0) {
            if (authLogger.isLoggable(Level.WARNING)) {
                authLogger.warning(toString() + " Authentication failed code : " + failure);
            }
            con.terminate(SQLServerException.DRIVER_ERROR_NONE,
                    SQLServerException.getErrString("R_integratedAuthenticationFailed"), linkError);
        }
        // allocate space based on the size returned
        byte[] output = new byte[outsize[0]];
        System.arraycopy(pOut, 0, output, 0, outsize[0]);
        return output;
    }

    void releaseClientContext() {
        int success = 0;
        if (sniSecLen[0] > 0) {
            success = SNISecReleaseClientContext(sniSec, sniSecLen[0], authLogger);
            sniSecLen[0] = 0;
        }
        if (authLogger.isLoggable(Level.FINER)) {
            authLogger.finer(toString() + " Release client context status : " + success);
        }
    }

    // note we handle the failures of the GetDNSName in this function, this function will return an empty string if the
    // underlying call fails.
    private static String initDNSArray(String address) {
        String[] dns = new String[1];
        if (GetDNSName(address, dns, authLogger) != 0) {
            // Simply initialize the DNS to address
            dns[0] = address;
        }
        return dns[0];
    }

    private static Thread cleanup(String dllDirectory) {
        return new Thread(() -> {
            File[] files = new File(dllDirectory).listFiles();

            if (null != files) {
                for (File dll : files) {
                    // If DLL is still loaded and in use, deletion will fail. So, it is safe
                    // to iterate and delete all files.
                    dll.delete();
                }
            }
        });
    }

    // we use arrays of size one in many places to retrieve output values
    // Java Integer objects are immutable so we cant use them to get the output sizes.
    // Same for String
    private static native int SNISecGenClientContext(byte[] psec, int[] secptrsize, byte[] pin, int insize, byte[] pOut,
            int[] outsize, boolean[] done, String servername, int port, String username, String password,
            java.util.logging.Logger log);

    private static native int SNISecReleaseClientContext(byte[] psec, int secptrsize, java.util.logging.Logger log);

    private static native int SNISecInitPackage(int[] pcbMaxToken, java.util.logging.Logger log);

    private static native int SNISecTerminatePackage(java.util.logging.Logger log);

    private static native int SNIGetSID(byte[] SID, java.util.logging.Logger log);

    private static native boolean SNIIsEqualToCurrentSID(byte[] SID, java.util.logging.Logger log);

    private static native int GetDNSName(String address, String[] DNSName, java.util.logging.Logger log);

    private static synchronized native FedAuthDllInfo ADALGetAccessTokenForWindowsIntegrated(String stsURL,
            String servicePrincipalName, String clientConnectionId, String clientId, long expirationFileTime,
            java.util.logging.Logger log);

    static synchronized native byte[] DecryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws DLLException;

    static synchronized native boolean VerifyColumnMasterKeyMetadata(String keyPath, boolean allowEnclaveComputations,
            byte[] signature) throws DLLException;
}
