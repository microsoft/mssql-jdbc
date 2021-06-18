/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.logging.Level;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.microsoft.sqlserver.jdbc.SQLServerConnection.ActiveDirectoryAuthentication;


/**
 * Various SQLServer security utilities.
 *
 */
class SQLServerSecurityUtility {
    static final private java.util.logging.Logger connectionlogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.internals.SQLServerConnection");

    static final int GONE = 410;
    static final int TOO_MANY_RESQUESTS = 429;
    static final int NOT_FOUND = 404;
    static final int INTERNAL_SERVER_ERROR = 500;
    static final int NETWORK_CONNECT_TIMEOUT_ERROR = 599;

    static final String WINDOWS_KEY_STORE_NAME = "MSSQL_CERTIFICATE_STORE";

    /**
     * Give the hash of given plain text
     * 
     * @param plainText
     * @param key
     * @param length
     * @return hash of the plain text using provided key
     * @throws NoSuchAlgorithmException
     * @throws InvalidKeyException
     */
    static byte[] getHMACWithSHA256(byte[] plainText, byte[] key,
            int length) throws NoSuchAlgorithmException, InvalidKeyException {
        byte[] computedHash;
        byte[] hash = new byte[length];
        Mac mac = Mac.getInstance("HmacSHA256");
        SecretKeySpec ivkeySpec = new SecretKeySpec(key, "HmacSHA256");
        mac.init(ivkeySpec);
        computedHash = mac.doFinal(plainText);
        // truncating hash if needed
        System.arraycopy(computedHash, 0, hash, 0, hash.length);
        return hash;
    }

    /**
     * Compare two arrays
     * 
     * @param buffer1
     *        first array
     * @param buffer2
     *        second array
     * @param buffer2Index
     * @param lengthToCompare
     * @return true if array contains same bytes otherwise false
     */
    static boolean compareBytes(byte[] buffer1, byte[] buffer2, int buffer2Index, int lengthToCompare) {
        if (null == buffer1 || null == buffer2) {
            return false;
        }

        if ((buffer2.length - buffer2Index) < lengthToCompare) {
            return false;
        }

        for (int index = 0; index < buffer1.length && index < lengthToCompare; ++index) {
            if (buffer1[index] != buffer2[buffer2Index + index]) {
                return false;
            }
        }
        return true;

    }

    static SQLServerColumnEncryptionKeyStoreProvider getColumnEncryptionKeyStoreProvider(String providerName, SQLServerConnection connection, SQLServerStatement statement) throws SQLServerException {
        assert providerName != null && providerName.length() != 0 : "Provider name should not be null or empty";

        // check statement level KeyStoreProvider if statement is not null.
        if (statement != null && statement.hasColumnEncryptionKeyStoreProvidersRegistered()) {
            return statement.getColumnEncryptionKeyStoreProvider(providerName);
        }

        return connection.getColumnEncryptionKeyStoreProviderOnConnection(providerName);
    }

    static boolean shouldUseInstanceLevelProviderFlow(String keyStoreName, SQLServerConnection connection, SQLServerStatement statement) {
        return !keyStoreName.equalsIgnoreCase(WINDOWS_KEY_STORE_NAME) 
            && (connection.hasConnectionColumnEncryptionKeyStoreProvidersRegistered() || (null != statement && statement.hasColumnEncryptionKeyStoreProvidersRegistered()));
    }

    static SQLServerSymmetricKey getKeyFromLocalProviders(EncryptionKeyInfo keyInfo, SQLServerConnection connection, SQLServerStatement statement) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert null != serverName : "serverName should not be null in getKey.";

        if (connectionlogger.isLoggable(java.util.logging.Level.FINE)) {
            connectionlogger.fine("Checking trusted master key path...");
        }
        Boolean[] hasEntry = new Boolean[1];
        List<String> trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(serverName, hasEntry);
        if (hasEntry[0]) {
            if ((null == trustedKeyPaths) || (0 == trustedKeyPaths.size()) || (!trustedKeyPaths.contains(keyInfo.keyPath))) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UntrustedKeyPath"));
                Object[] msgArgs = {keyInfo.keyPath, serverName};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
            }
        }

        SQLServerException lastException = null;
        SQLServerColumnEncryptionKeyStoreProvider provider = null;
        byte[] plaintextKey = null;
        
        try {
            provider = getColumnEncryptionKeyStoreProvider(keyInfo.keyStoreName, connection, statement);
            plaintextKey = provider.decryptColumnEncryptionKey(keyInfo.keyPath, keyInfo.algorithmName, keyInfo.encryptedKey);
            
        } catch (SQLServerException e) {
            lastException = e;
        }
        
        if (null == plaintextKey) {
            if (null != lastException) {
                throw lastException;
            } else {
                throw new SQLServerException(null, SQLServerException.getErrString("R_CEKDecryptionFailed"), null, 0, false);
            }
        }
        
        return new SQLServerSymmetricKey(plaintextKey);
    }

    /*
     * Encrypts the ciphertext.
     */
    static byte[] encryptWithKey(byte[] plainText, CryptoMetadata md,
            SQLServerConnection connection, SQLServerStatement statement) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert serverName != null : "Server name should not be null in EncryptWithKey";

        // Initialize cipherAlgo if not already done.
        if (!md.IsAlgorithmInitialized()) {
            SQLServerSecurityUtility.decryptSymmetricKey(md, connection, statement);
        }

        assert md.IsAlgorithmInitialized();
        byte[] cipherText = md.cipherAlgorithm.encryptData(plainText); // this call succeeds or throws.
        if (null == cipherText || 0 == cipherText.length) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_NullCipherTextAE"), null, 0, false);
        }
        return cipherText;
    }

    /**
     * Return the algorithm name mapped to an Id
     * 
     * @param cipherAlgorithmId
     *        The cipher algorithm Id
     * @param cipherAlgorithmName
     *        The cipher algorithm name
     * @return The cipher algorithm name
     */
    private static String ValidateAndGetEncryptionAlgorithmName(byte cipherAlgorithmId,
            String cipherAlgorithmName) throws SQLServerException {
        // Custom cipher algorithm not supported for CTP.
        if (TDS.AEAD_AES_256_CBC_HMAC_SHA256 != cipherAlgorithmId) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_CustomCipherAlgorithmNotSupportedAE"),
                    null, 0, false);
        }
        return SQLServerAeadAes256CbcHmac256Algorithm.algorithmName;
    }

    /**
     * Decrypts the symmetric key and saves it in metadata. In addition, initializes the SqlClientEncryptionAlgorithm
     * for rapid decryption.
     * 
     * @param md
     *        The cipher metadata
     * @param connection
     *        The connection
     * @param statement
     *        The statemenet
     */
    static void decryptSymmetricKey(CryptoMetadata md, SQLServerConnection connection, SQLServerStatement statement) throws SQLServerException {
        assert null != md : "md should not be null in DecryptSymmetricKey.";
        assert null != md.cekTableEntry : "md.EncryptionInfo should not be null in DecryptSymmetricKey.";
        assert null != md.cekTableEntry.columnEncryptionKeyValues : "md.EncryptionInfo.ColumnEncryptionKeyValues should not be null in DecryptSymmetricKey.";

        SQLServerSymmetricKey symKey = null;
        EncryptionKeyInfo encryptionkeyInfoChosen = null;
        SQLServerSymmetricKeyCache globalCEKCache = SQLServerSymmetricKeyCache.getInstance();
        Iterator<EncryptionKeyInfo> it = md.cekTableEntry.columnEncryptionKeyValues.iterator();
        SQLServerException lastException = null;
        while (it.hasNext()) {
            EncryptionKeyInfo keyInfo = it.next();
            try {
                symKey = shouldUseInstanceLevelProviderFlow(keyInfo.keyStoreName, connection, statement) ?
                getKeyFromLocalProviders(keyInfo, connection, statement) :
                globalCEKCache.getKey(keyInfo, connection);

                if (null != symKey) {
                    encryptionkeyInfoChosen = keyInfo;
                    break;
                }
            } catch (SQLServerException e) {
                lastException = e;
            }
        }

        if (null == symKey) {
            if (null != lastException) {
                throw lastException;
            } else {
                throw new SQLServerException(null, SQLServerException.getErrString("R_CEKDecryptionFailed"), null, 0,
                        false);
            }
        }

        // Given the symmetric key instantiate a SqlClientEncryptionAlgorithm object and cache it in metadata.
        md.cipherAlgorithm = null;
        SQLServerEncryptionAlgorithm cipherAlgorithm = null;
        String algorithmName = ValidateAndGetEncryptionAlgorithmName(md.cipherAlgorithmId, md.cipherAlgorithmName); // may
                                                                                                                    // throw
        cipherAlgorithm = SQLServerEncryptionAlgorithmFactoryList.getInstance().getAlgorithm(symKey, md.encryptionType,
                algorithmName); // will
                                // validate
                                // algorithm
                                // name and
                                // type
        assert null != cipherAlgorithm : "Cipher algorithm cannot be null in DecryptSymmetricKey";
        md.cipherAlgorithm = cipherAlgorithm;
        md.encryptionKeyInfo = encryptionkeyInfoChosen;
    }

    /*
     * Decrypts the ciphertext.
     */
    static byte[] decryptWithKey(byte[] cipherText, CryptoMetadata md,
            SQLServerConnection connection, SQLServerStatement statement) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert null != serverName : "serverName should not be null in DecryptWithKey.";

        // Initialize cipherAlgo if not already done.
        if (!md.IsAlgorithmInitialized()) {
            SQLServerSecurityUtility.decryptSymmetricKey(md, connection, statement);
        }

        assert md.IsAlgorithmInitialized() : "Decryption Algorithm is not initialized";
        byte[] plainText = md.cipherAlgorithm.decryptData(cipherText); // this call succeeds or throws.
        if (null == plainText) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_PlainTextNullAE"), null, 0, false);
        }

        return plainText;
    }

    /*
     * Verify the signature for the CMK
     */
    static void verifyColumnMasterKeyMetadata(SQLServerConnection connection, SQLServerStatement statement, String keyStoreName, String keyPath,
            String serverName, boolean isEnclaveEnabled, byte[] CMKSignature) throws SQLServerException {

        // check trusted key paths
        Boolean[] hasEntry = new Boolean[1];
        List<String> trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(serverName,
                hasEntry);
        if (hasEntry[0]) {
            if ((null == trustedKeyPaths) || (0 == trustedKeyPaths.size()) || (!trustedKeyPaths.contains(keyPath))) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UntrustedKeyPath"));
                Object[] msgArgs = {keyPath, serverName};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }

        SQLServerColumnEncryptionKeyStoreProvider provider = null;
        if (shouldUseInstanceLevelProviderFlow(keyStoreName, connection, statement)) {
            provider = getColumnEncryptionKeyStoreProvider(keyStoreName, connection, statement);
        } else {
            provider = connection.getSystemOrGlobalColumnEncryptionKeyStoreProvider(keyStoreName);
        }

        if (provider.verifyColumnMasterKeyMetadata(keyPath, isEnclaveEnabled, CMKSignature)) {
            throw new SQLServerException(SQLServerException.getErrString("R_VerifySignature"), null);
        }
    }

    /**
     * Get Managed Identity Authentication token
     * 
     * @param resource
     *        token resource
     * @param msiClientId
     *        Managed Identity or User Assigned Managed Identity
     * @return fedauth token
     * @throws SQLServerException
     */
    static SqlFedAuthToken getMSIAuthToken(String resource, String msiClientId) throws SQLServerException {
        // IMDS upgrade time can take up to 70s
        final int imdsUpgradeTimeInMs = 70 * 1000;
        final List<Integer> retrySlots = new ArrayList<>();

        StringBuilder urlString = new StringBuilder();
        int retry = 1, maxRetry = 1;

        // MSI_ENDPOINT and MSI_SECRET can be used instead of IDENTITY_ENDPOINT and IDENTITY_HEADER
        String identityEndpoint = System.getenv("IDENTITY_ENDPOINT");
        if (null == identityEndpoint || identityEndpoint.trim().isEmpty()) {
            identityEndpoint = System.getenv("MSI_ENDPOINT");
        }

        String identityHeader = System.getenv("IDENTITY_HEADER");
        if (null == identityHeader || identityHeader.trim().isEmpty()) {
            identityHeader = System.getenv("MSI_SECRET");
        }

        /*
         * isAzureFunction is used for identifying if the current client application is running in a Virtual Machine
         * (without Managed Identity environment variables) or App Service/Function (with Managed Identity environment
         * variables) as the APIs to be called for acquiring MSI Token are different for both cases.
         */
        boolean isAzureFunction = null != identityEndpoint && !identityEndpoint.isEmpty() && null != identityHeader
                && !identityHeader.isEmpty();

        if (isAzureFunction) {
            urlString.append(identityEndpoint).append("?api-version=2019-08-01&resource=").append(resource);
        } else {
            urlString.append(ActiveDirectoryAuthentication.AZURE_REST_MSI_URL).append("&resource=").append(resource);
            // Retry acquiring access token up to 20 times due to possible IMDS upgrade (Applies to VM only)
            maxRetry = 20;
            // Simplified variant of Exponential BackOff
            for (int x = 0; x < maxRetry; x++) {
                retrySlots.add(INTERNAL_SERVER_ERROR * ((2 << 1) - 1) / 1000);
            }
        }

        // Append Client Id if available
        if (null != msiClientId && !msiClientId.isEmpty()) {
            urlString.append("&client_id=").append(msiClientId);
        }

        // Loop while maxRetry reaches its limit
        while (retry <= maxRetry) {
            HttpURLConnection connection = null;

            try {
                connection = (HttpURLConnection) new URL(urlString.toString()).openConnection();
                connection.setRequestMethod("GET");

                if (isAzureFunction) {
                    connection.setRequestProperty("X-IDENTITY-HEADER", identityHeader);
                    if (connectionlogger.isLoggable(Level.FINER)) {
                        connectionlogger.finer("Using Azure Function/App Service Managed Identity auth: " + urlString);
                    }
                } else {
                    connection.setRequestProperty("Metadata", "true");
                    if (connectionlogger.isLoggable(Level.FINER)) {
                        connectionlogger.finer("Using Azure Managed Identity auth: " + urlString);
                    }
                }

                connection.connect();

                try (InputStream stream = connection.getInputStream()) {

                    BufferedReader reader = new BufferedReader(new InputStreamReader(stream, UTF_8), 100);
                    StringBuilder result = new StringBuilder(reader.readLine());

                    int startIndex_AT = result.indexOf(ActiveDirectoryAuthentication.ACCESS_TOKEN_IDENTIFIER)
                            + ActiveDirectoryAuthentication.ACCESS_TOKEN_IDENTIFIER.length();

                    String accessToken = result.substring(startIndex_AT, result.indexOf("\"", startIndex_AT + 1));

                    Calendar cal = new Calendar.Builder().setInstant(new Date()).build();

                    int startIndex_ATX;

                    // Fetch expires_on
                    if (isAzureFunction) {
                        startIndex_ATX = result
                                .indexOf(ActiveDirectoryAuthentication.ACCESS_TOKEN_EXPIRES_ON_IDENTIFIER)
                                + ActiveDirectoryAuthentication.ACCESS_TOKEN_EXPIRES_ON_IDENTIFIER.length();
                    } else {
                        startIndex_ATX = result
                                .indexOf(ActiveDirectoryAuthentication.ACCESS_TOKEN_EXPIRES_IN_IDENTIFIER)
                                + ActiveDirectoryAuthentication.ACCESS_TOKEN_EXPIRES_IN_IDENTIFIER.length();
                    }

                    String accessTokenExpiry = result.substring(startIndex_ATX,
                            result.indexOf("\"", startIndex_ATX + 1));
                    cal.add(Calendar.SECOND, Integer.parseInt(accessTokenExpiry));

                    return new SqlFedAuthToken(accessToken, cal.getTime());
                }
            } catch (Exception e) {
                retry++;
                // Below code applicable only when !isAzureFunctcion (VM)
                if (retry > maxRetry) {
                    // Do not retry if maxRetry limit has been reached.
                    break;
                } else {
                    try {
                        int responseCode = connection.getResponseCode();
                        // Check Error Response Code from Connection
                        if (GONE == responseCode || TOO_MANY_RESQUESTS == responseCode || NOT_FOUND == responseCode
                                || (INTERNAL_SERVER_ERROR <= responseCode
                                        && NETWORK_CONNECT_TIMEOUT_ERROR >= responseCode)) {
                            try {
                                int retryTimeoutInMs = retrySlots.get(ThreadLocalRandom.current().nextInt(retry - 1));
                                // Error code 410 indicates IMDS upgrade is in progress, which can take up to 70s
                                retryTimeoutInMs = (responseCode == 410
                                        && retryTimeoutInMs < imdsUpgradeTimeInMs) ? imdsUpgradeTimeInMs
                                                                                   : retryTimeoutInMs;
                                Thread.sleep(retryTimeoutInMs);
                            } catch (InterruptedException ex) {
                                // Throw runtime exception as driver must not be interrupted here
                                throw new RuntimeException(ex);
                            }
                        } else {
                            if (null != msiClientId && !msiClientId.isEmpty()) {
                                throw new SQLServerException(
                                        SQLServerException.getErrString("R_MSITokenFailureImdsClientId"), null);
                            } else {
                                throw new SQLServerException(SQLServerException.getErrString("R_MSITokenFailureImds"),
                                        null);
                            }
                        }
                    } catch (IOException io) {
                        // Throw error as unexpected if response code not available
                        throw new SQLServerException(SQLServerException.getErrString("R_MSITokenFailureUnexpected"),
                                null);
                    }
                }
            } finally {
                if (connection != null) {
                    connection.disconnect();
                }
            }
        }
        if (retry > maxRetry) {
            throw new SQLServerException(SQLServerException
                    .getErrString(isAzureFunction ? "R_MSITokenFailureEndpoint" : "R_MSITokenFailureImds"), null);
        }
        return null;
    }
}
