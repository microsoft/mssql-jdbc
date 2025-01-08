/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.security.InvalidKeyException;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Optional;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;

import com.azure.core.credential.AccessToken;
import com.azure.core.credential.TokenRequestContext;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.identity.DefaultAzureCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;

import static com.microsoft.sqlserver.jdbc.Util.getHashedSecret;


/**
 * Various SQLServer security utilities.
 *
 */
class SQLServerSecurityUtility {
    static final private java.util.logging.Logger logger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerSecurityUtility");

    static final int GONE = 410;
    static final int TOO_MANY_RESQUESTS = 429;
    static final int NOT_FOUND = 404;
    static final int INTERNAL_SERVER_ERROR = 500;
    static final int NETWORK_CONNECT_TIMEOUT_ERROR = 599;

    static final String WINDOWS_KEY_STORE_NAME = "MSSQL_CERTIFICATE_STORE";

    // Environment variable for intellij keepass database path
    private static final String INTELLIJ_KEEPASS_PASS = "INTELLIJ_KEEPASS_PATH";

    // Environment variable for additionally allowed tenants. The tenantIds are comma delimited
    private static final String ADDITIONALLY_ALLOWED_TENANTS = "ADDITIONALLY_ALLOWED_TENANTS";

    // Credential Cache for ManagedIdentityCredential and DefaultAzureCredential
    private static final HashMap<String, Credential> CREDENTIAL_CACHE = new HashMap<>();

    private static final Lock CREDENTIAL_LOCK = new ReentrantLock();

	private static final int TOKEN_WAIT_DURATION_MS = 20000;

    private SQLServerSecurityUtility() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

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

    static SQLServerColumnEncryptionKeyStoreProvider getColumnEncryptionKeyStoreProvider(String providerName,
            SQLServerConnection connection, SQLServerStatement statement) throws SQLServerException {
        assert providerName != null && providerName.length() != 0 : "Provider name should not be null or empty";

        // check statement level KeyStoreProvider if statement is not null.
        if (statement != null && statement.hasColumnEncryptionKeyStoreProvidersRegistered()) {
            return statement.getColumnEncryptionKeyStoreProvider(providerName);
        }

        return connection.getColumnEncryptionKeyStoreProviderOnConnection(providerName);
    }

    static boolean shouldUseInstanceLevelProviderFlow(String keyStoreName, SQLServerConnection connection,
            SQLServerStatement statement) {
        return !keyStoreName.equalsIgnoreCase(WINDOWS_KEY_STORE_NAME)
                && (connection.hasConnectionColumnEncryptionKeyStoreProvidersRegistered()
                        || (null != statement && statement.hasColumnEncryptionKeyStoreProvidersRegistered()));
    }

    static SQLServerSymmetricKey getKeyFromLocalProviders(EncryptionKeyInfo keyInfo, SQLServerConnection connection,
            SQLServerStatement statement) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert null != serverName : "serverName should not be null in getKey.";

        if (logger.isLoggable(java.util.logging.Level.FINEST)) {
            logger.finest("Checking trusted master key path...");
        }
        Boolean[] hasEntry = new Boolean[1];
        List<String> trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(serverName,
                hasEntry);
        if (hasEntry[0] && ((null == trustedKeyPaths) || (trustedKeyPaths.isEmpty())
                || (!trustedKeyPaths.contains(keyInfo.keyPath)))) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UntrustedKeyPath"));
            Object[] msgArgs = {keyInfo.keyPath, serverName};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }

        SQLServerException lastException = null;
        SQLServerColumnEncryptionKeyStoreProvider provider = null;
        byte[] plaintextKey = null;

        try {
            provider = getColumnEncryptionKeyStoreProvider(keyInfo.keyStoreName, connection, statement);
            plaintextKey = provider.decryptColumnEncryptionKey(keyInfo.keyPath, keyInfo.algorithmName,
                    keyInfo.encryptedKey);

        } catch (SQLServerException e) {
            lastException = e;
        }

        if (null == plaintextKey) {
            if (null != lastException) {
                throw lastException;
            } else {
                throw new SQLServerException(null, SQLServerException.getErrString("R_CEKDecryptionFailed"), null, 0,
                        false);
            }
        }

        return new SQLServerSymmetricKey(plaintextKey);
    }

    /*
     * Encrypts the ciphertext.
     */
    static byte[] encryptWithKey(byte[] plainText, CryptoMetadata md, SQLServerConnection connection,
            SQLServerStatement statement) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert serverName != null : "Server name should not be null in EncryptWithKey";

        // Initialize cipherAlgo if not already done.
        if (!md.isAlgorithmInitialized()) {
            SQLServerSecurityUtility.decryptSymmetricKey(md, connection, statement);
        }

        assert md.isAlgorithmInitialized();
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
    private static String validateAndGetEncryptionAlgorithmName(byte cipherAlgorithmId,
            String cipherAlgorithmName) throws SQLServerException {
        // Custom cipher algorithm not supported for CTP.
        if (TDS.AEAD_AES_256_CBC_HMAC_SHA256 != cipherAlgorithmId) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_CustomCipherAlgorithmNotSupportedAE"),
                    null, 0, false);
        }
        return SQLServerAeadAes256CbcHmac256Algorithm.AEAD_AES_256_CBC_HMAC_SHA256;
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
    static void decryptSymmetricKey(CryptoMetadata md, SQLServerConnection connection,
            SQLServerStatement statement) throws SQLServerException {
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
                symKey = shouldUseInstanceLevelProviderFlow(keyInfo.keyStoreName, connection,
                        statement) ? getKeyFromLocalProviders(keyInfo, connection, statement)
                                   : globalCEKCache.getKey(keyInfo, connection);

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
        String algorithmName = validateAndGetEncryptionAlgorithmName(md.cipherAlgorithmId, md.cipherAlgorithmName); // may
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
    static byte[] decryptWithKey(byte[] cipherText, CryptoMetadata md, SQLServerConnection connection,
            SQLServerStatement statement) throws SQLServerException {
        String serverName = connection.getTrustedServerNameAE();
        assert null != serverName : "serverName should not be null in DecryptWithKey.";

        // Initialize cipherAlgo if not already done.
        if (!md.isAlgorithmInitialized()) {
            SQLServerSecurityUtility.decryptSymmetricKey(md, connection, statement);
        }

        assert md.isAlgorithmInitialized() : "Decryption Algorithm is not initialized";
        byte[] plainText = md.cipherAlgorithm.decryptData(cipherText); // this call succeeds or throws.
        if (null == plainText) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_PlainTextNullAE"), null, 0, false);
        }

        return plainText;
    }

    /*
     * Verify the signature for the CMK
     */
    static void verifyColumnMasterKeyMetadata(SQLServerConnection connection, SQLServerStatement statement,
            String keyStoreName, String keyPath, String serverName, boolean isEnclaveEnabled,
            byte[] cmkSignature) throws SQLServerException {

        // check trusted key paths
        Boolean[] hasEntry = new Boolean[1];
        List<String> trustedKeyPaths = SQLServerConnection.getColumnEncryptionTrustedMasterKeyPaths(serverName,
                hasEntry);
        if (hasEntry[0]
                && ((null == trustedKeyPaths) || (trustedKeyPaths.isEmpty()) || (!trustedKeyPaths.contains(keyPath)))) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UntrustedKeyPath"));
            Object[] msgArgs = {keyPath, serverName};
            throw new SQLServerException(form.format(msgArgs), null);
        }

        SQLServerColumnEncryptionKeyStoreProvider provider = null;
        if (shouldUseInstanceLevelProviderFlow(keyStoreName, connection, statement)) {
            provider = getColumnEncryptionKeyStoreProvider(keyStoreName, connection, statement);
        } else {
            provider = connection.getSystemOrGlobalColumnEncryptionKeyStoreProvider(keyStoreName);
        }

        if (!provider.verifyColumnMasterKeyMetadata(keyPath, isEnclaveEnabled, cmkSignature)) {
            throw new SQLServerException(SQLServerException.getErrString("R_VerifySignatureFailed"), null);
        }
    }

    /**
     * Get Managed Identity Authentication token through a ManagedIdentityCredential
     * 
     * @param resource
     *        Token resource.
     * @param managedIdentityClientId
     *        Client ID of the user-assigned Managed Identity.
     * @return fedauth token
     * @throws SQLServerException
     */
    static SqlAuthenticationToken getManagedIdentityCredAuthToken(String resource,
            String managedIdentityClientId, long millisecondsRemaining) throws SQLServerException {

        if (logger.isLoggable(java.util.logging.Level.FINEST)) {
            logger.finest("Getting Managed Identity authentication token for: " + managedIdentityClientId);
        }

        String key = getHashedSecret(
                new String[] {managedIdentityClientId, ManagedIdentityCredential.class.getSimpleName()});
        ManagedIdentityCredential mic = (ManagedIdentityCredential) getCredentialFromCache(key);

        if (null == mic) {
            CREDENTIAL_LOCK.lock();

            try {
                mic = (ManagedIdentityCredential) getCredentialFromCache(key);
                if (null == mic) {
                    ManagedIdentityCredentialBuilder micBuilder = new ManagedIdentityCredentialBuilder();

                    if (null != managedIdentityClientId && !managedIdentityClientId.isEmpty()) {
                        mic = micBuilder.clientId(managedIdentityClientId).build();
                    } else {
                        mic = micBuilder.build();
                    }

                    Credential credential = new Credential(mic);
                    CREDENTIAL_CACHE.put(key, credential);
                }
            } finally {
                CREDENTIAL_LOCK.unlock();
            }
        }

        TokenRequestContext tokenRequestContext = new TokenRequestContext();
        String scope = resource.endsWith(SQLServerMSAL4JUtils.SLASH_DEFAULT) ? resource : resource
                + SQLServerMSAL4JUtils.SLASH_DEFAULT;
        tokenRequestContext.setScopes(Arrays.asList(scope));

        SqlAuthenticationToken sqlFedAuthToken = null;

        Optional<AccessToken> accessTokenOptional = mic.getToken(tokenRequestContext).timeout(Duration.of(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), ChronoUnit.MILLIS)).blockOptional();

        if (!accessTokenOptional.isPresent()) {
            throw new SQLServerException(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionFail"),
                    null);
        } else {
            AccessToken accessToken = accessTokenOptional.get();
            sqlFedAuthToken = new SqlAuthenticationToken(accessToken.getToken(),
                    accessToken.getExpiresAt().toInstant().toEpochMilli());
        }

        if (logger.isLoggable(java.util.logging.Level.FINEST)) {
            logger.finest("Got fedAuth token, expiry: " + sqlFedAuthToken.getExpiresOn().toString());
        }

        return sqlFedAuthToken;
    }

    /**
     * Get Managed Identity Authentication token through the DefaultAzureCredential
     *
     * @param resource
     *        Token resource.
     * @param managedIdentityClientId
     *        Client ID of the user-assigned Managed Identity.
     * @return fedauth token
     * @throws SQLServerException
     */
    static SqlAuthenticationToken getDefaultAzureCredAuthToken(String resource,
            String managedIdentityClientId, int millisecondsRemaining) throws SQLServerException {
        String intellijKeepassPath = System.getenv(INTELLIJ_KEEPASS_PASS);
        String[] additionallyAllowedTenants = getAdditonallyAllowedTenants();

        int secretsLength = null == additionallyAllowedTenants ? 3 : additionallyAllowedTenants.length + 3;
        String[] secrets = new String[secretsLength];

        if (null != additionallyAllowedTenants && additionallyAllowedTenants.length != 0) {
            System.arraycopy(additionallyAllowedTenants, 0, secrets, 3, additionallyAllowedTenants.length);
        }

        secrets[0] = DefaultAzureCredential.class.getSimpleName();
        secrets[1] = managedIdentityClientId;
        secrets[2] = intellijKeepassPath;

        String key = getHashedSecret(secrets);
        DefaultAzureCredential dac = (DefaultAzureCredential) getCredentialFromCache(key);

        if (null == dac) {
            CREDENTIAL_LOCK.lock();

            try {
                dac = (DefaultAzureCredential) getCredentialFromCache(key);
                if (null == dac) {
                    DefaultAzureCredentialBuilder dacBuilder = new DefaultAzureCredentialBuilder();

                    if (null != managedIdentityClientId && !managedIdentityClientId.isEmpty()) {
                        dacBuilder.managedIdentityClientId(managedIdentityClientId);
                    }

                    if (null != intellijKeepassPath && !intellijKeepassPath.isEmpty()) {
                        dacBuilder.intelliJKeePassDatabasePath(intellijKeepassPath);
                    }

                    if (null != additionallyAllowedTenants && additionallyAllowedTenants.length != 0) {
                        dacBuilder.additionallyAllowedTenants(additionallyAllowedTenants);
                    }

                    dac = dacBuilder.build();

                    Credential credential = new Credential(dac);
                    CREDENTIAL_CACHE.put(key, credential);
                }
            } finally {
                CREDENTIAL_LOCK.unlock();
            }
        }

        TokenRequestContext tokenRequestContext = new TokenRequestContext();
        String scope = resource.endsWith(SQLServerMSAL4JUtils.SLASH_DEFAULT) ? resource : resource
                + SQLServerMSAL4JUtils.SLASH_DEFAULT;
        tokenRequestContext.setScopes(Arrays.asList(scope));

        SqlAuthenticationToken sqlFedAuthToken = null;

        Optional<AccessToken> accessTokenOptional = dac.getToken(tokenRequestContext).timeout(Duration.of(Math.min(millisecondsRemaining, TOKEN_WAIT_DURATION_MS), ChronoUnit.MILLIS)).blockOptional();

        if (!accessTokenOptional.isPresent()) {
            throw new SQLServerException(SQLServerException.getErrString("R_ManagedIdentityTokenAcquisitionFail"),
                    null);
        } else {
            AccessToken accessToken = accessTokenOptional.get();
            sqlFedAuthToken = new SqlAuthenticationToken(accessToken.getToken(),
                    accessToken.getExpiresAt().toInstant().toEpochMilli());
        }

        return sqlFedAuthToken;
    }

    private static String[] getAdditonallyAllowedTenants() {
        String additonallyAllowedTenants = System.getenv(ADDITIONALLY_ALLOWED_TENANTS);

        if (null != additonallyAllowedTenants && !additonallyAllowedTenants.isEmpty()) {
            return System.getenv(ADDITIONALLY_ALLOWED_TENANTS).split(",");
        }

        return null;
    }

    private static Object getCredentialFromCache(String key) {
        Credential credential = CREDENTIAL_CACHE.get(key);

        if (null != credential) {
            return credential.tokenCredential;
        }

        return null;
    }

    private static class Credential {
        Object tokenCredential;

        public Credential(Object tokenCredential) {
            this.tokenCredential = tokenCredential;
        }
    }
}
