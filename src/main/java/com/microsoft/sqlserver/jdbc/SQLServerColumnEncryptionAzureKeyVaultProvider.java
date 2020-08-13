/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import static java.nio.charset.StandardCharsets.UTF_16LE;

import java.io.FileInputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Properties;
import java.util.logging.Level;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.identity.ManagedIdentityCredential;
import com.azure.identity.ManagedIdentityCredentialBuilder;
import com.azure.security.keyvault.keys.KeyClient;
import com.azure.security.keyvault.keys.KeyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.CryptographyClient;
import com.azure.security.keyvault.keys.cryptography.CryptographyClientBuilder;
import com.azure.security.keyvault.keys.cryptography.models.KeyWrapAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.SignResult;
import com.azure.security.keyvault.keys.cryptography.models.SignatureAlgorithm;
import com.azure.security.keyvault.keys.cryptography.models.UnwrapResult;
import com.azure.security.keyvault.keys.cryptography.models.VerifyResult;
import com.azure.security.keyvault.keys.cryptography.models.WrapResult;
import com.azure.security.keyvault.keys.models.KeyType;
import com.azure.security.keyvault.keys.models.KeyVaultKey;
import com.microsoft.aad.msal4j.ConfidentialClientApplication;

/**
 * Provides implementation similar to certificate store provider. A CEK encrypted with certificate store provider should
 * be decryptable by this provider and vice versa.
 * 
 * Envelope Format for the encrypted column encryption key version + keyPathLength + ciphertextLength + keyPath +
 * ciphertext + signature version: A single byte indicating the format version. keyPathLength: Length of the keyPath.
 * ciphertextLength: ciphertext length keyPath: keyPath used to encrypt the column encryption key. This is only used for
 * troubleshooting purposes and is not verified during decryption. ciphertext: Encrypted column encryption key
 * signature: Signature of the entire byte array. Signature is validated before decrypting the column encryption key.
 */
public class SQLServerColumnEncryptionAzureKeyVaultProvider extends SQLServerColumnEncryptionKeyStoreProvider {

    private final static java.util.logging.Logger akvLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.SQLServerColumnEncryptionAzureKeyVaultProvider");
    /**
     * Column Encryption Key Store Provider string
     */
    String name = "AZURE_KEY_VAULT";

    private static final String MSSQL_JDBC_PROPERTIES = "mssql-jdbc.properties";
    private static final String AKV_TRUSTED_ENDPOINTS_KEYWORD = "AKVTrustedEndpoints";
    private static final List<String> akvTrustedEndpoints;
    static {
        akvTrustedEndpoints = getTrustedEndpoints();
    }
//    private final String rsaEncryptionAlgorithmWithOAEPForAKV = "RSA-OAEP";

    /**
     * Algorithm version
     */
    private final byte[] firstVersion = new byte[] {0x01};

    private CryptographyClient cryptoClient;
    private KeyClient keyVaultClient;
    private TokenCredential credential;

    public void setName(String name) {
        this.name = name;
    }

    public String getName() {
        return this.name;
    }

    /**
     * Constructs a SQLServerColumnEncryptionAzureKeyVaultProvider with a client id and client key to authenticate to
     * AAD. This is used by KeyVaultClient at runtime to authenticate to Azure Key Vault.
     * 
     * @param clientId
     *        Identifier of the client requesting the token.
     * @param clientKey
     *        Key of the client requesting the token.
     * @param tenantId
     *        The tenant ID of the application.
     * @throws SQLServerException
     *         when an error occurs
     */
    public SQLServerColumnEncryptionAzureKeyVaultProvider(String clientId,
                                                          String clientKey,
                                                          String tenantId) throws SQLServerException {
        if (clientId == null || clientId.isEmpty()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client ID"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }
        if (clientKey == null || clientKey.isEmpty()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client Key"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }
        createKeyvaultClients(new ClientSecretCredentialBuilder()
              .clientId(clientId)
              .clientSecret(clientKey)
              .tenantId(tenantId)
              .build());
    }

    /**
     * Constructs a SQLServerColumnEncryptionAzureKeyVaultProvider to authenticate to AAD. This is used by
     * KeyVaultClient at runtime to authenticate to Azure Key Vault.
     */
    SQLServerColumnEncryptionAzureKeyVaultProvider() throws SQLServerException {
        createKeyvaultClients(new ManagedIdentityCredentialBuilder().build());
    }

    /**
     * Constructs a SQLServerColumnEncryptionAzureKeyVaultProvider to authenticate to AAD. This is used by
     * KeyVaultClient at runtime to authenticate to Azure Key Vault.
     *
     * @param clientId
     *        Identifier of the client requesting the token.
     */
    SQLServerColumnEncryptionAzureKeyVaultProvider(String clientId) throws SQLServerException {
        if (clientId == null || clientId.isEmpty()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NullValue"));
            Object[] msgArgs1 = {"Client ID"};
            throw new SQLServerException(form.format(msgArgs1), null);
        }
        createKeyvaultClients(new ManagedIdentityCredentialBuilder().clientId(clientId).build());
    }

    /**
     * Constructs a SQLServerColumnEncryptionAzureKeyVaultProvider using the provided TokenCredential to authenticate to
     * AAD. This is used by KeyVaultClient at runtime to authenticate to Azure Key Vault.
     *
     * @param tokenCredential
     *        The TokenCredential to use to authenticate to Azure Key Vault.
     */
    public SQLServerColumnEncryptionAzureKeyVaultProvider(TokenCredential tokenCredential) throws SQLServerException {
        createKeyvaultClients(tokenCredential);
    }

    private void createKeyvaultClients(TokenCredential credential) throws SQLServerException {
        String vaultBaseUrl = "susanakv.vault.azure.net"; //System.getenv("vaultBaseUrl");
        String vaultFullUrl = "https://" + vaultBaseUrl;

        if (null == vaultBaseUrl || vaultBaseUrl.isEmpty()) {
            // TODO externalise string
//            throw new SQLServerException(SQLServerException.getErrString("R_NullEncryptedColumnEncryptionKey"), null);
            throw new SQLServerException("vaultBaseUrl is not valid: " + vaultBaseUrl, null);
        }

        this.credential = credential;

        this.keyVaultClient = new KeyClientBuilder()
            .credential(credential)
            .vaultUrl(vaultFullUrl)
            .buildClient();
    }

    /**
     * Decrypts an encrypted CEK with RSA encryption algorithm using the asymmetric key specified by the key path
     * 
     * @param masterKeyPath
     *        - Complete path of an asymmetric key in AKV
     * @param encryptionAlgorithm
     *        - Asymmetric Key Encryption Algorithm
     * @param encryptedColumnEncryptionKey
     *        - Encrypted Column Encryption Key
     * @return Plain text column encryption key
     */
    @Override
    public byte[] decryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {

        // Validate the input parameters
        this.ValidateNonEmptyAKVPath(masterKeyPath);

        if (null == encryptedColumnEncryptionKey) {
            throw new SQLServerException(SQLServerException.getErrString("R_NullEncryptedColumnEncryptionKey"), null);
        }

        if (0 == encryptedColumnEncryptionKey.length) {
            throw new SQLServerException(SQLServerException.getErrString("R_EmptyEncryptedColumnEncryptionKey"), null);
        }

        // Validate encryptionAlgorithm
        KeyWrapAlgorithm _encryptionAlgorithm = this.validateEncryptionAlgorithm(encryptionAlgorithm);

        // Validate whether the key is RSA one or not and then get the key size
        int keySizeInBytes = getAKVKeySize(masterKeyPath);

        // Validate and decrypt the EncryptedColumnEncryptionKey
        // Format is
        // version + keyPathLength + ciphertextLength + keyPath + ciphertext + signature
        //
        // keyPath is present in the encrypted column encryption key for identifying the original source of the
        // asymmetric key pair and
        // we will not validate it against the data contained in the CMK metadata (masterKeyPath).

        // Validate the version byte
        if (encryptedColumnEncryptionKey[0] != firstVersion[0]) {
            MessageFormat form = new MessageFormat(
                    SQLServerException.getErrString("R_InvalidEcryptionAlgorithmVersion"));
            Object[] msgArgs = {String.format("%02X ", encryptedColumnEncryptionKey[0]),
                    String.format("%02X ", firstVersion[0])};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        // Get key path length
        int currentIndex = firstVersion.length;
        short keyPathLength = convertTwoBytesToShort(encryptedColumnEncryptionKey, currentIndex);
        // We just read 2 bytes
        currentIndex += 2;

        // Get ciphertext length
        short cipherTextLength = convertTwoBytesToShort(encryptedColumnEncryptionKey, currentIndex);
        currentIndex += 2;

        // Skip KeyPath
        // KeyPath exists only for troubleshooting purposes and doesnt need validation.
        currentIndex += keyPathLength;

        // validate the ciphertext length
        if (cipherTextLength != keySizeInBytes) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVKeyLengthError"));
            Object[] msgArgs = {cipherTextLength, keySizeInBytes, masterKeyPath};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        // Validate the signature length
        int signatureLength = encryptedColumnEncryptionKey.length - currentIndex - cipherTextLength;

        if (signatureLength != keySizeInBytes) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVSignatureLengthError"));
            Object[] msgArgs = {signatureLength, keySizeInBytes, masterKeyPath};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        // Get ciphertext
        byte[] cipherText = new byte[cipherTextLength];
        System.arraycopy(encryptedColumnEncryptionKey, currentIndex, cipherText, 0, cipherTextLength);
        currentIndex += cipherTextLength;

        // Get signature
        byte[] signature = new byte[signatureLength];
        System.arraycopy(encryptedColumnEncryptionKey, currentIndex, signature, 0, signatureLength);

        // Compute the hash to validate the signature
        byte[] hash = new byte[encryptedColumnEncryptionKey.length - signature.length];

        System.arraycopy(encryptedColumnEncryptionKey, 0, hash, 0,
                encryptedColumnEncryptionKey.length - signature.length);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_NoSHA256Algorithm"), e);
        }
        md.update(hash);
        byte dataToVerify[] = md.digest();

        if (null == dataToVerify) {
            throw new SQLServerException(SQLServerException.getErrString("R_HashNull"), null);
        }

        // Validate the signature
        if (!AzureKeyVaultVerifySignature(dataToVerify, signature, masterKeyPath)) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CEKSignatureNotMatchCMK"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        // Decrypt the CEK
        byte[] decryptedCEK = this.AzureKeyVaultUnWrap(masterKeyPath, _encryptionAlgorithm, cipherText);

        return decryptedCEK;
    }

    private short convertTwoBytesToShort(byte[] input, int index) throws SQLServerException {

        short shortVal;
        if (index + 1 >= input.length) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_ByteToShortConversion"), null, 0,
                    false);
        }
        ByteBuffer byteBuffer = ByteBuffer.allocate(2);
        byteBuffer.order(ByteOrder.LITTLE_ENDIAN);
        byteBuffer.put(input[index]);
        byteBuffer.put(input[index + 1]);
        shortVal = byteBuffer.getShort(0);
        return shortVal;

    }

    /**
     * Encrypts CEK with RSA encryption algorithm using the asymmetric key specified by the key path.
     * 
     * @param masterKeyPath
     *        - Complete path of an asymmetric key in AKV
     * @param encryptionAlgorithm
     *        - Asymmetric Key Encryption Algorithm
     * @param columnEncryptionKey
     *        - Plain text column encryption key
     * @return Encrypted column encryption key
     */
    @Override
    public byte[] encryptColumnEncryptionKey(String masterKeyPath, String encryptionAlgorithm,
            byte[] columnEncryptionKey) throws SQLServerException {

        // Validate the input parameters
        this.ValidateNonEmptyAKVPath(masterKeyPath);

        if (null == columnEncryptionKey) {
            throw new SQLServerException(SQLServerException.getErrString("R_NullColumnEncryptionKey"), null);
        }

        if (0 == columnEncryptionKey.length) {
            throw new SQLServerException(SQLServerException.getErrString("R_EmptyCEK"), null);
        }

        // Validate encryptionAlgorithm
        KeyWrapAlgorithm _encryptionAlgorithm = this.validateEncryptionAlgorithm(encryptionAlgorithm);

        // Validate whether the key is RSA one or not and then get the key size
        int keySizeInBytes = getAKVKeySize(masterKeyPath);

        // Construct the encryptedColumnEncryptionKey
        // Format is
        // version + keyPathLength + ciphertextLength + ciphertext + keyPath + signature
        //
        // We currently only support one version
        byte[] version = new byte[] {firstVersion[0]};

        // Get the Unicode encoded bytes of cultureinvariant lower case masterKeyPath
        byte[] masterKeyPathBytes = masterKeyPath.toLowerCase(Locale.ENGLISH).getBytes(UTF_16LE);

        byte[] keyPathLength = new byte[2];
        keyPathLength[0] = (byte) (((short) masterKeyPathBytes.length) & 0xff);
        keyPathLength[1] = (byte) (((short) masterKeyPathBytes.length) >> 8 & 0xff);

        // Encrypt the plain text
        byte[] cipherText = this.AzureKeyVaultWrap(masterKeyPath, _encryptionAlgorithm, columnEncryptionKey);

        byte[] cipherTextLength = new byte[2];
        cipherTextLength[0] = (byte) (((short) cipherText.length) & 0xff);
        cipherTextLength[1] = (byte) (((short) cipherText.length) >> 8 & 0xff);

        if (cipherText.length != keySizeInBytes) {
            throw new SQLServerException(SQLServerException.getErrString("R_CipherTextLengthNotMatchRSASize"), null);
        }

        // Compute hash
        // SHA-2-256(version + keyPathLength + ciphertextLength + keyPath + ciphertext)
        byte[] dataToHash = new byte[version.length + keyPathLength.length + cipherTextLength.length
                + masterKeyPathBytes.length + cipherText.length];
        int destinationPosition = version.length;
        System.arraycopy(version, 0, dataToHash, 0, version.length);

        System.arraycopy(keyPathLength, 0, dataToHash, destinationPosition, keyPathLength.length);
        destinationPosition += keyPathLength.length;

        System.arraycopy(cipherTextLength, 0, dataToHash, destinationPosition, cipherTextLength.length);
        destinationPosition += cipherTextLength.length;

        System.arraycopy(masterKeyPathBytes, 0, dataToHash, destinationPosition, masterKeyPathBytes.length);
        destinationPosition += masterKeyPathBytes.length;

        System.arraycopy(cipherText, 0, dataToHash, destinationPosition, cipherText.length);

        MessageDigest md = null;
        try {
            md = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_NoSHA256Algorithm"), e);
        }
        md.update(dataToHash);
        byte dataToSign[] = md.digest();

        // Sign the hash
        byte[] signedHash = AzureKeyVaultSignHashedData(dataToSign, masterKeyPath);

        if (signedHash.length != keySizeInBytes) {
            throw new SQLServerException(SQLServerException.getErrString("R_SignedHashLengthError"), null);
        }

        if (!this.AzureKeyVaultVerifySignature(dataToSign, signedHash, masterKeyPath)) {
            throw new SQLServerException(SQLServerException.getErrString("R_InvalidSignatureComputed"), null);
        }

        // Construct the encrypted column encryption key
        // EncryptedColumnEncryptionKey = version + keyPathLength + ciphertextLength + keyPath + ciphertext + signature
        int encryptedColumnEncryptionKeyLength = version.length + cipherTextLength.length + keyPathLength.length
                + cipherText.length + masterKeyPathBytes.length + signedHash.length;
        byte[] encryptedColumnEncryptionKey = new byte[encryptedColumnEncryptionKeyLength];

        // Copy version byte
        int currentIndex = 0;
        System.arraycopy(version, 0, encryptedColumnEncryptionKey, currentIndex, version.length);
        currentIndex += version.length;

        // Copy key path length
        System.arraycopy(keyPathLength, 0, encryptedColumnEncryptionKey, currentIndex, keyPathLength.length);
        currentIndex += keyPathLength.length;

        // Copy ciphertext length
        System.arraycopy(cipherTextLength, 0, encryptedColumnEncryptionKey, currentIndex, cipherTextLength.length);
        currentIndex += cipherTextLength.length;

        // Copy key path
        System.arraycopy(masterKeyPathBytes, 0, encryptedColumnEncryptionKey, currentIndex, masterKeyPathBytes.length);
        currentIndex += masterKeyPathBytes.length;

        // Copy ciphertext
        System.arraycopy(cipherText, 0, encryptedColumnEncryptionKey, currentIndex, cipherText.length);
        currentIndex += cipherText.length;

        // copy the signature
        System.arraycopy(signedHash, 0, encryptedColumnEncryptionKey, currentIndex, signedHash.length);

        return encryptedColumnEncryptionKey;
    }

    /**
     * Validates that the encryption algorithm is RSA_OAEP and if it is not, then throws an exception.
     *
     * @param encryptionAlgorithm
     *        - Asymmetric key encryptio algorithm
     * @return The encryption algorithm that is going to be used.
     * @throws SQLServerException
     */
    private KeyWrapAlgorithm validateEncryptionAlgorithm(String encryptionAlgorithm) throws SQLServerException {

        if (null == encryptionAlgorithm) {
            throw new SQLServerException(null, SQLServerException.getErrString("R_NullKeyEncryptionAlgorithm"), null, 0,
                    false);
        }

        // Transform to standard format (dash instead of underscore) to support enum lookup
        if ("RSA_OAEP".equalsIgnoreCase(encryptionAlgorithm)) {
            encryptionAlgorithm = "RSA-OAEP";
        }

        if (!"RSA-OAEP".equalsIgnoreCase(encryptionAlgorithm.trim())) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidKeyEncryptionAlgorithm"));
            Object[] msgArgs = {encryptionAlgorithm, "RSA-OAEP"};
            throw new SQLServerException(this, form.format(msgArgs), null, 0, false);
        }

        return KeyWrapAlgorithm.fromString(encryptionAlgorithm);
    }

    /**
     * Checks if the Azure Key Vault key path is Empty or Null (and raises exception if they are).
     * 
     * @param masterKeyPath
     * @throws SQLServerException
     */
    private void ValidateNonEmptyAKVPath(String masterKeyPath) throws SQLServerException {
        // throw appropriate error if masterKeyPath is null or empty
        if (null == masterKeyPath || masterKeyPath.trim().isEmpty()) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVPathNull"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        } else {
            URI parsedUri = null;
            try {
                parsedUri = new URI(masterKeyPath);

                // A valid URI.
                // Check if it is pointing to a trusted endpoint.
                String host = parsedUri.getHost();
                if (null != host) {
                    host = host.toLowerCase(Locale.ENGLISH);
                }
                for (final String endpoint : akvTrustedEndpoints) {
                    if (null != host && host.endsWith(endpoint)) {
                        return;
                    }
                }
            } catch (URISyntaxException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVURLInvalid"));
                Object[] msgArgs = {masterKeyPath};
                throw new SQLServerException(form.format(msgArgs), null, 0, e);
            }

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVMasterKeyPathInvalid"));
            Object[] msgArgs = {masterKeyPath};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }
    }

    /**
     * Encrypts the text using specified Azure Key Vault key.
     * 
     * @param masterKeyPath
     *        - Azure Key Vault key url.
     * @param encryptionAlgorithm
     *        - Encryption Algorithm.
     * @param columnEncryptionKey
     *        - Plain text Column Encryption Key.
     * @return Returns an encrypted blob or throws an exception if there are any errors.
     * @throws SQLServerException
     */
    private byte[] AzureKeyVaultWrap(String masterKeyPath, KeyWrapAlgorithm encryptionAlgorithm,
            byte[] columnEncryptionKey) throws SQLServerException {
        if (null == columnEncryptionKey) {
            throw new SQLServerException(SQLServerException.getErrString("R_CEKNull"), null);
        }

        WrapResult wrappedKey = cryptoClient.wrapKey(KeyWrapAlgorithm.RSA_OAEP, columnEncryptionKey);
        return wrappedKey.getEncryptedKey();
    }

    /**
     * Encrypts the text using specified Azure Key Vault key.
     * 
     * @param masterKeyPath
     *        - Azure Key Vault key url.
     * @param encryptionAlgorithm
     *        - Encrypted Column Encryption Key.
     * @param encryptedColumnEncryptionKey
     *        - Encrypted Column Encryption Key.
     * @return Returns the decrypted plaintext Column Encryption Key or throws an exception if there are any errors.
     * @throws SQLServerException
     */
    private byte[] AzureKeyVaultUnWrap(String masterKeyPath, KeyWrapAlgorithm encryptionAlgorithm,
            byte[] encryptedColumnEncryptionKey) throws SQLServerException {
        if (null == encryptedColumnEncryptionKey) {
            throw new SQLServerException(SQLServerException.getErrString("R_EncryptedCEKNull"), null);
        }

        if (0 == encryptedColumnEncryptionKey.length) {
            throw new SQLServerException(SQLServerException.getErrString("R_EmptyEncryptedCEK"), null);
        }

        UnwrapResult unwrappedKey = cryptoClient.unwrapKey(encryptionAlgorithm, encryptedColumnEncryptionKey);

        return unwrappedKey.getKey();
    }

    /**
     * Generates signature based on RSA PKCS#v1.5 scheme using a specified Azure Key Vault Key URL.
     * 
     * @param dataToSign
     *        - Text to sign.
     * @param masterKeyPath
     *        - Azure Key Vault key url.
     * @return Signature
     * @throws SQLServerException
     */
    private byte[] AzureKeyVaultSignHashedData(byte[] dataToSign, String masterKeyPath) throws SQLServerException {
        assert ((null != dataToSign) && (0 != dataToSign.length));

        SignResult signedData = cryptoClient.sign(SignatureAlgorithm.RS256, dataToSign);
        return signedData.getSignature();
    }

    /**
     * Verifies the given RSA PKCSv1.5 signature.
     * 
     * @param dataToVerify
     * @param signature
     * @param masterKeyPath
     *        - Azure Key Vault key url.
     * @return true if signature is valid, false if it is not valid
     * @throws SQLServerException
     */
    private boolean AzureKeyVaultVerifySignature(byte[] dataToVerify, byte[] signature,
            String masterKeyPath) throws SQLServerException {
        assert ((null != dataToVerify) && (0 != dataToVerify.length));
        assert ((null != signature) && (0 != signature.length));

        VerifyResult valid = cryptoClient.verify(SignatureAlgorithm.RS256, dataToVerify, signature);

        return valid.isValid();
    }

    /**
     * Returns the public Key size in bytes.
     * 
     * @param masterKeyPath
     *        - Azure Key Vault Key path
     * @return Key size in bytes
     * @throws SQLServerException
     *         when an error occurs
     */
    private int getAKVKeySize(String masterKeyPath) throws SQLServerException {
        masterKeyPath = "Always-Encrypted-Auto1";
        KeyVaultKey retrievedKey = keyVaultClient.getKey(masterKeyPath);

        if (null == retrievedKey) {
            String[] keyTokens = masterKeyPath.split("/");

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVKeyNotFound"));
            Object[] msgArgs = {keyTokens[keyTokens.length - 1]};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }

        if (retrievedKey.getKeyType() != KeyType.RSA && retrievedKey.getKeyType() != KeyType.RSA_HSM) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_NonRSAKey"));
            Object[] msgArgs = {retrievedKey.getKeyType().toString()};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }

        if (cryptoClient == null) {
            cryptoClient = new CryptographyClientBuilder()
                .credential(credential)
                .keyIdentifier(retrievedKey.getId())
                .buildClient();
        }

        return retrievedKey.getKey().getN().length;
    }

    @Override
    public boolean verifyColumnMasterKeyMetadata(String masterKeyPath, boolean allowEnclaveComputations,
            byte[] signature) throws SQLServerException {
        if (!allowEnclaveComputations)
            return false;

        KeyStoreProviderCommon.validateNonEmptyMasterKeyPath(masterKeyPath);

        try {
            MessageDigest md = MessageDigest.getInstance("SHA-256");
            md.update(name.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
            md.update(masterKeyPath.toLowerCase().getBytes(java.nio.charset.StandardCharsets.UTF_16LE));
            // value of allowEnclaveComputations is always true here
            md.update("true".getBytes(java.nio.charset.StandardCharsets.UTF_16LE));

            byte[] dataToVerify = md.digest();
            if (null == dataToVerify) {
                throw new SQLServerException(SQLServerException.getErrString("R_HashNull"), null);
            }

            // Sign the hash
            byte[] signedHash = AzureKeyVaultSignHashedData(dataToVerify, masterKeyPath);
            if (null == signedHash) {
                throw new SQLServerException(SQLServerException.getErrString("R_SignedHashLengthError"), null);
            }

            // Validate the signature
            return AzureKeyVaultVerifySignature(dataToVerify, signature, masterKeyPath);
        } catch (NoSuchAlgorithmException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_NoSHA256Algorithm"), e);
        }
    }

    private static List<String> getTrustedEndpoints() {
        Properties mssqlJdbcProperties = getMssqlJdbcProperties();
        List<String> trustedEndpoints = new ArrayList<String>();
        boolean append = true;
        if (null != mssqlJdbcProperties) {
            String endpoints = mssqlJdbcProperties.getProperty(AKV_TRUSTED_ENDPOINTS_KEYWORD);
            if (null != endpoints && !endpoints.trim().isEmpty()) {
                endpoints = endpoints.trim();
                // Append if the list starts with a semicolon.
                if (';' != endpoints.charAt(0)) {
                    append = false;
                } else {
                    endpoints = endpoints.substring(1);
                }
                String[] entries = endpoints.split(";");
                for (String entry : entries) {
                    if (null != entry && !entry.trim().isEmpty()) {
                        trustedEndpoints.add(entry.trim());
                    }
                }
            }
        }
        /*
         * List of Azure trusted endpoints
         * https://docs.microsoft.com/en-us/azure/key-vault/key-vault-secure-your-key-vault
         */
        if (append) {
            trustedEndpoints.add("vault.azure.net");
            trustedEndpoints.add("vault.azure.cn");
            trustedEndpoints.add("vault.usgovcloudapi.net");
            trustedEndpoints.add("vault.microsoftazure.de");
        }
        return trustedEndpoints;
    }

    /**
     * Attempt to read MSSQL_JDBC_PROPERTIES.
     *
     * @return corresponding Properties object or null if failed to read the file.
     */
    private static Properties getMssqlJdbcProperties() {
        Properties props = null;
        try (FileInputStream in = new FileInputStream(MSSQL_JDBC_PROPERTIES)) {
            props = new Properties();
            props.load(in);
        } catch (IOException e) {
            if (akvLogger.isLoggable(Level.FINER)) {
                akvLogger.finer("Unable to load the mssql-jdbc.properties file: " + e);
            }
        }
        return (null != props && !props.isEmpty()) ? props : null;
    }
}
