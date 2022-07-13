/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;


/**
 * Implements a cache for query metadata returned from sp_describe_parameter_encryption calls. Adding, removing, and
 * reading from the cache is handled here, with the location of the cache being in the EnclaveSession.
 * 
 */
class ParameterMetaDataCache {
    
    static final int CACHE_SIZE = 2000; // Size of the cache in number of entries
    static final int CACHE_TRIM_THRESHOLD = 300; // Threshold above which to trim the cache
    CryptoCache cache = new CryptoCache(); // Represents the actual cache of CEK and metadata
    static private java.util.logging.Logger metadataCacheLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ParameterMetaDataCache");

    /**
     * Retrieves the metadata from the cache, should it exist.
     * 
     * @param params
     *        Array of parameters used
     * @param parameterNames
     *        Names of parameters used
     * @param session
     *        The current enclave session containing the cache
     * @param connection
     *        The SQLServer connection
     * @param stmt
     *        The SQLServer statement, whose returned metadata we're checking
     * @return true, if the metadata for the query can be retrieved
     * 
     */
    boolean getQueryMetadata(Parameter[] params, ArrayList<String> parameterNames, SQLServerConnection connection, 
        SQLServerStatement stmt) throws SQLServerException {

        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeys(stmt, connection);
        ConcurrentHashMap<String, CryptoMetadata> metadataMap = cache.getCacheEntry(encryptionValues.getKey());

        if (metadataMap == null) {
            if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                metadataCacheLogger.finest("Cache Miss. Unable to retrieve cache entry from cache.");
            }
            return false;
        }

        for (int i = 0; i < params.length; i++) {
            boolean found = metadataMap.containsKey(parameterNames.get(i));
            CryptoMetadata foundData = metadataMap.get(parameterNames.get(i));

            /*
             * If ever the map doesn't contain a parameter, the cache entry cannot be used. If there is data found, it
             * should never have the initialized algorithm as that would contain the key. Clear all metadata that has
             * already been assigned in either case.
             */
            if (!found || (foundData != null && foundData.isAlgorithmInitialized())) {
                for (Parameter param : params) {
                    param.cryptoMeta = null;
                }
                if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                    metadataCacheLogger
                            .finest("Cache Miss. Cache entry either has missing parameter or initialized algorithm.");
                }
                return false;
            }
            params[i].cryptoMeta = foundData;
        }

        // Assign the key using a metadata copy. We shouldn't load from the cached version for security reasons.
        for (int i = 0; i < params.length; ++i) {
            try {
                CryptoMetadata cryptoCopy = null;
                CryptoMetadata metaData = params[i].getCryptoMetadata();
                if (metaData != null) {
                    cryptoCopy = new CryptoMetadata(metaData.getCekTableEntry(), metaData.getOrdinal(),
                            metaData.getEncryptionAlgorithmId(), metaData.getEncryptionAlgorithmName(),
                            metaData.getEncryptionType().getValue(), metaData.getNormalizationRuleVersion());
                }

                params[i].cryptoMeta = cryptoCopy;

                if (cryptoCopy != null) {
                    try {
                        SQLServerSecurityUtility.decryptSymmetricKey(cryptoCopy, connection, stmt);
                    } catch (SQLServerException e) {

                        removeCacheEntry(stmt, cache, connection);

                        for (Parameter paramToCleanup : params) {
                            paramToCleanup.cryptoMeta = null;
                        }

                        if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                            metadataCacheLogger.finest("Cache Miss. Unable to decrypt CEK.");
                        }
                        return false;
                    }
                }
            } catch (Exception e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CryptoCacheInaccessible"));
                Object[] msgArgs = {e.getMessage()};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }

        if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
            metadataCacheLogger.finest("Cache Hit. Successfully retrieved metadata from cache.");
        }
        return true;
    }

    /**
     * 
     * Adds the parameter metadata to the cache, also handles cache trimming.
     * 
     * @param params
     *        List of parameters used
     * @param parameterNames
     *        Names of parameters used
     * @param session
     *        Enclave session containing the cryptocache
     * @param connection
     *        SQLServerConnection
     * @param stmt
     *        SQLServer statement used to retrieve keys to find correct cache
     * @param cekList
     *        The list of CEKs (from the first RS) that is also added to the cache as well as parameter metadata
     * @return true, if the query metadata has been added correctly
     */
    boolean addQueryMetadata(Parameter[] params, ArrayList<String> parameterNames, SQLServerConnection connection, 
            SQLServerStatement stmt, Map<Integer, CekTableEntry> cekList) throws SQLServerException {

        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeys(stmt, connection);
        if (encryptionValues.getKey() == null) {
            return false;
        }

        ConcurrentHashMap<String, CryptoMetadata> metadataMap = new ConcurrentHashMap<>(params.length);

        for (int i = 0; i < params.length; i++) {
            try {
                CryptoMetadata cryptoCopy = null;
                CryptoMetadata metaData = params[i].getCryptoMetadata();
                if (metaData != null) {

                    cryptoCopy = new CryptoMetadata(metaData.getCekTableEntry(), metaData.getOrdinal(),
                            metaData.getEncryptionAlgorithmId(), metaData.getEncryptionAlgorithmName(),
                            metaData.getEncryptionType().getValue(), metaData.getNormalizationRuleVersion());
                }
                if (cryptoCopy != null && !cryptoCopy.isAlgorithmInitialized()) {
                    String paramName = parameterNames.get(i);
                    metadataMap.put(paramName, cryptoCopy);
                } else {
                    return false;
                }
            } catch (SQLServerException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CryptoCacheInaccessible"));
                Object[] msgArgs = {e.getMessage()};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }

        // If the size of the cache exceeds the threshold, set that we are in trimming and trim the cache accordingly.
        int cacheSizeCurrent = cache.getParamMap().size();
        if (cacheSizeCurrent > CACHE_SIZE + CACHE_TRIM_THRESHOLD) {
            int entriesToRemove = cacheSizeCurrent - CACHE_SIZE;
            ConcurrentHashMap<String, ConcurrentHashMap<String, CryptoMetadata>> newMap = new ConcurrentHashMap<>();
            ConcurrentHashMap<String, ConcurrentHashMap<String, CryptoMetadata>> oldMap = cache.getParamMap();
            int count = 0;

            for (Map.Entry<String, ConcurrentHashMap<String, CryptoMetadata>> entry : oldMap.entrySet()) {
                if (count >= entriesToRemove) {
                    newMap.put(entry.getKey(), entry.getValue());
                }
                count++;
            }
            cache.replaceParamMap(newMap);
            if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                metadataCacheLogger.finest("Cache successfully trimmed.");
            }
        }
        if (connection.getServerColumnEncryptionVersion() == ColumnEncryptionVersion.AE_V3) {
            Map<Integer, CekTableEntry> keysToBeCached = copyEnclaveKeys(cekList);
            cache.addEnclaveEntry(encryptionValues.getValue(), keysToBeCached);
        }

        cache.addParamEntry(encryptionValues.getKey(), metadataMap);
        return true;
    }

    /**
     * 
     * Remove the cache entry.
     * 
     * @param stmt
     *        SQLServer statement used to retrieve keys
     * @param session
     *        The enclave session where the cryptocache is stored
     * @param connection
     *        The SQLServerConnection, also used to retrieve keys
     */
    void removeCacheEntry(SQLServerStatement stmt, CryptoCache cache, SQLServerConnection connection) {
        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeys(stmt, connection);
        if (encryptionValues.getKey() == null) {
            return;
        }

        cache.removeParamEntry(encryptionValues.getKey());
    }

    /**
     * 
     * Returns the cache and enclave lookup keys for a given connection and statement
     * 
     * @param statement
     *        The SQLServer statement used to construct part of the keys
     * @param connection
     *        The connection from which database name is retrieved
     * @return A key value pair containing cache lookup key and enclave lookup key
     */
    private AbstractMap.SimpleEntry<String, String> getCacheLookupKeys(SQLServerStatement statement,
            SQLServerConnection connection) {

        StringBuilder cacheLookupKeyBuilder = new StringBuilder();
        cacheLookupKeyBuilder.append(":::");
        String databaseName = connection.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString());
        cacheLookupKeyBuilder.append(databaseName);
        cacheLookupKeyBuilder.append(":::");
        cacheLookupKeyBuilder.append(statement.toString());

        String cacheLookupKey = cacheLookupKeyBuilder.toString();
        String enclaveLookupKey = cacheLookupKeyBuilder.append(":::enclaveKeys").toString();

        return new AbstractMap.SimpleEntry<>(cacheLookupKey, enclaveLookupKey);
    }
    
    /**
    * Creates a copy of the enclave keys to be sent to the crypto cache. This allows reconnection when table information
    * has changed but session, and enclave information, is the same.
    * 
    * @param keysToBeCached
    *        The keys sent to the cryptocache
    * @return A copy of the enclave keys, stored in the cryptoCache
    */
    private Map<Integer, CekTableEntry> copyEnclaveKeys(Map<Integer, CekTableEntry> keysToBeCached) {
        Map<Integer, CekTableEntry> enclaveKeys = new HashMap<Integer, CekTableEntry>();
        for (Map.Entry<Integer, CekTableEntry> entry : keysToBeCached.entrySet())
        {
            int ordinal = entry.getKey();
            CekTableEntry original = entry.getValue();
            CekTableEntry copy = new CekTableEntry(ordinal);
            for (EncryptionKeyInfo cekInfo : original.columnEncryptionKeyValues)
            {
                copy.add(cekInfo.encryptedKey, cekInfo.databaseId, cekInfo.cekId, cekInfo.cekVersion,
                        cekInfo.cekMdVersion, cekInfo.keyPath, cekInfo.keyStoreName, cekInfo.algorithmName);
            }
            enclaveKeys.put(ordinal, copy);
        }
        return enclaveKeys;
    }
}


/**
 * Represents a cache of all queries for a given enclave session.
 */
class CryptoCache {
    /**
     * The cryptocache stores both result sets returned from sp_describe_parameter_encryption calls. CEK data in cekMap,
     * and parameter data in paramMap.
     */
    private final ConcurrentHashMap<String, Map<Integer, CekTableEntry>> cekMap = new ConcurrentHashMap<>(16);
    private ConcurrentHashMap<String, ConcurrentHashMap<String, CryptoMetadata>> paramMap = new ConcurrentHashMap<>(16);

    ConcurrentHashMap<String, ConcurrentHashMap<String, CryptoMetadata>> getParamMap() {
        return paramMap;
    }

    void replaceParamMap(ConcurrentHashMap<String, ConcurrentHashMap<String, CryptoMetadata>> newMap) {
        paramMap = newMap;
    }

    Map<Integer, CekTableEntry> getEnclaveEntry(String enclaveLookupKey) {
        return cekMap.get(enclaveLookupKey);
    }

    ConcurrentHashMap<String, CryptoMetadata> getCacheEntry(String cacheLookupKey) {
        return paramMap.get(cacheLookupKey);
    }
    
    void addEnclaveEntry(String key, Map<Integer, CekTableEntry> value) {
        cekMap.put(key, value);
    }

    void addParamEntry(String key, ConcurrentHashMap<String, CryptoMetadata> value) {
        paramMap.put(key, value);
    }

    void removeParamEntry(String cacheLookupKey) {
        paramMap.remove(cacheLookupKey);
    }
}
