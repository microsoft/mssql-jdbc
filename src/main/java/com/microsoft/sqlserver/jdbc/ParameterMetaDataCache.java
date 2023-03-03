/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Map;

import mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap;
import mssql.googlecode.concurrentlinkedhashmap.ConcurrentLinkedHashMap.Builder;


/**
 * Implements a cache for query metadata returned from sp_describe_parameter_encryption calls. Adding, removing, and
 * reading from the cache is handled here, with the location of the cache being in the EnclaveSession.
 * 
 */
class ParameterMetaDataCache {

    private ParameterMetaDataCache() {
        throw new UnsupportedOperationException(SQLServerException.getErrString("R_notSupported"));
    }

    static final int CACHE_SIZE = 2000; // Size of the cache in number of entries
    static final int MAX_WEIGHTED_CAPACITY = 2300; // Size of cache + threshold, above which we trim.
    static CryptoCache cache = new CryptoCache();
    static private java.util.logging.Logger metadataCacheLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ParameterMetaDataCache");

    /**
     * Retrieves the metadata from the cache, should it exist.
     * 
     * @param params
     *        Array of parameters used
     * @param parameterNames
     *        Names of parameters used
     * @param connection
     *        The SQLServer connection
     * @param stmt
     *        The SQLServer statement, whose returned metadata we're checking
     * @param userSql
     *        The query executed by the user
     * @return true, if the metadata for the query can be retrieved
     */
    static boolean getQueryMetadata(Parameter[] params, ArrayList<String> parameterNames,
            SQLServerConnection connection, SQLServerStatement stmt, String userSql) throws SQLServerException {

        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeys(connection, userSql);
        ConcurrentLinkedHashMap<String, CryptoMetadata> metadataMap = cache.getCacheEntry(encryptionValues.getKey());

        if (metadataMap == null) {
            if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                metadataCacheLogger.finest("Cache Miss. Unable to retrieve cache entry from cache.");
            }
            return false;
        }

        for (int i = 0; i < params.length; i++) {
            CryptoMetadata foundData = metadataMap.get(parameterNames.get(i));

            /*
             * A parameter could be missing, this means it uses plaintext encryption. A warning is logged in this case.
             * If data is found with an initialized algorithm, all metadata is cleared, as this should never be the
             * case.
             */
            if (!metadataMap.containsKey(parameterNames.get(i))
                    && metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                metadataCacheLogger.finest("Parameter uses Plaintext (type 0) encryption.");
            }
            if (foundData != null && foundData.isAlgorithmInitialized()) {
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

                        removeCacheEntry(connection, userSql);

                        for (Parameter paramToCleanup : params) {
                            paramToCleanup.cryptoMeta = null;
                        }

                        if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                            metadataCacheLogger.finest("Cache Miss. Unable to decrypt CEK.");
                        }
                        return false;
                    }
                }
            } catch (SQLServerException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CryptoCacheInaccessible"));
                Object[] msgArgs = {"R_unknownColumnEncryptionType", e.getMessage()};
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
     * @param connection
     *        SQLServerConnection
     * @param stmt
     *        SQLServer statement used to retrieve keys to find correct cache
     * @param cekList
     *        The list of CEKs (from the first RS) that is also added to the cache as well as parameter metadata
     * @param userSql
     *        The query executed by the user
     * @return true, if the query metadata has been added correctly
     */
    static boolean addQueryMetadata(Parameter[] params, ArrayList<String> parameterNames,
            SQLServerConnection connection, SQLServerStatement stmt, String userSql) throws SQLServerException {

        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeys(connection, userSql);
        if (encryptionValues.getKey() == null) {
            return false;
        }

        ConcurrentLinkedHashMap<String, CryptoMetadata> metadataMap = new Builder<String, CryptoMetadata>()
                .maximumWeightedCapacity(params.length).build();

        for (int i = 0; i < params.length; i++) {
            try {
                CryptoMetadata cryptoCopy = null;
                CryptoMetadata metaData = params[i].getCryptoMetadata();
                if (metaData != null) {

                    cryptoCopy = new CryptoMetadata(metaData.getCekTableEntry(), metaData.getOrdinal(),
                            metaData.getEncryptionAlgorithmId(), metaData.getEncryptionAlgorithmName(),
                            metaData.getEncryptionType().getValue(), metaData.getNormalizationRuleVersion());
                    if (cryptoCopy.isAlgorithmInitialized()) {
                        return false;
                    }
                    String paramName = parameterNames.get(i);
                    metadataMap.put(paramName, cryptoCopy);
                }
            } catch (SQLServerException e) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_CryptoCacheInaccessible"));
                Object[] msgArgs = {"R_unknownColumnEncryptionType", e.getMessage()};
                throw new SQLServerException(form.format(msgArgs), null);
            }
        }

        // If the size of the cache exceeds the threshold, set that we are in trimming and trim the cache accordingly.
        int cacheSizeCurrent = cache.getParamMap().size();
        if (cacheSizeCurrent > MAX_WEIGHTED_CAPACITY) {
            int entriesToRemove = cacheSizeCurrent - CACHE_SIZE;
            ConcurrentLinkedHashMap<String, ConcurrentLinkedHashMap<String, CryptoMetadata>> map = cache.getParamMap();
            int count = 0;
            for (Map.Entry<String, ConcurrentLinkedHashMap<String, CryptoMetadata>> entry : map.entrySet()) {
                if (count < entriesToRemove) {
                    map.remove(entry.getKey(), entry.getValue());
                } else {
                    break;
                }
                count++;
            }

            if (metadataCacheLogger.isLoggable(java.util.logging.Level.FINEST)) {
                metadataCacheLogger.finest("Cache successfully trimmed.");
            }
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
     * @param connection
     *        The SQLServerConnection, also used to retrieve keys
     * @param userSql
     *        The query executed by the user
     */
    static void removeCacheEntry(SQLServerConnection connection, String userSql) {
        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeys(connection, userSql);
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
     * @param userSql
     *        The query executed by the user
     * @return A key value pair containing cache lookup key and enclave lookup key
     */
    private static AbstractMap.SimpleEntry<String, String> getCacheLookupKeys(SQLServerConnection connection,
            String userSql) {

        StringBuilder cacheLookupKeyBuilder = new StringBuilder();
        cacheLookupKeyBuilder.append(":::");
        String databaseName = connection.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString());
        cacheLookupKeyBuilder.append(databaseName);
        cacheLookupKeyBuilder.append(":::");
        cacheLookupKeyBuilder.append(userSql);

        String cacheLookupKey = cacheLookupKeyBuilder.toString();
        String enclaveLookupKey = cacheLookupKeyBuilder.append(":::enclaveKeys").toString();

        return new AbstractMap.SimpleEntry<>(cacheLookupKey, enclaveLookupKey);
    }
}


/**
 * Represents a cache of all queries for a given enclave session.
 */
class CryptoCache {
    /**
     * The cryptocache stores both result sets returned from sp_describe_parameter_encryption calls. Column Encryption
     * Key data in cekMap, and parameter data in paramMap.
     */
    static final int MAX_WEIGHTED_CAPACITY = 2300;
    private ConcurrentLinkedHashMap<String, ConcurrentLinkedHashMap<String, CryptoMetadata>> paramMap = new Builder<String, ConcurrentLinkedHashMap<String, CryptoMetadata>>()
            .maximumWeightedCapacity(MAX_WEIGHTED_CAPACITY).build();

    ConcurrentLinkedHashMap<String, ConcurrentLinkedHashMap<String, CryptoMetadata>> getParamMap() {
        return paramMap;
    }

    ConcurrentLinkedHashMap<String, CryptoMetadata> getCacheEntry(String cacheLookupKey) {
        return paramMap.get(cacheLookupKey);
    }

    void addParamEntry(String key, ConcurrentLinkedHashMap<String, CryptoMetadata> value) {
        paramMap.put(key, value);
    }

    void removeParamEntry(String cacheLookupKey) {
        paramMap.remove(cacheLookupKey);
    }
}
