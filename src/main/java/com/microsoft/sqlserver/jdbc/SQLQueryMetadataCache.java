/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


/**
 * Implements a cache for query metadata returned from sp_describe_parameter_encryption calls. Adding, removing, and
 * reading from the cache is handled here, with the location of the cache being in the EnclaveSession.
 * 
 */

class SQLQueryMetadataCache {

    final static int cacheSize = 2000; // Size of the cache in number of entries
    final static int cacheTrimThreshold = 300; // Threshold above which to trim the cache

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
    public static boolean getQueryMetadataIfExists(Parameter[] params, ArrayList<String> parameterNames,
            EnclaveSession session, SQLServerConnection connection, SQLServerStatement stmt) {

        // Caching is enabled if column encryption is enabled, return false if it's not
        if (connection.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString()).equalsIgnoreCase("Disabled")) {
            return false;
        }

        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeysFromSqlCommand(stmt, connection);
        HashMap<String, CryptoMetadata> metadataMap = session.getCryptoCache().getEntry(encryptionValues.getKey());

        if (metadataMap == null) {
            return false;
        }

        // Iterate over all parameters and get the metadata
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
                    // Try to get the encryption key. If the key information is stale, this might fail.
                    // In this case, just fail the cache lookup.
                    try {
                        SQLServerSecurityUtility.decryptSymmetricKey(cryptoCopy, connection, stmt);
                    } catch (SQLServerException e) {

                        removeCacheEntry(stmt, session, connection);

                        for (Parameter paramToCleanup : params) {
                            paramToCleanup.cryptoMeta = null;
                        }

                        return false;
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }

        }

        return true;
    }

    // Add the metadata for a specific query to the cache.
    public static boolean addQueryMetadata(Parameter[] params, ArrayList<String> parameterNames, EnclaveSession session,
            SQLServerConnection connection, SQLServerStatement stmt) {

        // Caching is enabled if column encryption is enabled, return false if it's not
        if (connection.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.COLUMN_ENCRYPTION.toString()).equalsIgnoreCase("Disabled")) {
            return false;
        }

        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeysFromSqlCommand(stmt, connection);
        if (encryptionValues.getKey() == null) {
            return false;
        }

        HashMap<String, CryptoMetadata> metadataMap = new HashMap<>(params.length);

        // Create a copy of the cypherMetadata that doesn't have the algorithm
        for (int i = 0; i < params.length; i++) {
            try {
                CryptoMetadata cryptoCopy = null;
                CryptoMetadata metaData = params[i].getCryptoMetadata();
                if (metaData != null) {

                    cryptoCopy = new CryptoMetadata(metaData.getCekTableEntry(), metaData.getOrdinal(),
                            metaData.getEncryptionAlgorithmId(), metaData.getEncryptionAlgorithmName(),
                            metaData.getEncryptionType().getValue(), metaData.getNormalizationRuleVersion());
                }
                // Cached cipher MD should never have an initialized algorithm since this would contain the key.
                if (cryptoCopy != null && !cryptoCopy.isAlgorithmInitialized()) {
                    String paramName = parameterNames.get(i);
                    metadataMap.put(paramName, cryptoCopy);
                } else {
                    return false;
                }
            } catch (SQLServerException e) {
                e.printStackTrace();
            }
        }

        // If the size of the cache exceeds the threshold, set that we are in trimming and trim the cache accordingly.
        int cacheSizeCurrent = session.getCryptoCache().getMap().size();
        if (cacheSizeCurrent > cacheSize + cacheTrimThreshold) {
            try {
                int entriesToRemove = cacheSizeCurrent - cacheSize;
                HashMap<String, HashMap<String, CryptoMetadata>> newMap = new HashMap<>();
                HashMap<String, HashMap<String, CryptoMetadata>> oldMap = session.getCryptoCache().getMap();
                int count = 0;

                for (Map.Entry<String, HashMap<String, CryptoMetadata>> entry : oldMap.entrySet()) {
                    if (count >= entriesToRemove) {
                        newMap.put(entry.getKey(), entry.getValue());
                    }
                    count++;
                }

            } catch (Exception e) {
                e.printStackTrace();
            }
        }

        return true;
    }

    public static void removeCacheEntry(SQLServerStatement stmt, EnclaveSession session,
            SQLServerConnection connection) {
        AbstractMap.SimpleEntry<String, String> encryptionValues = getCacheLookupKeysFromSqlCommand(stmt, connection);
        if (encryptionValues.getKey() == null) {
            return;
        }

        session.getCryptoCache().remove(encryptionValues.getKey());
    }

    private static AbstractMap.SimpleEntry<String, String> getCacheLookupKeysFromSqlCommand(
            SQLServerStatement statement, SQLServerConnection connection) {
        final int sqlIdentifierLength = 128;

        // Return null if we have no connection.
        if (connection == null) {
            return new AbstractMap.SimpleEntry<>(null, null);
        }

        StringBuilder cacheLookupKeyBuilder = new StringBuilder();
        cacheLookupKeyBuilder.append(":::");
        // Pad database name to 128 characters to avoid any false cache matches because of weird DB names.
        String databaseName = connection.activeConnectionProperties
                .getProperty(SQLServerDriverStringProperty.DATABASE_NAME.toString());
        cacheLookupKeyBuilder.append(databaseName);
        for (int i = databaseName.length() - 1; i < sqlIdentifierLength; ++i) {
            cacheLookupKeyBuilder.append(" ");
        }
        cacheLookupKeyBuilder.append(":::");
        cacheLookupKeyBuilder.append(statement.toString());

        String cacheLookupKey = cacheLookupKeyBuilder.toString();
        String enclaveLookupKey = cacheLookupKeyBuilder.append(":::enclaveKeys").toString();

        return new AbstractMap.SimpleEntry<>(cacheLookupKey, enclaveLookupKey);
    }
}
