/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allows configurable statement retry through the use of the 'retryExec' connection property. Each rule read in is
 * converted to ConfigRetryRule objects, which are stored and referenced during statement retry.
 *
 */
public class ConfigurableRetryLogic {
    private final static int INTERVAL_BETWEEN_READS_IN_MS = 30000;
    private final static String DEFAULT_PROPS_FILE = "mssql-jdbc.properties";
    private static final java.util.logging.Logger CONFIGURABLE_RETRY_LOGGER = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ConfigurableRetryLogic");
    private static ConfigurableRetryLogic driverInstance = null;
    private static long timeLastModified;
    private static long timeLastRead;
    private static String lastQuery = ""; // The last query executed (used when rule is process-dependent)
    private static String newRulesFromConnectionString = ""; // Are their new rules to read?
    private static String lastRulesFromConnectionString = ""; // Have we read from conn string in the past?
    private static HashMap<Integer, ConfigRetryRule> stmtRules = new HashMap<>();
    private static final Lock CRL_LOCK = new ReentrantLock();
    private static final String SEMI_COLON = ";";
    private static final String COMMA = ",";
    private static final String FORWARD_SLASH = "/";
    private static final String EQUALS_SIGN = "=";
    private static final String RETRY_EXEC = "retryExec";

    private ConfigurableRetryLogic() throws SQLServerException {
        timeLastRead = new Date().getTime();
        setUpRules();
    }

    /**
     * Fetches the static instance of ConfigurableRetryLogic, instantiating it if it hasn't already been.
     * Each time the instance is fetched, we check if a re-read is needed, and do so if properties should be re-read.
     *
     * @return The static instance of ConfigurableRetryLogic
     * @throws SQLServerException
     *         an exception
     */
    public static ConfigurableRetryLogic getInstance() throws SQLServerException {
        if (driverInstance == null) {
            CRL_LOCK.lock();
            try {
                if (driverInstance == null) {
                    driverInstance = new ConfigurableRetryLogic();
                } else {
                    refreshRuleSet();
                }
            } finally {
                CRL_LOCK.unlock();
            }
        } else {
            refreshRuleSet();
        }
        return driverInstance;
    }

    /**
     * If it has been INTERVAL_BETWEEN_READS_IN_MS (30 secs) since last read and EITHER, we're using connection string
     * props OR the file contents have been updated, reread.
     *
     * @throws SQLServerException
     *         when an exception occurs
     */
    private static void refreshRuleSet() throws SQLServerException {
        long currentTime = new Date().getTime();
        if ((currentTime - timeLastRead) >= INTERVAL_BETWEEN_READS_IN_MS
                && ((!lastRulesFromConnectionString.isEmpty()) || rulesHaveBeenChanged())) {
            timeLastRead = currentTime;
            setUpRules();
        }
    }

    private static boolean rulesHaveBeenChanged() {
        String inputToUse = getCurrentClassPath() + DEFAULT_PROPS_FILE;

        try {
            File f = new File(inputToUse);
            return f.lastModified() != timeLastModified;
        } catch (Exception e) {
            return true;
        }
    }

    void setFromConnectionString(String custom) throws SQLServerException {
        newRulesFromConnectionString = custom;
        setUpRules();
    }

    void storeLastQuery(String sql) {
        lastQuery = sql.toLowerCase();
    }

    String getLastQuery() {
        return lastQuery;
    }

    private static void setUpRules() throws SQLServerException {
        stmtRules = new HashMap<>();
        lastQuery = "";
        LinkedList<String> temp;

        if (!newRulesFromConnectionString.isEmpty()) {
            temp = new LinkedList<>();
            Collections.addAll(temp, newRulesFromConnectionString.split(SEMI_COLON));
            lastRulesFromConnectionString = newRulesFromConnectionString;
            newRulesFromConnectionString = "";
        } else {
            temp = readFromFile();
        }
        createRules(temp);
    }

    private static void createRules(LinkedList<String> listOfRules) throws SQLServerException {
        stmtRules = new HashMap<>();

        for (String potentialRule : listOfRules) {
            ConfigRetryRule rule = new ConfigRetryRule(potentialRule);

            if (rule.getError().contains(COMMA)) {
                String[] arr = rule.getError().split(COMMA);

                for (String retryError : arr) {
                    ConfigRetryRule splitRule = new ConfigRetryRule(retryError, rule);
                    stmtRules.put(Integer.parseInt(splitRule.getError()), splitRule);
                }
            } else {
                stmtRules.put(Integer.parseInt(rule.getError()), rule);
            }
        }
    }

    private static String getCurrentClassPath() {
        try {
            String className = new Object() {}.getClass().getEnclosingClass().getName();
            String location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();
            location = location.substring(0, location.length() - 16);
            URI uri = new URI(location + FORWARD_SLASH);
            return uri.getPath();
        } catch (Exception e) {
            if (CONFIGURABLE_RETRY_LOGGER.isLoggable(java.util.logging.Level.FINEST)) {
                CONFIGURABLE_RETRY_LOGGER.finest("Unable to get current class path for properties file reading.");
            }
        }
        return null;
    }

    private static LinkedList<String> readFromFile() {
        String filePath = getCurrentClassPath();
        LinkedList<String> list = new LinkedList<>();

        try {
            File f = new File(filePath + DEFAULT_PROPS_FILE);
            timeLastModified = f.lastModified();
            try (BufferedReader buffer = new BufferedReader(new FileReader(f))) {
                String readLine;
                while ((readLine = buffer.readLine()) != null) {
                    if (readLine.startsWith(RETRY_EXEC)) {
                        String value = readLine.split(EQUALS_SIGN)[1];
                        Collections.addAll(list, value.split(SEMI_COLON));
                    }
                }
            }
        } catch (IOException e) {
            if (CONFIGURABLE_RETRY_LOGGER.isLoggable(java.util.logging.Level.FINEST)) {
                CONFIGURABLE_RETRY_LOGGER.finest("No properties file exists or file is badly formatted.");
            }
        }
        return list;
    }

    ConfigRetryRule searchRuleSet(int ruleToSearch) throws SQLServerException {
        refreshRuleSet();
        for (Map.Entry<Integer, ConfigRetryRule> entry : stmtRules.entrySet()) {
            if (entry.getKey() == ruleToSearch) {
                return entry.getValue();
            }
        }
        return null;
    }
}
