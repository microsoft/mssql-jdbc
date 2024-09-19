/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


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
    private static ConfigurableRetryLogic singleInstance = null;
    private static long timeLastModified;
    private static long timeLastRead;
    private static String lastQuery = ""; // The last query executed (used when rule is process-dependent)
    private static String prevRulesFromConnectionString = "";
    private static HashMap<Integer, ConfigurableRetryRule> stmtRules = new HashMap<>();
    private static final String SEMI_COLON = ";";
    private static final String COMMA = ",";
    private static final String FORWARD_SLASH = "/";
    private static final String EQUALS_SIGN = "=";
    private static final String RETRY_EXEC = "retryExec";

    private ConfigurableRetryLogic() throws SQLServerException {
        timeLastRead = new Date().getTime();
        setUpRules(null);
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
        // No need for lock; static initializer singleInstance is thread-safe
        if (singleInstance == null) {
            singleInstance = new ConfigurableRetryLogic();
        } else {
            refreshRuleSet();
        }
        return singleInstance;
    }

    /**
     * If it has been INTERVAL_BETWEEN_READS_IN_MS (30 secs) since last read, see if we last did a file read, if so
     * only reread if the file has been modified. If no file read, set up rules using the prev. connection string rules.
     *
     * @throws SQLServerException
     *         when an exception occurs
     */
    private static void refreshRuleSet() throws SQLServerException {
        long currentTime = new Date().getTime();
        if ((currentTime - timeLastRead) >= INTERVAL_BETWEEN_READS_IN_MS) {
            timeLastRead = currentTime;
            if (timeLastModified != 0) {
                // If timeLastModified has been set, we have previously read from a file, so we setUpRules
                // reading from file
                File f = new File(getCurrentClassPath());
                if (f.lastModified() != timeLastModified) {
                    setUpRules(null);
                }
            } else {
                setUpRules(prevRulesFromConnectionString);
            }
        }
    }

    void setFromConnectionString(String newRules) throws SQLServerException {
        prevRulesFromConnectionString = newRules;
        setUpRules(prevRulesFromConnectionString);
    }

    void storeLastQuery(String newQueryToStore) {
        lastQuery = newQueryToStore.toLowerCase();
    }

    String getLastQuery() {
        return lastQuery;
    }

    /**
     * Sets up rules based on either connection string option or file read.
     *
     * @param cxnStrRules
     *        If null, rules are constructed from file, else, this parameter is used to construct rules
     * @throws SQLServerException
     *         If an exception occurs
     */
    private static void setUpRules(String cxnStrRules) throws SQLServerException {
        stmtRules = new HashMap<>();
        lastQuery = "";
        LinkedList<String> temp;

        if (cxnStrRules == null || cxnStrRules.isEmpty()) {
            temp = readFromFile();
        } else {
            temp = new LinkedList<>();
            Collections.addAll(temp, cxnStrRules.split(SEMI_COLON));
        }
        createRules(temp);
    }

    private static void createRules(LinkedList<String> listOfRules) throws SQLServerException {
        stmtRules = new HashMap<>();

        for (String potentialRule : listOfRules) {
            ConfigurableRetryRule rule = new ConfigurableRetryRule(potentialRule);

            if (rule.getError().contains(COMMA)) {
                String[] arr = rule.getError().split(COMMA);

                for (String retryError : arr) {
                    ConfigurableRetryRule splitRule = new ConfigurableRetryRule(retryError, rule);
                    stmtRules.put(Integer.parseInt(splitRule.getError()), splitRule);
                }
            } else {
                stmtRules.put(Integer.parseInt(rule.getError()), rule);
            }
        }
    }

    private static String getCurrentClassPath() throws SQLServerException {
        String location = "";
        String className = "";

        try {
            className = new Object() {}.getClass().getEnclosingClass().getName();
            location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();
            location = location.substring(0, location.length() - 16);
            URI uri = new URI(location + FORWARD_SLASH);
            return uri.getPath() + DEFAULT_PROPS_FILE; // For now, we only allow "mssql-jdbc.properties" as file name.
        } catch (URISyntaxException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_AKVURLInvalid"));
            Object[] msgArgs = {location + FORWARD_SLASH};
            throw new SQLServerException(form.format(msgArgs), null, 0, e);
        } catch (ClassNotFoundException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnableToFindClass"));
            Object[] msgArgs = {className};
            throw new SQLServerException(form.format(msgArgs), null, 0, e);
        }
    }

    private static LinkedList<String> readFromFile() throws SQLServerException {
        String filePath = getCurrentClassPath();
        LinkedList<String> list = new LinkedList<>();

        try {
            File f = new File(filePath);
            try (BufferedReader buffer = new BufferedReader(new FileReader(f))) {
                String readLine;
                while ((readLine = buffer.readLine()) != null) {
                    if (readLine.startsWith(RETRY_EXEC)) {
                        String value = readLine.split(EQUALS_SIGN)[1];
                        Collections.addAll(list, value.split(SEMI_COLON));
                    }
                }
            }
            timeLastModified = f.lastModified();
        } catch (FileNotFoundException e) {
            // If the file is not found either A) We're not using CRL OR B) the path is wrong. Do not error out, instead
            // log a message.
            if (CONFIGURABLE_RETRY_LOGGER.isLoggable(java.util.logging.Level.FINER)) {
                CONFIGURABLE_RETRY_LOGGER.finest("File not found at path - \"" + filePath + "\"");
            }
        } catch (IOException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
            Object[] msgArgs = {e.getMessage() + ", from path - \"" + filePath + "\""};
            throw new SQLServerException(form.format(msgArgs), null, 0, e);
        }
        return list;
    }

    ConfigurableRetryRule searchRuleSet(int ruleToSearchFor) throws SQLServerException {
        refreshRuleSet();
        for (Map.Entry<Integer, ConfigurableRetryRule> entry : stmtRules.entrySet()) {
            if (entry.getKey() == ruleToSearchFor) {
                return entry.getValue();
            }
        }
        return null;
    }
}
