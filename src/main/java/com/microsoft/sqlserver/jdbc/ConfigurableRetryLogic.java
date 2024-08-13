/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.io.*;
import java.net.URI;
import java.net.URISyntaxException;
import java.text.MessageFormat;
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
    private static ConfigurableRetryLogic driverInstance = null;
    private static long timeLastModified;
    private static long timeLastRead;
    private static String lastQuery = ""; // The last query executed (used when rule is process-dependent)
    private static String prevRulesFromConnectionString = "";
    private static HashMap<Integer, ConfigRetryRule> stmtRules = new HashMap<>();
    private static final Lock CRL_LOCK = new ReentrantLock();
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
     * If it has been INTERVAL_BETWEEN_READS_IN_MS (30 secs) since last read, see if we last did a file read, if so
     * only reread if the file has been modified. If no file read, set up rules using the prev. connection string rules.
     *
     * @throws SQLServerException
     *         when an exception occurs
     */
    private static void refreshRuleSet() throws SQLServerException {
        long currentTime = new Date().getTime();
        if ((currentTime - timeLastRead) >= INTERVAL_BETWEEN_READS_IN_MS) {
            // If it has been 30 secs, reread
            timeLastRead = currentTime;
            if (timeLastModified != 0 && rulesHaveBeenChanged()) {
                // If timeLastModified has been set, we have previously read from a file
                setUpRules(null);
            } else {
                setUpRules(prevRulesFromConnectionString);
            }
        }
    }

    private static boolean rulesHaveBeenChanged() throws SQLServerException {
        String inputToUse = getCurrentClassPath() + DEFAULT_PROPS_FILE;

        try {
            File f = new File(inputToUse);
            return f.lastModified() != timeLastModified;
        } catch (Exception e) {
            return true;
        }
    }

    void setFromConnectionString(String custom) throws SQLServerException {
        prevRulesFromConnectionString = custom;
        setUpRules(prevRulesFromConnectionString);
    }

    void storeLastQuery(String sql) {
        lastQuery = sql.toLowerCase();
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

    private static String getCurrentClassPath() throws SQLServerException {
        String location = "";
        String className = "";

        try {
            className = new Object() {}.getClass().getEnclosingClass().getName();
            location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();
            location = location.substring(0, location.length() - 16);
            URI uri = new URI(location + FORWARD_SLASH);
            return uri.getPath();
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
            File f = new File(filePath + DEFAULT_PROPS_FILE);
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
            throw new SQLServerException(SQLServerException.getErrString("R_PropertiesFileNotFound"), null, 0, null);
        } catch (IOException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
            Object[] msgArgs = {e.toString()};
            throw new SQLServerException(form.format(msgArgs), null, 0, e);
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
