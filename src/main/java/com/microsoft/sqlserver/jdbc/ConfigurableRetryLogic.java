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
import java.nio.file.Files;
import java.nio.file.Paths;
import java.text.MessageFormat;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;


/**
 * Allows configurable statement retry through the use of the 'retryExec' connection property. Each rule read in is
 * converted to ConfigRetryRule objects, which are stored and referenced during statement retry.
 */
public class ConfigurableRetryLogic {
    private static final int INTERVAL_BETWEEN_READS_IN_MS = 30000;
    private static final String DEFAULT_PROPS_FILE = "mssql-jdbc.properties";
    private static final Lock CRL_LOCK = new ReentrantLock();
    private static final java.util.logging.Logger CONFIGURABLE_RETRY_LOGGER = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ConfigurableRetryLogic");
    private static final String SEMI_COLON = ";";
    private static final String COMMA = ",";
    private static final String EQUALS_SIGN = "=";
    private static final String RETRY_EXEC = "retryExec";
    private static final String RETRY_CONN = "retryConn";
    private static final String STATEMENT = "statement";
    private static boolean replaceFlag = false; // Are we replacing the list of transient errors?
    /**
     * The time the properties file was last modified.
     */
    private static final AtomicLong timeLastModified = new AtomicLong(0);
    /**
     * The time we last read the properties file.
     */
    private static final AtomicLong timeLastRead = new AtomicLong(0);
    /**
     * The last query executed (used when rule is process-dependent).
     */
    private static final AtomicReference<String> lastQuery = new AtomicReference<>("");
    /**
     * The previously read statement rules from the connection string.
     */
    private static final AtomicReference<String> prevStmtRulesFromConnString = new AtomicReference<>("");
    /**
     * The previously read connection rules from the connection string.
     */
    private static final AtomicReference<String> prevConnRulesFromConnString = new AtomicReference<>("");
    /**
     * The list of statement retry rules.
     */
    private static final AtomicReference<HashMap<Integer, ConfigurableRetryRule>> stmtRules = new AtomicReference<>(
            new HashMap<>());
    /**
     * The list of connection retry rules.
     */
    private static final AtomicReference<HashMap<Integer, ConfigurableRetryRule>> connRules = new AtomicReference<>(
            new HashMap<>());
    private static ConfigurableRetryLogic singleInstance;

    /**
     * Constructs the ConfigurableRetryLogic object reading rules from available sources.
     *
     * @throws SQLServerException
     *         if unable to construct
     */
    private ConfigurableRetryLogic() throws SQLServerException {
        timeLastRead.compareAndSet(0, new Date().getTime());
        setUpStatementRules(null);
        setUpConnectionRules(null);
    }

    /**
     * Fetches the static instance of ConfigurableRetryLogic, instantiating it if it hasn't already been. Each time the
     * instance is fetched, we check if a re-read is needed, and do so if properties should be re-read.
     *
     * @return the static instance of ConfigurableRetryLogic
     * @throws SQLServerException
     *         an exception
     */
    public static ConfigurableRetryLogic getInstance() throws SQLServerException {
        if (singleInstance == null) {
            CRL_LOCK.lock();
            try {
                if (singleInstance == null) {
                    singleInstance = new ConfigurableRetryLogic();
                } else {
                    refreshRuleSet();
                }
            } finally {
                CRL_LOCK.unlock();
            }
        } else {
            refreshRuleSet();
        }

        return singleInstance;
    }

    /**
     * If it has been INTERVAL_BETWEEN_READS_IN_MS (30 secs) since last read, see if we last did a file read, if so
     * only reread if the file has been modified. If no file read, set up rules using the previous connection
     * string (statement and connection) rules
     *
     * @throws SQLServerException
     *         when an exception occurs
     */
    private static void refreshRuleSet() throws SQLServerException {
        long currentTime = new Date().getTime();

        if ((currentTime - timeLastRead.get()) >= INTERVAL_BETWEEN_READS_IN_MS) {
            timeLastRead.set(currentTime);
            if (timeLastModified.get() != 0) {
                // If timeLastModified is set, we previously read from file, so we setUpRules also reading from file
                File f = new File(getCurrentClassPath());
                if (f.lastModified() != timeLastModified.get()) {
                    setUpStatementRules(null);
                    setUpConnectionRules(null);
                }
            } else {
                setUpStatementRules(prevStmtRulesFromConnString.get());
                setUpConnectionRules(prevConnRulesFromConnString.get());
            }
        }
    }

    /**
     * Sets statement rules given from connection string.
     *
     * @param newRules
     *        the new rules to use
     * @throws SQLServerException
     *         when an exception occurs
     */
    void setStatementRulesFromConnectionString(String newRules) throws SQLServerException {
        prevStmtRulesFromConnString.set(newRules);
        setUpStatementRules(prevStmtRulesFromConnString.get());
    }

    /**
     * Sets connection rules given from connection string.
     *
     * @param newRules
     *        the new rules to use
     * @throws SQLServerException
     *         when an exception occurs
     */
    void setConnectionRulesFromConnectionString(String newRules) throws SQLServerException {
        prevConnRulesFromConnString.set(newRules);
        setUpConnectionRules(prevConnRulesFromConnString.get());
    }

    /**
     * Stores last query executed.
     *
     * @param newQueryToStore
     *        the new query to store
     */
    void storeLastQuery(String newQueryToStore) {
        lastQuery.set(newQueryToStore.toLowerCase());
    }

    /**
     * Gets last query.
     *
     * @return the last query
     */
    String getLastQuery() {
        return lastQuery.get();
    }

    /**
     * Sets up rules based on either connection string option or file read.
     *
     * @param cxnStrRules
     *        if null, rules are constructed from file, else, this parameter is used to construct rules
     * @throws SQLServerException
     *         if an exception occurs
     */
    private static void setUpStatementRules(String cxnStrRules) throws SQLServerException {
        LinkedList<String> temp;

        stmtRules.set(new HashMap<>());
        lastQuery.set("");

        if (cxnStrRules == null || cxnStrRules.isEmpty()) {
            temp = readFromFile(RETRY_EXEC);
        } else {
            temp = new LinkedList<>();
            Collections.addAll(temp, cxnStrRules.split(SEMI_COLON));
        }
        createStatementRules(temp);
    }

    private static void setUpConnectionRules(String cxnStrRules) throws SQLServerException {
        LinkedList<String> temp;

        connRules.set(new HashMap<>());
        lastQuery.set("");

        if (cxnStrRules == null || cxnStrRules.isEmpty()) {
            temp = readFromFile(RETRY_CONN);
        } else {
            temp = new LinkedList<>();
            Collections.addAll(temp, cxnStrRules.split(SEMI_COLON));
        }
        createConnectionRules(temp);
    }

    /**
     * Creates and stores rules based on the inputted list of rules.
     *
     * @param listOfRules
     *        the list of rules, as a String LinkedList
     * @throws SQLServerException
     *         if unable to create rules from the inputted list
     */
    private static void createStatementRules(LinkedList<String> listOfRules) throws SQLServerException {
        stmtRules.set(new HashMap<>());

        for (String potentialRule : listOfRules) {
            ConfigurableRetryRule rule = new ConfigurableRetryRule(potentialRule);

            if (rule.getError().contains(COMMA)) {
                String[] arr = rule.getError().split(COMMA);

                for (String retryError : arr) {
                    ConfigurableRetryRule splitRule = new ConfigurableRetryRule(retryError, rule);
                    stmtRules.get().put(Integer.parseInt(splitRule.getError()), splitRule);
                }
            } else {
                stmtRules.get().put(Integer.parseInt(rule.getError()), rule);
            }
        }
    }

    private static void createConnectionRules(LinkedList<String> listOfRules) throws SQLServerException {
        connRules.set(new HashMap<>());
        replaceFlag = false;

        for (String potentialRule : listOfRules) {
            ConfigurableRetryRule rule = new ConfigurableRetryRule(potentialRule);
            if (rule.replaceExisting) {
                replaceFlag = true;
            }

            if (rule.getError().contains(COMMA)) {
                String[] arr = rule.getError().split(COMMA);

                for (String retryError : arr) {
                    ConfigurableRetryRule splitRule = new ConfigurableRetryRule(retryError, rule);
                    connRules.get().put(Integer.parseInt(splitRule.getError()), splitRule);
                }
            } else {
                connRules.get().put(Integer.parseInt(rule.getError()), rule);
            }
        }
    }

    /**
     * Gets the current class path (for use in file reading).
     *
     * @return the current class path, as a String
     * @throws SQLServerException
     *         if unable to retrieve the current class path
     */
    private static String getCurrentClassPath() throws SQLServerException {
        String location = "";
        String className = "";

        try {
            className = new Object() {}.getClass().getEnclosingClass().getName();
            location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();

            if (Files.isDirectory(Paths
                    .get(ConfigurableRetryLogic.class.getProtectionDomain().getCodeSource().getLocation().toURI()))) {
                // We check if the Path we get from the CodeSource location is a directory. If so, we are running
                // from class files and should remove a suffix (i.e. the props file is in a different location from the
                // location returned)
                location = location.substring(0, location.length() - ("target/classes/").length());
            }

            return new URI(location).getPath() + DEFAULT_PROPS_FILE; // TODO: Allow custom paths
        } catch (URISyntaxException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_URLInvalid"));
            Object[] msgArgs = {location};
            throw new SQLServerException(form.format(msgArgs), null, 0, e);
        } catch (ClassNotFoundException e) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_UnableToFindClass"));
            Object[] msgArgs = {className};
            throw new SQLServerException(form.format(msgArgs), null, 0, e);
        }
    }

    /**
     * Attempts to read rules from the properties file.
     *
     * @return the list of rules as a LinkedList<String>
     * @throws SQLServerException
     *         if unable to read from the file
     */
    private static LinkedList<String> readFromFile(String connectionStringProperty) throws SQLServerException {
        String filePath = getCurrentClassPath();
        LinkedList<String> list = new LinkedList<>();

        try {
            File f = new File(filePath);
            try (BufferedReader buffer = new BufferedReader(new FileReader(f))) {
                String readLine;
                while ((readLine = buffer.readLine()) != null) {
                    if (readLine.startsWith(connectionStringProperty)) { // Either "retryExec" or "retryConn"
                        String value = readLine.split(EQUALS_SIGN)[1];
                        Collections.addAll(list, value.split(SEMI_COLON));
                    }
                }
            }
            timeLastModified.set(f.lastModified());
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

    /**
     * Searches rule set for the given rule.
     *
     * @param ruleToSearchFor
     *        the rule to search for
     * @return the configurable retry rule
     * @throws SQLServerException
     *         when an exception occurs
     */
    ConfigurableRetryRule searchRuleSet(int ruleToSearchFor, String ruleSet) throws SQLServerException {
        refreshRuleSet();
        if (ruleSet.equals(STATEMENT)) {
            for (Map.Entry<Integer, ConfigurableRetryRule> entry : stmtRules.get().entrySet()) {
                if (entry.getKey() == ruleToSearchFor) {
                    return entry.getValue();
                }
            }
        } else {
            for (Map.Entry<Integer, ConfigurableRetryRule> entry : connRules.get().entrySet()) {
                if (entry.getKey() == ruleToSearchFor) {
                    return entry.getValue();
                }
            }
        }
        return null;
    }

    boolean getReplaceFlag() {
        return replaceFlag;
    }
}
