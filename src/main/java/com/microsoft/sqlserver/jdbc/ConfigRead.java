package com.microsoft.sqlserver.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class ConfigRead {
    private final static int intervalBetweenReads = 30000; // How many ms must have elapsed before we re-read
    private final static String defaultPropsFile = "mssql-jdbc.properties";
    private static ConfigRead driverInstance = null;
    private static long timeLastModified;
    private static long lastTimeRead;
    private static String lastQuery = "";
    private static String customRetryRules = ""; // Rules imported from connection string
    private static boolean replaceFlag; // Are we replacing the list of transient errors (for connection retry)?
    private static HashMap<Integer, ConfigRetryRule> cxnRules = new HashMap<>();
    private static HashMap<Integer, ConfigRetryRule> stmtRules = new HashMap<>();

    private ConfigRead() throws SQLServerException {
        // On instantiation, set last time read and set up rules
        lastTimeRead = new Date().getTime();
        setUpRules();
    }

    /**
     * Fetches the static instance of ConfigRead, instantiating it if it hasn't already been.
     *
     * @return The static instance of ConfigRead
     * @throws SQLServerException
     *         an exception
     */
    public static synchronized ConfigRead getInstance() throws SQLServerException {
        // Every time we fetch this static instance, instantiate if it hasn't been. If it has then re-read and return
        // the instance.
        if (driverInstance == null) {
            driverInstance = new ConfigRead();
        } else {
            reread();
        }

        return driverInstance;
    }

    /**
     * Check if it's time to re-read, and if the file has changed. If so, then re-set up rules.
     *
     * @throws SQLServerException
     *         an exception
     */
    private static void reread() throws SQLServerException {
        long currentTime = new Date().getTime();

        if ((currentTime - lastTimeRead) >= intervalBetweenReads && !compareModified()) {
            lastTimeRead = currentTime;
            setUpRules();
        }
    }

    private static boolean compareModified() {
        String inputToUse = getCurrentClassPath() + defaultPropsFile;

        try {
            File f = new File(inputToUse);
            return f.lastModified() == timeLastModified;
        } catch (Exception e) {
            return false;
        }
    }

    public void setCustomRetryRules(String cRR) throws SQLServerException {
        customRetryRules = cRR;
        setUpRules();
    }

    public void setFromConnectionString(String custom) throws SQLServerException {
        if (!custom.isEmpty()) {
            setCustomRetryRules(custom);
        }
    }

    public void storeLastQuery(String sql) {
        lastQuery = sql.toLowerCase();
    }

    public String getLastQuery() {
        return lastQuery;
    }

    private static void setUpRules() throws SQLServerException {
        LinkedList<String> temp = null;

        if (!customRetryRules.isEmpty()) {
            // If user as set custom rules in connection string, then we use those over any file
            temp = new LinkedList<>();
            for (String s : customRetryRules.split(";")) {
                temp.add(s);
            }
        } else {
            try {
                temp = readFromFile();
            } catch (IOException e) {
                // TODO handle IO exception
            }
        }

        if (temp != null) {
            createRules(temp);
        }
    }

    private static void createRules(LinkedList<String> list) throws SQLServerException {
        cxnRules = new HashMap<>();
        stmtRules = new HashMap<>();

        for (String temp : list) {

            ConfigRetryRule rule = new ConfigRetryRule(temp);
            if (rule.getError().contains(",")) {

                String[] arr = rule.getError().split(",");

                for (String s : arr) {
                    ConfigRetryRule rulez = new ConfigRetryRule(s, rule);
                    if (rule.getConnectionStatus()) {
                        if (rule.getReplaceExisting()) {
                            cxnRules = new HashMap<>();
                            replaceFlag = true;
                        }
                        cxnRules.put(Integer.parseInt(rulez.getError()), rulez);
                    } else {
                        stmtRules.put(Integer.parseInt(rulez.getError()), rulez);
                    }
                }
            } else {
                if (rule.getConnectionStatus()) {
                    if (rule.getReplaceExisting()) {
                        cxnRules = new HashMap<>();
                        replaceFlag = true;
                    }
                    cxnRules.put(Integer.parseInt(rule.getError()), rule);
                } else {
                    stmtRules.put(Integer.parseInt(rule.getError()), rule);
                }
            }
        }
    }

    private static String getCurrentClassPath() {
        try {
            String className = new Object() {}.getClass().getEnclosingClass().getName();
            String location = Class.forName(className).getProtectionDomain().getCodeSource().getLocation().getPath();
            location = location.substring(0, location.length() - 16);
            URI uri = new URI(location + "/");
            return uri.getPath();
        } catch (Exception e) {
            // TODO handle exception
        }
        return null;
    }

    private static LinkedList<String> readFromFile() throws IOException {
        String filePath = getCurrentClassPath();

        LinkedList<String> list = new LinkedList<>();
        try {
            File f = new File(filePath + defaultPropsFile);
            timeLastModified = f.lastModified();
            try (BufferedReader buffer = new BufferedReader(new FileReader(f))) {
                String readLine;

                while ((readLine = buffer.readLine()) != null) {
                    if (readLine.startsWith("retryExec")) {
                        String value = readLine.split("=")[1];
                        for (String s : value.split(";")) {
                            list.add(s);
                        }
                    }
                    // list.add(readLine);
                }
            }
        } catch (IOException e) {
            // TODO handle IO Exception
            throw new IOException();
        }
        return list;
    }

    public ConfigRetryRule searchRuleSet(int ruleToSearch, String ruleSet) throws SQLServerException {
        reread();
        if (ruleSet.equals("statement")) {
            for (Map.Entry<Integer, ConfigRetryRule> entry : stmtRules.entrySet()) {
                if (entry.getKey() == ruleToSearch) {
                    return entry.getValue();
                }
            }
        } else {
            for (Map.Entry<Integer, ConfigRetryRule> entry : cxnRules.entrySet()) {
                if (entry.getKey() == ruleToSearch) {
                    return entry.getValue();
                }
            }
        }

        return null;
    }

    public boolean getReplaceFlag() {
        return replaceFlag;
    }
}
