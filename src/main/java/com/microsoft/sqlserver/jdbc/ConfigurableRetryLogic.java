package com.microsoft.sqlserver.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class ConfigurableRetryLogic {
    private static ConfigurableRetryLogic driverInstance = null;
    private final static int intervalBetweenReads = 30000; // How many ms must have elapsed before we re-read
    private final static String defaultPropsFile = "mssql-jdbc.properties";
    private static long timeLastModified;
    private static long timeLastRead;
    private static String lastQuery = ""; // The last query executed (used when rule is process-dependent)
    private static String rulesFromConnectionString = "";
    private static boolean replaceFlag; // Are we replacing the list of transient errors (for connection retry)?
    private static HashMap<Integer, ConfigRetryRule> cxnRules = new HashMap<>();
    private static HashMap<Integer, ConfigRetryRule> stmtRules = new HashMap<>();
    static private java.util.logging.Logger configReadLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ConfigurableRetryLogic");

    private ConfigurableRetryLogic() throws SQLServerException {
        timeLastRead = new Date().getTime();
        setUpRules();
    }

    /**
     * Fetches the static instance of ConfigurableRetryLogic, instantiating it if it hasn't already been. Each time the instance
     * is fetched, we check if a re-read is needed, and do so if properties should be re-read.
     *
     * @return The static instance of ConfigurableRetryLogic
     * @throws SQLServerException
     *         an exception
     */
    public static synchronized ConfigurableRetryLogic getInstance() throws SQLServerException {
        if (driverInstance == null) {
            driverInstance = new ConfigurableRetryLogic();
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

        if ((currentTime - timeLastRead) >= intervalBetweenReads && !compareModified()) {
            timeLastRead = currentTime;
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

//    public void setCustomRetryRules(String cRR) throws SQLServerException {
//        rulesFromConnectionString = cRR;
//        setUpRules();
//    }

    public void setFromConnectionString(String custom) throws SQLServerException {
        rulesFromConnectionString = custom;
        setUpRules();
    }

    public void storeLastQuery(String sql) {
        lastQuery = sql.toLowerCase();
    }

    public String getLastQuery() {
        return lastQuery;
    }

    private static void setUpRules() throws SQLServerException {
        //For every new setup, everything should be reset
        cxnRules = new HashMap<>();
        stmtRules = new HashMap<>();
        replaceFlag = false;
        lastQuery = "";

        LinkedList<String> temp = null;

        if (!rulesFromConnectionString.isEmpty()) {
            temp = new LinkedList<>();
            for (String s : rulesFromConnectionString.split(";")) {
                temp.add(s);
            }
            rulesFromConnectionString = "";
        } else {
            temp = readFromFile();
        }

        createRules(temp);
    }

    private static void createRules(LinkedList<String> listOfRules) throws SQLServerException {
        cxnRules = new HashMap<>();
        stmtRules = new HashMap<>();

        for (String potentialRule : listOfRules) {
            ConfigRetryRule rule = new ConfigRetryRule(potentialRule);

            if (rule.getError().contains(",")) {
                String[] arr = rule.getError().split(",");

                for (String s : arr) {
                    ConfigRetryRule splitRule = new ConfigRetryRule(s, rule);
                    if (rule.getConnectionStatus()) {
                        if (rule.getReplaceExisting()) {
                            if (!replaceFlag) {
                                cxnRules = new HashMap<>();
                            }
                            replaceFlag = true;
                        }
                        cxnRules.put(Integer.parseInt(splitRule.getError()), splitRule);
                    } else {
                        stmtRules.put(Integer.parseInt(splitRule.getError()), splitRule);
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
            if (configReadLogger.isLoggable(java.util.logging.Level.FINEST)) {
                configReadLogger.finest("Unable to get current class path for properties file reading.");
            }
        }
        return null;
    }

    private static LinkedList<String> readFromFile() {
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
                }
            }
        } catch (IOException e) {
            if (configReadLogger.isLoggable(java.util.logging.Level.FINEST)) {
                configReadLogger.finest("No properties file exists or file is badly formatted.");
            }
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


class ConfigRetryRule {
    private String retryError;
    private String operand = "+";
    private int initialRetryTime = 0;
    private int retryChange = 2;
    private int retryCount = 1;
    private String retryQueries = "";
    private ArrayList<Integer> waitTimes = new ArrayList<>();
    private boolean isConnection = false;
    private boolean replaceExisting = false;

    public ConfigRetryRule(String s) throws SQLServerException {
        String[] stArr = parse(s);
        addElements(stArr);
        calcWaitTime();
    }

    public ConfigRetryRule(String rule, ConfigRetryRule r) {
        copyFromCopy(r);
        this.retryError = rule;
    }

    private void copyFromCopy(ConfigRetryRule r) {
        this.retryError = r.getError();
        this.operand = r.getOperand();
        this.initialRetryTime = r.getInitialRetryTime();
        this.retryChange = r.getRetryChange();
        this.retryCount = r.getRetryCount();
        this.retryQueries = r.getRetryQueries();
        this.waitTimes = r.getWaitTimes();
        this.isConnection = r.getConnectionStatus();
    }

    private String[] parse(String s) {
        String temp = s + " ";

        temp = temp.replace(": ", ":0");
        temp = temp.replace("{", "");
        temp = temp.replace("}", "");
        temp = temp.trim();

        return temp.split(":");
    }

    private void parameterIsNumber(String value) throws SQLServerException {
        if (!StringUtils.isNumeric(value)) {
            String[] arr = value.split(",");
            for (String st : arr) {
                if (!StringUtils.isNumeric(st)) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                    throw new SQLServerException(null, form.format(new Object[] {}), null, 0, true);
                }
            }
        }
    }

    private void addElements(String[] s) throws SQLServerException {
        if (s.length == 1) {
            String errorWithoutOptionalPrefix = appendOrReplace(s[0]);
            parameterIsNumber(errorWithoutOptionalPrefix);
            isConnection = true;
            retryError = errorWithoutOptionalPrefix;
        } else if (s.length == 2 || s.length == 3) {
            parameterIsNumber(s[0]);
            retryError = s[0];

            String[] st = s[1].split(",");
            parameterIsNumber(st[0]);
            if (Integer.parseInt(st[0]) > 0) {
                retryCount = Integer.parseInt(st[0]);
            } else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                throw new SQLServerException(null, form.format(new Object[] {}), null, 0, true);
            }

            if (st.length == 2) {
                if (st[1].contains("*")) {
                    String[] sss = st[1].split("\\*");
                    parameterIsNumber(sss[0]);
                    initialRetryTime = Integer.parseInt(sss[0]);
                    operand = "*";
                    if (sss.length > 1) {
                        parameterIsNumber(sss[1]);
                        retryChange = Integer.parseInt(sss[1]);
                    } else {
                        retryChange = initialRetryTime;
                    }
                } else if (st[1].contains("+")) {
                    String[] sss = st[1].split("\\+");
                    parameterIsNumber(sss[0]);

                    initialRetryTime = Integer.parseInt(sss[0]);
                    operand = "+";
                    if (sss.length > 1) {
                        parameterIsNumber(sss[1]);
                        retryChange = Integer.parseInt(sss[1]);
                    }
                } else {
                    parameterIsNumber(st[1]);
                    initialRetryTime = Integer.parseInt(st[1]);
                }
            } else if (st.length > 2) {
                // If the timing options have more than 2 parts, they are badly formatted.
                StringBuilder builder = new StringBuilder();

                for (String string : s) {
                    builder.append(string);
                }

                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidRuleFormat"));
                Object[] msgArgs = {builder.toString()};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
            }

            if (s.length == 3) {
                retryQueries = (s[2].equals("0") ? "" : s[2].toLowerCase());
            }
        } else {
            // If the length is not 1,2,3, then the provided option is invalid
            StringBuilder builder = new StringBuilder();

            for (String string : s) {
                builder.append(string);
            }

            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidRuleFormat"));
            Object[] msgArgs = {builder.toString()};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
        }
    }

    private String appendOrReplace(String s) {
        if (s.charAt(0) == '+') {
            replaceExisting = false;
            StringUtils.isNumeric(s.substring(1));
            return s.substring(1);
        } else {
            replaceExisting = true;
            return s;
        }
    }

    public void calcWaitTime() {
        for (int i = 0; i < retryCount; ++i) {
            int waitTime = initialRetryTime;
            if (operand.equals("+")) {
                for (int j = 0; j < i; ++j) {
                    waitTime += retryChange;
                }
            } else if (operand.equals("*")) {
                for (int k = 0; k < i; ++k) {
                    waitTime *= retryChange;
                }

            }
            waitTimes.add(waitTime);
        }
    }

    public String getError() {
        return retryError;
    }

    public String getOperand() {
        return operand;
    }

    public int getInitialRetryTime() {
        return initialRetryTime;
    }

    public int getRetryChange() {
        return retryChange;
    }

    public int getRetryCount() {
        return retryCount;
    }

    public boolean getConnectionStatus() {
        return isConnection;
    }

    public String getRetryQueries() {
        return retryQueries;
    }

    public ArrayList<Integer> getWaitTimes() {
        return waitTimes;
    }

    public boolean getReplaceExisting() {
        return replaceExisting;
    }
}
