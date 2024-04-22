package com.microsoft.sqlserver.jdbc;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;


public class ConfigurableRetryLogic {
    private final static int INTERVAL_BETWEEN_READS = 30000; // How many ms must have elapsed before we re-read
    private final static String DEFAULT_PROPS_FILE = "mssql-jdbc.properties";
    private static final java.util.logging.Logger CONFIGURABLE_RETRY_LOGGER = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.ConfigurableRetryLogic");
    private static ConfigurableRetryLogic driverInstance = null;
    private static long timeLastModified;
    private static long timeLastRead;
    private static String lastQuery = ""; // The last query executed (used when rule is process-dependent)
    private static String rulesFromConnectionString = "";
    private static boolean replaceFlag; // Are we replacing the list of transient errors (for connection retry)?
    private static HashMap<Integer, ConfigRetryRule> cxnRules = new HashMap<>();
    private static HashMap<Integer, ConfigRetryRule> stmtRules = new HashMap<>();


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

    private static void reread() throws SQLServerException {
        long currentTime = new Date().getTime();
        if ((currentTime - timeLastRead) >= INTERVAL_BETWEEN_READS && !compareModified()) {
            timeLastRead = currentTime;
            setUpRules();
        }
    }

    private static boolean compareModified() {
        String inputToUse = getCurrentClassPath() + DEFAULT_PROPS_FILE;

        try {
            File f = new File(inputToUse);
            return f.lastModified() == timeLastModified;
        } catch (Exception e) {
            return false;
        }
    }

    void setFromConnectionString(String custom) throws SQLServerException {
        rulesFromConnectionString = custom;
        setUpRules();
    }

    void storeLastQuery(String sql) {
        lastQuery = sql.toLowerCase();
    }

    String getLastQuery() {
        return lastQuery;
    }

    private static void setUpRules() throws SQLServerException {
        cxnRules = new HashMap<>();
        stmtRules = new HashMap<>();
        replaceFlag = false;
        lastQuery = "";
        LinkedList<String> temp;

        if (!rulesFromConnectionString.isEmpty()) {
            temp = new LinkedList<>();
            Collections.addAll(temp, rulesFromConnectionString.split(";"));
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

                for (String retryError : arr) {
                    ConfigRetryRule splitRule = new ConfigRetryRule(retryError, rule);
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
                    if (readLine.startsWith("retryExec")) {
                        String value = readLine.split("=")[1];
                        Collections.addAll(list, value.split(";"));
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

    ConfigRetryRule searchRuleSet(int ruleToSearch, String ruleSet) throws SQLServerException {
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

    boolean getReplaceFlag() {
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

    public ConfigRetryRule(String rule) throws SQLServerException {
        addElements(parse(rule));
        calcWaitTime();
    }

    public ConfigRetryRule(String rule, ConfigRetryRule base) {
        copyFromExisting(base);
        this.retryError = rule;
    }

    private void copyFromExisting(ConfigRetryRule base) {
        this.retryError = base.getError();
        this.operand = base.getOperand();
        this.initialRetryTime = base.getInitialRetryTime();
        this.retryChange = base.getRetryChange();
        this.retryCount = base.getRetryCount();
        this.retryQueries = base.getRetryQueries();
        this.waitTimes = base.getWaitTimes();
        this.isConnection = base.getConnectionStatus();
    }

    private String[] parse(String rule) {
        String parsed = rule + " ";

        parsed = parsed.replace(": ", ":0");
        parsed = parsed.replace("{", "");
        parsed = parsed.replace("}", "");
        parsed = parsed.trim();

        return parsed.split(":");
    }

    private void parameterIsNumeric(String value) throws SQLServerException {
        if (!StringUtils.isNumeric(value)) {
            String[] arr = value.split(",");
            for (String error : arr) {
                if (!StringUtils.isNumeric(error)) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                    throw new SQLServerException(null, form.format(new Object[] {}), null, 0, true);
                }
            }
        }
    }

    private void addElements(String[] rule) throws SQLServerException {
        if (rule.length == 1) {
            String errorWithoutOptionalPrefix = appendOrReplace(rule[0]);
            parameterIsNumeric(errorWithoutOptionalPrefix);
            isConnection = true;
            retryError = errorWithoutOptionalPrefix;
        } else if (rule.length == 2 || rule.length == 3) {
            parameterIsNumeric(rule[0]);
            retryError = rule[0];
            String[] timings = rule[1].split(",");
            parameterIsNumeric(timings[0]);

            if (Integer.parseInt(timings[0]) > 0) {
                retryCount = Integer.parseInt(timings[0]);
            } else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                throw new SQLServerException(null, form.format(new Object[] {}), null, 0, true);
            }

            if (timings.length == 2) {
                if (timings[1].contains("*")) {
                    String[] initialAndChange = timings[1].split("\\*");
                    parameterIsNumeric(initialAndChange[0]);
                    initialRetryTime = Integer.parseInt(initialAndChange[0]);
                    operand = "*";
                    if (initialAndChange.length > 1) {
                        parameterIsNumeric(initialAndChange[1]);
                        retryChange = Integer.parseInt(initialAndChange[1]);
                    } else {
                        retryChange = initialRetryTime;
                    }
                } else if (timings[1].contains("+")) {
                    String[] initialAndChange = timings[1].split("\\+");
                    parameterIsNumeric(initialAndChange[0]);

                    initialRetryTime = Integer.parseInt(initialAndChange[0]);
                    operand = "+";
                    if (initialAndChange.length > 1) {
                        parameterIsNumeric(initialAndChange[1]);
                        retryChange = Integer.parseInt(initialAndChange[1]);
                    }
                } else {
                    parameterIsNumeric(timings[1]);
                    initialRetryTime = Integer.parseInt(timings[1]);
                }
            } else if (timings.length > 2) {
                StringBuilder builder = new StringBuilder();
                for (String string : rule) {
                    builder.append(string);
                }
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidRuleFormat"));
                Object[] msgArgs = {builder.toString()};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
            }

            if (rule.length == 3) {
                retryQueries = (rule[2].equals("0") ? "" : rule[2].toLowerCase());
            }
        } else {
            StringBuilder builder = new StringBuilder();
            for (String string : rule) {
                builder.append(string);
            }
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidRuleFormat"));
            Object[] msgArgs = {builder.toString()};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
        }
    }

    private String appendOrReplace(String retryError) {
        if (retryError.charAt(0) == '+') {
            replaceExisting = false;
            StringUtils.isNumeric(retryError.substring(1));
            return retryError.substring(1);
        } else {
            replaceExisting = true;
            return retryError;
        }
    }

    private void calcWaitTime() {
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
