package com.microsoft.sqlserver.jdbc;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;

public class RetryLogicProvider {
    // For each connection we would have a retry logic provider, given that retry logic has been enabled.
    // Read from SQLServerConnection

    private final static java.util.logging.Logger retryLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.RetryLogicProvider");

    private final String providerRules;
    private final ConcurrentHashMap<String, RetryLogic> logicObjects = new ConcurrentHashMap<>(0);

    public RetryLogicProvider(String rules) {
        providerRules = rules;
    }

    private static final String GLOBAL_PROPERTIES = "\\path\\to\\global\\properties";

    private static Properties getPropertiesFile() {
        Properties props = null;
        try (FileInputStream in = new FileInputStream(GLOBAL_PROPERTIES)) {
            props = new Properties();
            props.load(in);
        } catch (IOException e) {
            if (retryLogger.isLoggable(Level.FINER)) {
                retryLogger.finer("Unable to load the global.properties file: " + e);
            }
        }
        return (null != props && !props.isEmpty()) ? props : null;
    }

    /**
     * Every time we get an error, if configurable retry logic has been enabled, we want to go here and follow this
     * logic path.
     */
    void execute(String error) throws Exception {
        if (logicObjects.isEmpty()) {
            start();
        }

        RetryLogic logObj = getLogicObject(error);

        if (null == logObj) {
            // There is no rule associated with this error
            // Return something
            System.out.println("NO RULE");
        }

        // If we reach here, there is a rule associated with the error
        // We need to do the following
        // a) check if the rule has a specific query, and then see if the queries match
        // b) set up intervals based on interval rules
        // c) pass this information somehow to where we want to retry - ??

        // a) QueryMatch
        if (!queryCheck(logObj)) {
            // If the query is not a match, we get false
            System.out.println("NO RULE WITH MATCHING QUERY");
        }

        // b) Interval set up
        // Use an interval object


    }

    boolean queryCheck(RetryLogic obj) {
        String query = ""; //Temp to prevent syntax error
        if (null == obj.getQuery()) {
            // No query, so everything matches
            return true;
        } else {
            if (obj.getQuery().equals(query)) {
                // If it matches the query we're executing, this is also good
                // Need some way to get this, use a placeholder for now.
                return true;
            }
        }
        return true; //Should be false, but true for now for testing
    }

    // Logic from here should be
    // 1) Create this RetryLogicProvider based on the presence of configurable retry being enabled
    //      In SQLServerConnection, a provider object will be created based on the set of rules passed in alongside
    //      the connection string option.
    // 2) For each rule, we will create one RetryLogic object

    void start() {
        // For each rule passed in, we need to create a RetryLogic object and assign it to a readily accessible list
        // Each rule is separated from the other with a semicolon.
        String[] rules = providerRules.split(";");
        for (String rule : rules) {
            RetryLogic obj = new RetryLogic(rule);
            logicObjects.put(obj.getError(), obj);
        }
    }

    RetryLogic getLogicObject(String error) {
        // Find the logic object associated with the error
        for (Map.Entry<String, RetryLogic> entry : logicObjects.entrySet()) {
            if (entry.getKey().equals(error)) {
                return entry.getValue();
            }
        }
        return null;
    }

}

class RetryLogic {
    String rule;
    String error;
    String intervals;
    String query = null;
    RetryLogic(String rule) {
        this.rule = rule;
        parseRule(rule);
    }

    // Consider the rule to be of form: errorList:retryPolicy:queryMatch
    // For now, consider this to be singleError:retryPolicy:queryMatch
    void parseRule(String rule) {
        String[] parts = rule.split(":");
        error = parts[0]; // This assumes only a single error
        intervals = parts[1];
        if (parts.length > 2) {
            query = parts[2];
        }
    }

    String getError() {
        return error;
    }

    String getIntervals() {return intervals;}

    String getQuery() {return query;}
}

class Interval {
    // Change the name
    // The interval is [Number of retries, base [op] change]
    // Number of retries makes sense
    // Base is the initial retry delay
    // op is either additive (+) or multiplicative (*)
    // change is applied with each op to each base and is optional (defaults to base)

    int numberOfRetries = 0;
    int base = 0;
    boolean additive = false;
    int change = 0;

    Interval(String intervalRules) {
        // Interval rules are presented in the following form: numberOfRetries,base[op]change
        if (intervalRules.isEmpty() || null == intervalRules) {
            // throw an exception. At the point this is created, we know retry logic has been enabled and some
            // rule has been passed in. But if interval is missing, this is wrong
        }
        String[] sep = intervalRules.split(",");
        if (sep.length < 2) {
            // Throw an exception, we need both the number of retries and retry interval
        }

        // Need to check if this is a valid int first
        numberOfRetries = Integer.getInteger(sep[0]);

        // The second part, sep[1] needs to be parsed into base[op]change

        String[] additiveInterval = sep[1].split("\\+");
        if (!additiveInterval[0].equals(sep[1])) {
            // This means it does contain a '+'
            // Just b/c it has a + doesn't mean the rest is correct, need to have error checking for if there
            // is both the base and change, as well as if these are valid values. As well, change is optional, need
            // to add a path that takes that into account.
            base = Integer.getInteger(additiveInterval[0]);
            change = Integer.getInteger(additiveInterval[1]);
        }

        // If we get here, the above interval did not contain a +, now check for a *
        String[] multiplicativeInterval = sep[1].split("\\*");
        if (!multiplicativeInterval[0].equals(sep[1])) {
            // This means it does contain a '*'
            // Just b/c it has a * doesn't mean the rest is correct, need to have error checking for if there
            // is both the base and change, as well as if these are valid values. As well, change is optional, need
            // to add a path that takes that into account.
            base = Integer.getInteger(multiplicativeInterval[0]);
            change = Integer.getInteger(multiplicativeInterval[1]);
        }

        // If neither a + nor * is found in the second string, the interval rules are invalid
    }
}