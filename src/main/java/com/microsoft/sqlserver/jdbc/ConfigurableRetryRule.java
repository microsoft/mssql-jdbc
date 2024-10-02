/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.ArrayList;


/**
 * The ConfigRetryRule object is what is used by the ConfigurableRetryLogic class to handle statement retries. Each
 * ConfigRetryRule object allows for one rule.
 *
 */
class ConfigurableRetryRule {
    private final String PLUS_SIGN = "+";
    private final String MULTIPLICATION_SIGN = "*";
    private final String COMMA = ",";
    private final String ZERO = "0";
    private String operand = "+";
    private int initialRetryTime = 0;
    private int retryChange = 2;
    private int retryCount = 1;
    private String retryQueries = "";
    private String retryError;
    boolean isConnection = false;
    boolean replaceExisting = false;

    private ArrayList<Integer> waitTimes = new ArrayList<>();

    /**
     * Construct a ConfigurableRetryRule object from a String rule.
     *
     * @param rule
     *        the rule used to construct the ConfigRetryRule object
     * @throws SQLServerException
     *         if there is a problem parsing the rule
     */
    ConfigurableRetryRule(String rule) throws SQLServerException {
        addElements(removeExtraElementsAndSplitRule(rule));
        calculateWaitTimes();
    }

    /**
     * Allows constructing a ConfigRetryRule object from another ConfigRetryRule object. Used when the first object has
     * multiple errors provided. We pass in the multi-error object and create 1 new object for each error in the initial
     * object.
     *
     * @param newRule
     *        the rule used to construct the ConfigRetryRule object
     * @param baseRule
     *        the ConfigRetryRule object to base the new objects off of
     */
    ConfigurableRetryRule(String newRule, ConfigurableRetryRule baseRule) {
        copyFromRule(baseRule);
        this.retryError = newRule;
    }

    /**
     * Copy elements from the base rule to this rule.
     *
     * @param baseRule
     *        the rule to copy elements from
     */
    private void copyFromRule(ConfigurableRetryRule baseRule) {
        this.retryError = baseRule.retryError;
        this.operand = baseRule.operand;
        this.initialRetryTime = baseRule.initialRetryTime;
        this.retryChange = baseRule.retryChange;
        this.retryCount = baseRule.retryCount;
        this.retryQueries = baseRule.retryQueries;
        this.waitTimes = baseRule.waitTimes;
        this.isConnection = baseRule.isConnection;
    }

    private String appendOrReplace(String retryError) {
        if (retryError.startsWith(PLUS_SIGN)) {
            replaceExisting = false;
            StringUtils.isNumeric(retryError.substring(1));
            return retryError.substring(1);
        } else {
            replaceExisting = true;
            return retryError;
        }
    }

    /**
     * Removes extra elements in the rule (e.g. '{') and splits the rule based on ':' (colon).
     *
     * @param rule
     *        the rule to format and split
     * @return the split rule as a string array
     */
    private String[] removeExtraElementsAndSplitRule(String rule) {
        if (rule.endsWith(":")) {
            rule = rule + ZERO; // Add a zero to make below parsing easier
        }

        rule = rule.replace("{", "");
        rule = rule.replace("}", "");
        rule = rule.trim();

        return rule.split(":"); // Split on colon
    }

    /**
     * Checks if the value passed in is numeric. In the case where the value contains a comma, the value must be a
     * multi-error value, e.g. 2714,2716. This must be separated, and each error checked separately.
     *
     * @param value
     *        the value to be checked
     * @throws SQLServerException
     *         if a non-numeric value is passed in
     */
    private void checkParameter(String value) throws SQLServerException {
        if (!StringUtils.isNumeric(value)) {
            String[] arr = value.split(COMMA);
            for (String error : arr) {
                if (!StringUtils.isNumeric(error)) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidParameterNumber"));
                    Object[] msgArgs = {error};
                    throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
                }
            }
        }
    }

    /**
     * Parses the passed in string array, containing all elements from the original rule, and assigns the information
     * to the class variables. The logic is as follows:
     * <p>
     * </p>
     * The rule array, which was created by splitting the rule string based on ":", must be of length 2 or 3. If not
     * there are too many parts, and an error is thrown.
     * <p>
     * </p>
     * If it is of length 2 or 3, the first part is always the retry error (the error to retry on). We check if its
     * numeric, and if so, assign it to the class variable. The second part are the retry timings, which include
     * retry count (mandatory), initial retry time (optional), operand (optional), and retry change (optional). A
     * parameter can only be included, if ALL parameters prior to it are included. Thus, these are the only valid rule
     * formats for rules of length 2:
     * error; count
     * error; count, initial retry time
     * error; count, initial retry time [OPERAND]
     * error; count, initial retry time [OPERAND] retry change
     * <p>
     * </p>
     * Next, the second part of the rule is parsed based on "," and each part checked. The retry count is mandatory
     * and must be numeric and greater than 0, else an error is thrown.
     * <p>
     * </p>
     * If there is a second part to the retry timings, it includes any of the parameters mentioned above: initial retry
     * time, operand, and retry change. We first check if there is an operand, if not, then only initial retry time has
     * been given, and it is assigned. If there is an operand, we split this second part based on the operand.
     * Whatever was before the operand was the initial retry time, and if there was something after the operand, this
     * is the retry change. If there are more than 2 parts to the timing, i.e. more than 2 commas, throw an error.
     * <p>
     * </p>
     * Finally, if the rule has 3 parts, it includes a query specifier, parse this and assign it.
     *
     * @param rule
     *        the passed in rule, as a string array
     * @throws SQLServerException
     *         if a rule or parameter has invalid inputs
     */
    private void addElements(String[] rule) throws SQLServerException {
        if (rule.length == 1) {
            String errorWithoutOptionalPrefix = appendOrReplace(rule[0]);
            checkParameter(errorWithoutOptionalPrefix);
            isConnection = true;
            retryError = errorWithoutOptionalPrefix;
        } else if (rule.length == 2 || rule.length == 3) {
            checkParameter(rule[0]);
            retryError = rule[0];
            String[] timings = rule[1].split(COMMA);
            checkParameter(timings[0]);
            retryCount = Integer.parseInt(timings[0]);

            if (timings.length == 2) {
                if (timings[1].contains(MULTIPLICATION_SIGN)) {
                    String[] initialAndChange = timings[1].split("\\*");
                    checkParameter(initialAndChange[0]);

                    initialRetryTime = Integer.parseInt(initialAndChange[0]);
                    operand = MULTIPLICATION_SIGN;
                    if (initialAndChange.length > 1) {
                        checkParameter(initialAndChange[1]);
                        retryChange = Integer.parseInt(initialAndChange[1]);
                    } else {
                        retryChange = initialRetryTime;
                    }
                } else if (timings[1].contains(PLUS_SIGN)) {
                    String[] initialAndChange = timings[1].split("\\+");
                    checkParameter(initialAndChange[0]);

                    initialRetryTime = Integer.parseInt(initialAndChange[0]);
                    operand = PLUS_SIGN;
                    if (initialAndChange.length > 1) {
                        checkParameter(initialAndChange[1]);
                        retryChange = Integer.parseInt(initialAndChange[1]);
                    } else {
                        retryChange = 2;
                    }
                } else {
                    checkParameter(timings[1]);
                    initialRetryTime = Integer.parseInt(timings[1]);
                }
            } else if (timings.length > 2) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidParameterNumber"));
                Object[] msgArgs = {rule[1]};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
            }

            if (rule.length == 3) {
                retryQueries = (rule[2].equals(ZERO) ? "" : rule[2].toLowerCase());
            }
        } else {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidRuleFormat"));
            Object[] msgArgs = {rule.length};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
        }
    }

    /**
     * Calculates all the 'wait times', i.e. how long the driver waits between re-execution of statement, based
     * on the parameters in the rule. Saves all these times in "waitTimes" to be referenced during statement re-execution.
     */
    private void calculateWaitTimes() {
        for (int i = 0; i < retryCount; ++i) {
            int waitTime = initialRetryTime;
            if (operand.equals(PLUS_SIGN)) {
                for (int j = 0; j < i; ++j) {
                    waitTime += retryChange;
                }
            } else if (operand.equals(MULTIPLICATION_SIGN)) {
                for (int k = 0; k < i; ++k) {
                    waitTime *= retryChange;
                }
            }
            waitTimes.add(waitTime);
        }
    }

    /**
     * Returns the retry error for this ConfigRetryRule object.
     *
     * @return the retry error
     */
    String getError() {
        return retryError;
    }

    /**
     * Returns the retry count (amount of times to retry) for this ConfigRetryRule object.
     *
     * @return the retry count
     */
    int getRetryCount() {
        return retryCount;
    }

    /**
     * Returns the retry query specifier for this ConfigRetryRule object.
     *
     * @return the retry query specifier
     */
    String getRetryQueries() {
        return retryQueries;
    }

    /**
     * Returns an array listing the waiting times between each retry, for this ConfigRetryRule object.
     *
     * @return the list of waiting times
     */
    ArrayList<Integer> getWaitTimes() {
        return waitTimes;
    }
}
