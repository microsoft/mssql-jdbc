package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.ArrayList;


/**
 * The ConfigRetryRule object is what is used by the ConfigurableRetryLogic class to handle statement retries. Each
 * ConfigRetryRule object allows for one rule.
 *
 */
public class ConfigRetryRule {
    private String retryError;
    private String operand = "+";
    private final String PLUS_SIGN = "+";
    private final String MULTIPLICATION_SIGN = "*";
    private int initialRetryTime = 0;
    private int retryChange = 2;
    private int retryCount = 1;
    private String retryQueries = "";
    private final String NON_POSITIVE_INT = "Not a positive number";
    private final String TOO_MANY_ARGS = "Too many arguments";
    private final String OPEN_BRACE = "{";
    private final String CLOSING_BRACE = "}";
    private final String COMMA = ",";
    private final String COLON = ":";
    private final String ZERO = "0";

    private ArrayList<Integer> waitTimes = new ArrayList<>();

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
    }

    private String[] parse(String rule) {
        if (rule.endsWith(COLON)) {
            rule = rule + ZERO; // Add a zero to make below parsing easier
        }

        rule = rule.replace(OPEN_BRACE, "");
        rule = rule.replace(CLOSING_BRACE, "");
        rule = rule.trim();

        return rule.split(COLON);
    }

    /**
     * Checks if the value passed in is numeric. In the case where the value contains a comma, the value must be a
     * multi-error value, e.g. 2714,2716. This must be separated, and each error checked separately.
     *
     * @param value
     *        The value to be checked
     * @throws SQLServerException
     *         if a non-numeric value is passed in
     */
    private void parameterIsNumeric(String value) throws SQLServerException {
        if (!StringUtils.isNumeric(value)) {
            String[] arr = value.split(COMMA);
            for (String error : arr) {
                if (!StringUtils.isNumeric(error)) {
                    MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                    Object[] msgArgs = {error, NON_POSITIVE_INT};
                    throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
                }
            }
        }
    }

    private void addElements(String[] rule) throws SQLServerException {
        if (rule.length == 2 || rule.length == 3) {
            parameterIsNumeric(rule[0]);
            retryError = rule[0];
            String[] timings = rule[1].split(COMMA);
            parameterIsNumeric(timings[0]);
            int parsedRetryCount = Integer.parseInt(timings[0]);

            if (parsedRetryCount > 0) {
                retryCount = parsedRetryCount;
            } else {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                Object[] msgArgs = {parsedRetryCount, NON_POSITIVE_INT};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
            }

            if (timings.length == 2) {
                if (timings[1].contains(MULTIPLICATION_SIGN)) {
                    String[] initialAndChange = timings[1].split("\\*");
                    parameterIsNumeric(initialAndChange[0]);

                    initialRetryTime = Integer.parseInt(initialAndChange[0]);
                    operand = MULTIPLICATION_SIGN;
                    if (initialAndChange.length > 1) {
                        parameterIsNumeric(initialAndChange[1]);
                        retryChange = Integer.parseInt(initialAndChange[1]);
                    } else {
                        retryChange = initialRetryTime;
                    }
                } else if (timings[1].contains(PLUS_SIGN)) {
                    String[] initialAndChange = timings[1].split("\\+");
                    parameterIsNumeric(initialAndChange[0]);

                    initialRetryTime = Integer.parseInt(initialAndChange[0]);
                    operand = PLUS_SIGN;
                    if (initialAndChange.length > 1) {
                        parameterIsNumeric(initialAndChange[1]);
                        retryChange = Integer.parseInt(initialAndChange[1]);
                    } else {
                        retryChange = initialRetryTime;
                    }
                } else {
                    parameterIsNumeric(timings[1]);
                    initialRetryTime = Integer.parseInt(timings[1]);
                }
            } else if (timings.length > 2) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                Object[] msgArgs = {rule[1], TOO_MANY_ARGS};
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

    private void calcWaitTime() {
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

    public String getRetryQueries() {
        return retryQueries;
    }

    public ArrayList<Integer> getWaitTimes() {
        return waitTimes;
    }
}
