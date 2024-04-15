package com.microsoft.sqlserver.jdbc;

import java.text.MessageFormat;
import java.util.ArrayList;


public class ConfigRetryRule {
    private String retryError;
    private String operand = "*";
    private int initialRetryTime = 10;
    private int retryChange = 2;
    private int retryCount;
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

        // We want to do an empty string check here

        if (temp.isEmpty()) {

        }

        return temp.split(":");
    }

    private void addElements(String[] s) throws SQLServerException {
        // +"retryExec={2714,2716:1,2*2:CREATE;2715:1,3;+4060,4070};"
        if (s.length == 1) {
            // If single element, connection
            isConnection = true;
            retryError = appendOrReplace(s[0]);
        } else if (s.length == 2 || s.length == 3) {
            // If 2 or 3, statement, either with or without query
            // Parse first element (statement rules)
            if (!StringUtils.isNumeric(s[0])) {
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_InvalidParameterFormat"));
                Object[] msgArgs = {s[0], "\"Retry Error\""};
                throw new SQLServerException(null, form.format(msgArgs), null, 0, true);
            } else {
                retryError = s[0];
            }

            // Parse second element (retry options)
            String[] st = s[1].split(",");

            // We have retry count AND timing rules
            retryCount = Integer.parseInt(st[0]); // TODO input validation
            // Second half can either be N, N OP, N OP N
            if (st[1].contains("*")) {
                // We know its either N OP, N OP N
                String[] sss = st[1].split("/*");
                initialRetryTime = Integer.parseInt(sss[0]);
                operand = "*";
                if (sss.length > 2) {
                    retryChange = Integer.parseInt(sss[2]);
                }
            } else if (st[1].contains("+")) {
                // We know its either N OP, N OP N
                String[] sss = st[1].split("/+");
                initialRetryTime = Integer.parseInt(sss[0]);
                operand = "*";
                if (sss.length > 2) {
                    retryChange = Integer.parseInt(sss[2]);
                }
            } else {
                initialRetryTime = Integer.parseInt(st[1]);
                // TODO set defaults
            }
            if (s.length == 3) {
                // Query has also been provided
                retryQueries = (s[2].equals("0") ? "" : s[2].toLowerCase());
            }
        } else {
            // If the length is not 1,2,3, then the provided option is invalid
            // Prov

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
