package com.microsoft.sqlserver.jdbc;

public enum PerformanceActivity {
    CONNECTION("Connection"),
    TOKEN_ACQUISITION("Token acquisition"),
    LOGIN("Login"),
    PRELOGIN("Prelogin");

    private final String activity;

    PerformanceActivity(String activity) {
        this.activity = activity;
    }

    public String activity() {
        return activity;
    }

    @Override
    public String toString() {
        return activity;
    }
}
