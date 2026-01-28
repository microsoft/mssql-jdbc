/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.connection;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Set;

/**
 * Connection property definition for Model-Based Testing.
 * 
 * Each property has:
 * - keyword: connection string keyword
 * - flags: REQUIRED, OPTIONAL, USER, PASSWORD, etc.
 * - validValues: list of valid values for this property
 * - defaultValue: default value if not specified
 */
public class ConnectionProperty {
    
    private final String keyword;
    private final Set<PropertyFlag> flags;
    private final List<String> validValues;
    private final String defaultValue;
    private String value;
    
    public ConnectionProperty(String keyword, Set<PropertyFlag> flags, 
                              List<String> validValues, String defaultValue) {
        this.keyword = keyword;
        this.flags = EnumSet.copyOf(flags);
        this.validValues = new ArrayList<>(validValues);
        this.defaultValue = defaultValue;
        this.value = defaultValue;
    }
    
    public String getKeyword() {
        return keyword;
    }
    
    public Set<PropertyFlag> getFlags() {
        return EnumSet.copyOf(flags);
    }
    
    public boolean hasFlag(PropertyFlag flag) {
        return flags.contains(flag);
    }
    
    public boolean isOptional() {
        return flags.contains(PropertyFlag.OPTIONAL);
    }
    
    public boolean isRequired() {
        return flags.contains(PropertyFlag.REQUIRED);
    }
    
    public List<String> getValidValues() {
        return new ArrayList<>(validValues);
    }
    
    public String getDefaultValue() {
        return defaultValue;
    }
    
    public String getValue() {
        return value;
    }
    
    public void setValue(String value) {
        this.value = value;
    }
    
    public String getRandomValidValue() {
        if (validValues.isEmpty()) {
            return defaultValue;
        }
        int index = (int) (Math.random() * validValues.size());
        return validValues.get(index);
    }
    
    @Override
    public String toString() {
        return String.format("ConnectionProperty[%s=%s]", keyword, value);
    }
    
    /**
     * Property flags defining characteristics of connection properties.
     */
    public enum PropertyFlag {
        /** Property is optional */
        OPTIONAL,
        /** Property is required */
        REQUIRED,
        /** Property represents user name */
        USER,
        /** Property represents password */
        PASSWORD,
        /** Property represents server name */
        SERVER,
        /** Property represents database name */
        DATABASE,
        /** Property is boolean type */
        BOOLEAN,
        /** Property represents integrated security */
        INTEGRATED_AUTH,
        /** Property affects encryption */
        ENCRYPTION,
        /** Property affects login timeout */
        LOGIN_TIMEOUT,
        /** Property affects connection timeout */
        CONNECTION_TIMEOUT,
        /** Property can be set via URL */
        URL_PROPERTY,
        /** Property can be set via Properties object */
        INFO_PROPERTY
    }
}
