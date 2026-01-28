/*
 * Microsoft JDBC Driver for SQL Server
 * Model-Based Testing Framework for JUnit 5
 */
package com.microsoft.sqlserver.jdbc.mbt.connection;

import com.microsoft.sqlserver.jdbc.mbt.connection.ConnectionProperty.PropertyFlag;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;

/**
 * SQL Server Driver property definitions for Model-Based Testing.
 * 
 * Defines all connection properties with their flags (OPTIONAL, REQUIRED, etc.)
 * and valid values. This is equivalent to fxSqlServerDriver.java.
 * 
 * The OPTIONAL properties are used to create multiple model runs,
 * each with a different optional property set.
 */
public class SqlServerDriverProperties {
    
    private static final List<ConnectionProperty> PROPERTIES = new ArrayList<>();
    
    static {
        // Required properties
        PROPERTIES.add(new ConnectionProperty(
            "serverName",
            EnumSet.of(PropertyFlag.REQUIRED, PropertyFlag.SERVER, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            "localhost"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "user",
            EnumSet.of(PropertyFlag.REQUIRED, PropertyFlag.USER, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "password",
            EnumSet.of(PropertyFlag.REQUIRED, PropertyFlag.PASSWORD, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        // Optional properties (equivalent to OPTIONAL flag in fxSqlServerDriver)
        PROPERTIES.add(new ConnectionProperty(
            "applicationName",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("MBT_TestApp", "JDBCTestApp", "MyApplication"),
            "Microsoft JDBC Driver for SQL Server"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "authentication",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("SqlPassword", "ActiveDirectoryPassword", "ActiveDirectoryIntegrated", 
                          "ActiveDirectoryMSI", "ActiveDirectoryServicePrincipal", "NotSpecified"),
            "NotSpecified"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "columnEncryptionSetting",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("Enabled", "Disabled"),
            "Disabled"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "databaseName",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.DATABASE, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("master", "tempdb", "msdb"),
            "master"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "disableStatementPooling",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "true"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "encrypt",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.ENCRYPTION, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false", "strict"),
            "true"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "failoverPartner",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "hostNameInCertificate",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "instanceName",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "integratedSecurity",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.INTEGRATED_AUTH, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "false"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "lastUpdateCount",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "true"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "lockTimeout",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("-1", "0", "1000", "5000", "30000"),
            "-1"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "loginTimeout",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.LOGIN_TIMEOUT, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("15", "30", "60", "120"),
            "15"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "multiSubnetFailover",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "false"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "packetSize",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("512", "4096", "8000", "16384", "32767"),
            "8000"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "portNumber",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("1433"),
            "1433"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "responseBuffering",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("adaptive", "full"),
            "adaptive"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "selectMethod",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("direct", "cursor"),
            "direct"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "sendStringParametersAsUnicode",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "true"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "sendTimeAsDatetime",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "true"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "serverNameAsACE",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "false"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "transparentNetworkIPResolution",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "true"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "trustServerCertificate",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "false"
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "trustStore",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "trustStorePassword",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Collections.emptyList(),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "workstationID",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("MBT_Workstation", "TestWorkstation"),
            ""
        ));
        
        PROPERTIES.add(new ConnectionProperty(
            "xopenStates",
            EnumSet.of(PropertyFlag.OPTIONAL, PropertyFlag.BOOLEAN, PropertyFlag.URL_PROPERTY, PropertyFlag.INFO_PROPERTY),
            Arrays.asList("true", "false"),
            "false"
        ));
    }
    
    /**
     * Gets all defined properties.
     */
    public static List<ConnectionProperty> getAllProperties() {
        return new ArrayList<>(PROPERTIES);
    }
    
    /**
     * Gets all optional properties (equivalent to OPTIONAL flag in FX).
     */
    public static List<ConnectionProperty> getOptionalProperties() {
        List<ConnectionProperty> optional = new ArrayList<>();
        for (ConnectionProperty prop : PROPERTIES) {
            if (prop.isOptional()) {
                optional.add(prop);
            }
        }
        return optional;
    }
    
    /**
     * Gets all required properties.
     */
    public static List<ConnectionProperty> getRequiredProperties() {
        List<ConnectionProperty> required = new ArrayList<>();
        for (ConnectionProperty prop : PROPERTIES) {
            if (prop.isRequired()) {
                required.add(prop);
            }
        }
        return required;
    }
    
    /**
     * Gets properties by flag.
     */
    public static List<ConnectionProperty> getPropertiesByFlag(PropertyFlag flag) {
        List<ConnectionProperty> result = new ArrayList<>();
        for (ConnectionProperty prop : PROPERTIES) {
            if (prop.hasFlag(flag)) {
                result.add(prop);
            }
        }
        return result;
    }
    
    /**
     * Gets property by keyword.
     */
    public static ConnectionProperty getProperty(String keyword) {
        for (ConnectionProperty prop : PROPERTIES) {
            if (prop.getKeyword().equalsIgnoreCase(keyword)) {
                return prop;
            }
        }
        return null;
    }
    
    /**
     * Gets the count of optional properties.
     * This determines how many ModelEngine runs will occur per variation.
     */
    public static int getOptionalPropertyCount() {
        return getOptionalProperties().size();
    }
}
