/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

import java.util.HashMap;
import java.util.Map;

import javax.security.auth.login.AppConfigurationEntry;
import javax.security.auth.login.Configuration;


/**
 * Overrides JAAS Configuration and always provide a configuration is not defined for default configuration.
 */
public class JaasConfiguration extends Configuration {

    private final Configuration delegate;
    private AppConfigurationEntry[] defaultValue;

    private static AppConfigurationEntry[] generateDefaultConfiguration() throws SQLServerException {
        try {
            if (Util.isIBM()) {
                return loadIbmModule();
            }
            Class.forName("com.sun.security.auth.module.Krb5LoginModule");
            Map<String, String> confDetails = new HashMap<>();
            confDetails.put("useTicketCache", "true");
            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry("com.sun.security.auth.module.Krb5LoginModule",
                            AppConfigurationEntry.LoginModuleControlFlag.REQUIRED, confDetails)};
        } catch (ClassNotFoundException e) {
            throw new SQLServerException(SQLServerException.getErrString("R_moduleNotFound"), null);
        }
    }

    private static AppConfigurationEntry[] loadIbmModule() throws SQLServerException {
        try {
            Class.forName("com.ibm.security.auth.module.Krb5LoginModule");
            Map<String, String> confDetailsWithoutPassword = new HashMap<>();
            confDetailsWithoutPassword.put("useDefaultCcache", "true");
            Map<String, String> confDetailsWithPassword = new HashMap<>();
            final String ibmLoginModule = "com.ibm.security.auth.module.Krb5LoginModule";
            return new AppConfigurationEntry[] {
                    new AppConfigurationEntry(ibmLoginModule, AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                            confDetailsWithoutPassword),
                    new AppConfigurationEntry(ibmLoginModule, AppConfigurationEntry.LoginModuleControlFlag.SUFFICIENT,
                            confDetailsWithPassword)};
        } catch (ClassNotFoundException ex) {
            throw new SQLServerException(SQLServerException.getErrString("R_ibmModuleNotFound"), null);
        }
    }

    /**
     * Package protected constructor.
     * 
     * @param delegate
     *        a possibly null delegate
     * @throws SQLServerException
     *         if neither Kerberos module is found: com.sun.security.auth.module.Krb5LoginModule or com.ibm.security.auth.module.Krb5LoginModule
     */
    JaasConfiguration(Configuration delegate) throws SQLServerException {
        this.delegate = delegate;
        this.defaultValue = generateDefaultConfiguration();
    }

    @Override
    public AppConfigurationEntry[] getAppConfigurationEntry(String name) {
        AppConfigurationEntry[] conf = delegate == null ? null : delegate.getAppConfigurationEntry(name);
        // We return our configuration only if user requested default one
        // In case where user did request another JAAS Configuration name, we expect he knows what he is doing.
        if (conf == null && name.equals(SQLServerDriverStringProperty.JAAS_CONFIG_NAME.getDefaultValue())) {
            return defaultValue;
        }
        return conf;
    }

    @Override
    public void refresh() {
        if (null != delegate)
            delegate.refresh();
    }
}
