/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.osgi;

import java.util.Dictionary;
import java.util.Hashtable;

import org.osgi.framework.BundleActivator;
import org.osgi.framework.BundleContext;
import org.osgi.framework.ServiceRegistration;
import org.osgi.service.jdbc.DataSourceFactory;

import com.microsoft.sqlserver.jdbc.SQLServerDriver;


/**
 * Allows plugins to register the driver as an OSGI Framework service.
 */
public class Activator implements BundleActivator {

    private ServiceRegistration<DataSourceFactory> service;

    @Override
    public void start(BundleContext context) throws Exception {
        Dictionary<String, Object> properties = new Hashtable<>();
        SQLServerDriver driver = new SQLServerDriver();
        properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_CLASS, driver.getClass().getName());
        properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_NAME, "Microsoft JDBC Driver for SQL Server");
        properties.put(DataSourceFactory.OSGI_JDBC_DRIVER_VERSION,
                driver.getMajorVersion() + "." + driver.getMinorVersion());
        service = context.registerService(DataSourceFactory.class, new SQLServerDataSourceFactory(), properties);
        SQLServerDriver.register();
    }

    @Override
    public void stop(BundleContext context) throws Exception {
        if (service != null) {
            service.unregister();
        }
        SQLServerDriver.deregister();
    }
}
