/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.osgi;

import java.sql.Driver;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Properties;
import java.util.ResourceBundle;
import java.util.logging.Level;

import javax.activation.DataSource;
import javax.sql.ConnectionPoolDataSource;
import javax.sql.XADataSource;

import org.osgi.service.jdbc.DataSourceFactory;

import com.microsoft.sqlserver.jdbc.SQLServerConnectionPoolDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDataSource;
import com.microsoft.sqlserver.jdbc.SQLServerDriver;
import com.microsoft.sqlserver.jdbc.SQLServerXADataSource;


/**
 * Implementation of the Data Service Specification for JDBC™ Technology. Refer to the OSGI 7.0.0 specifications for
 * more details.
 */
public class SQLServerDataSourceFactory implements DataSourceFactory {

    private static java.util.logging.Logger osgiLogger = java.util.logging.Logger
            .getLogger("com.microsoft.sqlserver.jdbc.osgi.SQLServerDataSourceFactory");
    private static final String NOT_SUPPORTED_MSG = ResourceBundle
            .getBundle("com.microsoft.sqlserver.jdbc.SQLServerResource", Locale.getDefault())
            .getString("R_propertyNotSupported");

    @Override
    public javax.sql.DataSource createDataSource(Properties props) throws SQLException {
        SQLServerDataSource source = new SQLServerDataSource();
        setup(source, props);
        return source;
    }

    @Override
    public ConnectionPoolDataSource createConnectionPoolDataSource(Properties props) throws SQLException {
        SQLServerConnectionPoolDataSource poolDataSource = new SQLServerConnectionPoolDataSource();
        setupXSource(poolDataSource, props);
        return poolDataSource;
    }

    @Override
    public XADataSource createXADataSource(Properties props) throws SQLException {
        SQLServerXADataSource xaDataSource = new SQLServerXADataSource();
        setupXSource(xaDataSource, props);
        return xaDataSource;
    }

    @Override
    public Driver createDriver(Properties props) throws SQLException {
        SQLServerDriver driver = new SQLServerDriver();
        return driver;
    }

    /**
     * Sets up the basic properties for {@link DataSource}s
     */
    private void setup(SQLServerDataSource source, Properties props) {
        if (props == null) {
            return;
        }
        if (props.containsKey(JDBC_DATABASE_NAME)) {
            source.setDatabaseName(props.getProperty(JDBC_DATABASE_NAME));
        }
        if (props.containsKey(JDBC_DATASOURCE_NAME)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_DATASOURCE_NAME);
        }
        if (props.containsKey(JDBC_DESCRIPTION)) {
            source.setDescription(props.getProperty(JDBC_DESCRIPTION));
        }
        if (props.containsKey(JDBC_NETWORK_PROTOCOL)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_NETWORK_PROTOCOL);
        }
        if (props.containsKey(JDBC_PASSWORD)) {
            source.setPassword(props.getProperty(JDBC_PASSWORD));
        }
        if (props.containsKey(JDBC_PORT_NUMBER)) {
            source.setPortNumber(Integer.parseInt(props.getProperty(JDBC_PORT_NUMBER)));
        }
        if (props.containsKey(JDBC_ROLE_NAME)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_ROLE_NAME);
        }
        if (props.containsKey(JDBC_SERVER_NAME)) {
            source.setServerName(props.getProperty(JDBC_SERVER_NAME));
        }
        if (props.containsKey(JDBC_URL)) {
            source.setURL(props.getProperty(JDBC_URL));
        }
        if (props.containsKey(JDBC_USER)) {
            source.setUser(props.getProperty(JDBC_USER));
        }
    }

    /**
     * Sets up the basic and extended properties for {@link XADataSource}s and {@link ConnectionPoolDataSource}s
     */
    private void setupXSource(SQLServerConnectionPoolDataSource source, Properties props) {
        if (props == null) {
            return;
        }
        setup(source, props);
        if (props.containsKey(JDBC_INITIAL_POOL_SIZE)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_INITIAL_POOL_SIZE);
        }
        if (props.containsKey(JDBC_MAX_IDLE_TIME)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_MAX_IDLE_TIME);
        }
        if (props.containsKey(JDBC_MAX_STATEMENTS)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_MAX_STATEMENTS);
        }
        if (props.containsKey(JDBC_MAX_POOL_SIZE)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_MAX_POOL_SIZE);
        }
        if (props.containsKey(JDBC_MIN_POOL_SIZE)) {
            osgiLogger.log(Level.WARNING, NOT_SUPPORTED_MSG, JDBC_MIN_POOL_SIZE);
        }
    }

}
