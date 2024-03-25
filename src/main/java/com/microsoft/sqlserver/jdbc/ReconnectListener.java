/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

/**
 * This functional interface represents a listener which is called before a reconnect of {@link SQLServerConnection}.
 */
@FunctionalInterface
public interface ReconnectListener {

    void beforeReconnect();

}
