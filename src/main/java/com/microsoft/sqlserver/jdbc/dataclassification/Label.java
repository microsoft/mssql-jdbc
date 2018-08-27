/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.dataclassification;

/**
 * Represents the Data Classification Labels as received from SQL Server for the active resultSet
 */
public class Label {
    private String name;
    private String id;

    /**
     * Constructs a Label
     * 
     * @param name
     *        Name of Label
     * @param id
     *        ID of Label
     */
    public Label(String name, String id) {
        this.name = name;
        this.id = id;
    }

    /**
     * Returns the name of this <code>InformationType</code> Object
     * 
     * @return Name of Information Type
     */
    public String getName() {
        return name;
    }

    /**
     * Returns the ID for this <code>InformationType</code> object
     * 
     * @return ID of Information Type
     */
    public String getId() {
        return id;
    }
}
