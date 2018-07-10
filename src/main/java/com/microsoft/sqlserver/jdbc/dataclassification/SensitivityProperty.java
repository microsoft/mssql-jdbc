/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.dataclassification;

/**
 * This class contains information about a Sensitivity Property as recieved from SQL Server for the active resultSet
 */
public class SensitivityProperty {
    private Label label;
    private InformationType informationType;

    /**
     * Constructor for Sensitivity Property class object
     * 
     * @param label
     *        Label as recieved from SQL Server for this SensitivityProperty
     * @param informationType
     *        InformationType as recieved from SQL Server for this SensitivityProperty
     */
    public SensitivityProperty(Label label, InformationType informationType) {
        this.label = label;
        this.informationType = informationType;
    }

    /**
     * Retrieves Label data for this <code>SensitivityProperty</code> Object
     * 
     * @return label
     */
    public Label getLabel() {
        return label;
    }

    /**
     * Retrieves InformationType data for this <code>SensitivityProperty</code> Object
     * 
     * @return informationType
     */
    public InformationType getInformationType() {
        return informationType;
    }
}
