/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.dataclassification;

/**
 * Represents the Data Classification Sensitivity Property as received from SQL Server for the active resultSet
 */
public class SensitivityProperty {
    private Label label;
    private InformationType informationType;
    private int sensitivityRank;

    /**
     * Constructs a SensitivityProperty
     * 
     * @param label
     *        Label as received from SQL Server for this SensitivityProperty
     * @param informationType
     *        InformationType as received from SQL Server for this SensitivityProperty
     */
    public SensitivityProperty(Label label, InformationType informationType) {
        this.label = label;
        this.informationType = informationType;
    }

    /**
     * Constructs a SensitivityProperty
     * 
     * @param label
     *        Label as received from SQL Server for this SensitivityProperty
     * @param informationType
     *        InformationType as received from SQL Server for this SensitivityProperty
     * @param sensitivityRank
     *        sensitivity rank as received from SQL Server for this SensitivityProperty
     */
    public SensitivityProperty(Label label, InformationType informationType, int sensitivityRank) {
        this.label = label;
        this.informationType = informationType;
        this.sensitivityRank = sensitivityRank;
    }

    /**
     * Returns the label data for this <code>SensitivityProperty</code> Object
     * 
     * @return label
     */
    public Label getLabel() {
        return label;
    }

    /**
     * Returns the information type data for this <code>SensitivityProperty</code> Object
     * 
     * @return informationType
     */
    public InformationType getInformationType() {
        return informationType;
    }

    /**
     * Returns the sensitivity rank for this <code>SensitivityProperty</code> Object
     * 
     * @return sensitivityRank
     */
    public int getSensitivityRank() {
        return sensitivityRank;
    }
}
