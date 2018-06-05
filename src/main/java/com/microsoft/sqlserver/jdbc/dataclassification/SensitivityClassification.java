package com.microsoft.sqlserver.jdbc.dataclassification;

import java.util.ArrayList;
import java.util.List;

public class SensitivityClassification {
    private List<Label> labels;
    private List<InformationType> informationTypes;
    private List<ColumnSensitivity> columnSensitivities;

    // Creating new ArrayList here assures that 'informationTypes' and 'labels' properties will not be null.
    // The Count of the ColumnSensitivities property will be equal to the number of output columns for the query result set.
    public SensitivityClassification(List<Label> labels,
            List<InformationType> informationTypes,
            List<ColumnSensitivity> columnSensitivity) {
        this.labels = new ArrayList<Label>(labels);
        this.informationTypes = new ArrayList<InformationType>(informationTypes);
        this.columnSensitivities = new ArrayList<ColumnSensitivity>(columnSensitivity);
    }

    public List<Label> getLabels() {
        return labels;
    }

    public List<InformationType> getInformationTypes() {
        return informationTypes;
    }

    public List<ColumnSensitivity> getColumnSensitivities() {
        return columnSensitivities;
    }
}
