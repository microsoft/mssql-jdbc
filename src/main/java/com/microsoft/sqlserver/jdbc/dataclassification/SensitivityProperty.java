package com.microsoft.sqlserver.jdbc.dataclassification;

public class SensitivityProperty {
    private Label label;
    private InformationType informationType;

    public SensitivityProperty(Label label,
            InformationType informationType) {
        this.label = label;
        this.informationType = informationType;
    }

    public Label getLabel() {
        return label;
    }

    public InformationType getInformationType() {
        return informationType;
    }
}
