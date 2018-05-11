package com.microsoft.sqlserver.jdbc;

public class SensitivityProperty {
    public Label label;
    public InformationType informationType;

	public SensitivityProperty(Label label, InformationType informationType) {
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
