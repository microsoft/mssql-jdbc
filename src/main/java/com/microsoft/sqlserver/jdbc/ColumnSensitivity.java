package com.microsoft.sqlserver.jdbc;

import java.util.ArrayList;
import java.util.List;

public class ColumnSensitivity {
    public List<SensitivityProperty> sensitivityProperties;

	public ColumnSensitivity(List<SensitivityProperty> sensitivityProperties) {
        this.sensitivityProperties = new ArrayList<SensitivityProperty>(sensitivityProperties);
    }

    public List<SensitivityProperty> getSensitivityProperties() {
		return sensitivityProperties;
	}
}
