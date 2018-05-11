package com.microsoft.sqlserver.jdbc;

public class Label {
    public String name;
    public String id;

	public Label(String name, String id) {
        this.name = name;
        this.id = id;
    }
	
    public String getName() {
		return name;
	}

	public String getId() {
		return id;
	}
}
