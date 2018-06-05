package com.microsoft.sqlserver.jdbc.dataclassification;

public class Label {
    private String name;
    private String id;

    public Label(String name,
            String id) {
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
