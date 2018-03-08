package com.microsoft.sqlserver.jdbc;

public enum InternalSpatialDatatype {
    POINT((byte)1, "POINT"),
    LINESTRING((byte)2, "LINESTRING"),
    POLYGON((byte)3, "POLYGON"),
    MULTIPOINT((byte)4, "MULTIPOINT"),
    MULTILINESTRING((byte)5, "MULTILINESTRING"),
    MULTIPOLYGON((byte)6, "MULTIPOLYGON"),
    GEOMETRYCOLLECTION((byte)7, "GEOMETRYCOLLECTION"),
    CIRCULARSTRING((byte)8, "CIRCULARSTRING"),
    COMPOUNDCURVE((byte)9, "COMPOUNDCURVE"),
    CURVEPOLYGON((byte)10, "CURVEPOLYGON"),
    FULLGLOBE((byte)11, "FULLGLOBE"),
    INVALID_TYPE((byte)0, null);
    
    private byte typeCode;
    private String typeName;
    
    private InternalSpatialDatatype(byte typeCode, String typeName) {
        this.typeCode = typeCode;
        this.typeName = typeName;
    }
    
    public byte getTypeCode() {
        return this.typeCode;
    }
    
    public String getTypeName() {
        return this.typeName;
    }
    
    public static InternalSpatialDatatype valueOf(byte typeCode) {
        for (InternalSpatialDatatype internalType : values()) {
            if (internalType.typeCode == typeCode) {
                return internalType;
            }
        }
        return INVALID_TYPE;
    }
}
