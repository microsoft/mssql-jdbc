/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

/**
 * Specifies the spatial data types values
 */
enum InternalSpatialDatatype {
    POINT((byte) 1, "POINT"),
    LINESTRING((byte) 2, "LINESTRING"),
    POLYGON((byte) 3, "POLYGON"),
    MULTIPOINT((byte) 4, "MULTIPOINT"),
    MULTILINESTRING((byte) 5, "MULTILINESTRING"),
    MULTIPOLYGON((byte) 6, "MULTIPOLYGON"),
    GEOMETRYCOLLECTION((byte) 7, "GEOMETRYCOLLECTION"),
    CIRCULARSTRING((byte) 8, "CIRCULARSTRING"),
    COMPOUNDCURVE((byte) 9, "COMPOUNDCURVE"),
    CURVEPOLYGON((byte) 10, "CURVEPOLYGON"),
    FULLGLOBE((byte) 11, "FULLGLOBE"),
    INVALID_TYPE((byte) 0, null);

    private byte typeCode;
    private String typeName;
    private static final InternalSpatialDatatype[] VALUES = values();

    private InternalSpatialDatatype(byte typeCode, String typeName) {
        this.typeCode = typeCode;
        this.typeName = typeName;
    }

    byte getTypeCode() {
        return this.typeCode;
    }

    String getTypeName() {
        return this.typeName;
    }

    static InternalSpatialDatatype valueOf(byte typeCode) {
        for (InternalSpatialDatatype internalType : VALUES) {
            if (internalType.typeCode == typeCode) {
                return internalType;
            }
        }
        return INVALID_TYPE;
    }
}
