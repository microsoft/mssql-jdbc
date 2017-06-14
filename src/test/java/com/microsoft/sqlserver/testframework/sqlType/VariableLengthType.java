/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.testframework.sqlType;

/**
 * Used to identify if the SQL type is of fixed length, or has Precision or Scale
 */
public enum VariableLengthType {
    Fixed,      // primitive types with fixed Length
    Precision,  // variable length type that just has precision char/varchar
    Scale,      // variable length type with scale and precision
    ScaleOnly,  // variable length type with just scale like Time
    Variable
}
