/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc;

/**
 * This class holds information regarding the basetype of a sql_variant data. 
 *
 */
public class SqlVariant {

    private int baseType;
    private int cbPropsActual;
    private int properties;
    private int value;
    private int precision;
    private int scale;
    private int maxLength;  // for Character basetypes in sqlVariant
    private SQLCollation collation; //for Character basetypes in sqlVariant
    private boolean isBaseTypeTime = false;  //we need this when we need to read time as timestamp (for instance in bulkcopy)
    private JDBCType baseJDBCType;
    
    boolean isBaseTypeTimeValue(){
        return this.isBaseTypeTime;
    }
    
    void setIsBaseTypeTimeValue(boolean isBaseTypeTime){
        this.isBaseTypeTime = isBaseTypeTime;
    }
    
    /**
     * Constructor for sqlVariant
     */
     SqlVariant(int baseType) {
        this.baseType = baseType;
    }
    
     void setBaseType(int baseType){
        this.baseType = baseType;
    }
     
     void setBaseJDBCType(JDBCType baseJDBCType){
         this.baseJDBCType = baseJDBCType;
     }
     
     JDBCType getBaseJDBCType() {
         return this.baseJDBCType;
     }
    
     void setScale(int scale){
        this.scale = scale;
    }
    
     void setPrecision(int precision){
        this.precision = precision;
    }
    
     int getPrecision(){
         return this.precision;
     }
     
     int getScale() {
         return this.scale;
     }
     
     void setCollation (SQLCollation collation){
         this.collation = collation;
     }
     
     SQLCollation getCollation(){
         return this.collation;
     }
     
     void setMaxLength(int maxLength){
         this.maxLength = maxLength;
     }
     
     int getMaxLength(){
         return this.maxLength;
     }
}
