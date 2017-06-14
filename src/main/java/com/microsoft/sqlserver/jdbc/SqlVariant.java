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
    
    /**
     * Check if the basetype for variant is of time value
     * @return
     */
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
    
     /**
      * store the base type for sql-variant
      * @param baseType
      */
     void setBaseType(int baseType){
        this.baseType = baseType;
    }
     
     /**
      * retrieves the base type for sql-variant
      * @return
      */
     int getBaseType(){
         return this.baseType;
     }
     
     /**
      * Store the basetype as jdbc type
      * @param baseJDBCType
      */
     void setBaseJDBCType(JDBCType baseJDBCType){
         this.baseJDBCType = baseJDBCType;
     }
     
     /**
      * retrieves the base type as jdbc type
      * @return
      */
     JDBCType getBaseJDBCType() {
         return this.baseJDBCType;
     }
    
     /**
      * stores the scale if applicable
      * @param scale
      */
     void setScale(int scale){
        this.scale = scale;
    }
    
     /**
      * stores the precision if applicable
      * @param precision
      */
     void setPrecision(int precision){
        this.precision = precision;
    }
    
     /**
      * retrieves the precision
      * @return
      */
     int getPrecision(){
         return this.precision;
     }
     
     /**
      * retrieves the scale
      * @return
      */
     int getScale() {
         return this.scale;
     }
     
     /**
      * stores the collation if applicable
      * @param collation
      */
     void setCollation (SQLCollation collation){
         this.collation = collation;
     }
     
     /**
      * Retrieves the collation
      * @return
      */
     SQLCollation getCollation(){
         return this.collation;
     }
     
     /**
      * stores the maximum length 
      * @param maxLength
      */
     void setMaxLength(int maxLength){
         this.maxLength = maxLength;
     }
     
     /**
      * retrieves the maximum length
      * @return
      */
     int getMaxLength(){
         return this.maxLength;
     }
}
