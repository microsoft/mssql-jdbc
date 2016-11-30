//---------------------------------------------------------------------------------------------------------------------------------
// File: ISQLServerDataRecord.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
//  to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
//  and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
//  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
//  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
//  IN THE SOFTWARE.
//---------------------------------------------------------------------------------------------------------------------------------
 
 
package com.microsoft.sqlserver.jdbc;

import java.util.Set;

/**
 * The ISQLServerDataRecord interface can be used to create classes that read in data from any source (such as a file)
 * and allow a structured type to be sent to SQL Server tables.
 */

public interface ISQLServerDataRecord 
{
	/**
	 * Get the column meta data
	 * @param column the first column is 1, the second is 2, and so on
	 * @return SQLServerMetaData of column
	 */
	public SQLServerMetaData getColumnMetaData(int column);
	
    /**
     * Get the column count.
     * 
     * @return Set of ordinals for the columns.
     */
    public int getColumnCount();
            
    /**
     * Gets the data for the current row as an array of Objects.
     *  
     * Each Object must match the Java language Type that is used to represent the indicated 
     * JDBC data type for the given column.  For more information, see 
     * 'Understanding the JDBC Driver Data Types' for the appropriate mappings.
     * 
     * @return The data for the row.
     */
    public Object[] getRowData();
    
    /**
     * Advances to the next data row.
     * 
     * @return True if rows are available; false if there are no more rows
     */
    public boolean next();	
}

