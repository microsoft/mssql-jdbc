// ---------------------------------------------------------------------------------------------------------------------------------
// File: DBResultSet.java
//
//
// Microsoft JDBC Driver for SQL Server
// Copyright(c) Microsoft Corporation
// All rights reserved.
// MIT License
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files(the "Software"),
// to deal in the Software without restriction, including without limitation the
// rights to use, copy, modify, merge, publish, distribute, sublicense,
// and / or sell copies of the Software, and to permit persons to whom the
// Software is furnished to do so, subject to the following conditions :
// The above copyright notice and this permission notice shall be included in
// all copies or substantial portions of the Software.
// THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS
// IN THE SOFTWARE.
// ---------------------------------------------------------------------------------------------------------------------------------

package com.microsoft.sqlserver.testframework;

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;


import static org.junit.jupiter.api.Assertions.fail;

/**
 * wrapper class for ResultSet
 * 
 * @author Microsoft
 *
 */
public class DBResultSet extends AbstractParentWrapper {

    // TODO: add cursors
    // TODO: add resultSet level holdability
    // TODO: add concurrency control
    public DBTable currentTable;
    public static final int VERIFY_MOVERS_NEXT = 0x100;
    public int _currentrow = 0;       // The row this rowset is currently pointing to

    ResultSet resultSet = null;

    DBResultSet(DBStatement dbstatement, ResultSet internal) {
        super(dbstatement, internal, "resultSet");
        resultSet = internal;
    }

    /**
     * Close the ResultSet object
     * 
     * @throws SQLException
     */
    public void close() throws SQLException {
        if (null != resultSet) {
            resultSet.close();
        }
    }

    /**
     * 
     * @return true new row is valid
     * @throws SQLException
     */
    public boolean next() throws SQLException {
//        if(_currentrow < DBTa)
        _currentrow++;
        return resultSet.next();
    }

    /**
     * 
     * @param index
     * @return Object with the column value
     * @throws SQLException
     */
    public Object getObject(int index) throws SQLException {
        // call individual getters based on type
        return resultSet.getObject(index);
    }

    /**
     * 
     * @param index
     * @return
     */
    public void updateObject(int index) throws SQLException {
        // TODO: update object based on cursor type
    }
    
    /**
     * 
     * @throws SQLException
     */
    public void verify(DBTable table) throws SQLException {
        currentTable = table;
        DBResultSetMetaData metaData = this.getMetaData();
        metaData.verify();

        while (this.next())
            this.verifyCurrentRow();
    }
    
    /**
     * @throws SQLException 
     * 
     */
    public void verifyCurrentRow() throws SQLException{
        int size = ((ResultSet) product()).getMetaData().getColumnCount();
        
        Class _class = Object.class;
        for ( int i=0; i< size; i++)
            verifydata(i, _class, null);
        
    }
    
    public void verifydata(int ordinal, Class coercion, Object arg) throws SQLException
    {
                Object backendData =  this.currentrow().get(ordinal);
                       
                //getXXX - default mapping
                Object retrieved = this.getXXX(ordinal +1 , coercion);

                //Verify
                verifydata(ordinal, coercion, backendData, retrieved);
    }
    
    public void verifydata(int ordinal, Class coercion, Object backendData, Object retrieved) throws SQLException
    {
        
        if (backendData != null) {
            if (retrieved instanceof BigDecimal) {
                if (((BigDecimal) retrieved).compareTo(new BigDecimal("" + backendData)) != 0)                
                    fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                            + " , inserted value is " + backendData);
                  
            } else if (retrieved instanceof Float) {
                if (Float.compare(new Float("" + backendData), (float) retrieved) != 0)
                    fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                            + " ,inserted value is " + backendData);
            } else if (retrieved instanceof Double) {
                if (Double.compare(new Double("" + backendData), (double) retrieved) != 0)
                    fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                            + " , inserted value is " + backendData);
            } else if (retrieved instanceof byte[]) {
                if (!parseByte((byte[]) retrieved).contains("" + backendData))
                    fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                            + " , inserted value is " + backendData);
            } else if (retrieved instanceof String) {
                if (!(((String) retrieved).trim()).equalsIgnoreCase(((String) backendData).trim()))
                    fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                            + " , inserted value is " + backendData);
            } else if (retrieved instanceof Boolean) {
                 if ( retrieved.equals(true) && !backendData.equals(1))
                 {
                    fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                            + " , inserted value is " + backendData);
                 }
                 else if ( retrieved.equals(false) && !backendData.equals(0))
                 {
                     fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved
                             + " , inserted value is " + backendData);
                  }
            } else if (!(("" + retrieved).equalsIgnoreCase("" + backendData)))
                fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved + " , inserted value is "
                        + backendData);
        }
        // if data is null
        else {
            if (retrieved != backendData)
                fail(" Verification failed at index: " + ordinal + " , retrieved value: " + retrieved + " , inserted value is "
                        + backendData);
        }
        
    }
    
    
    public ArrayList<Object> currentrow(){
        return currentTable.getAllRows().get(_currentrow -1);
    }
    
    private Object getXXX(int idx, Class coercion) throws SQLException {
        if (coercion == Object.class) {
            return this.getObject(idx);
        }
        return null;
    }

    /**
     * 
     * @return
     */
    public DBResultSetMetaData getMetaData() {
        ResultSetMetaData product = null;
        DBResultSetMetaData wrapper = null;
        try {
            product = resultSet.getMetaData();
            wrapper = new DBResultSetMetaData(parent, product, name);
        }
        catch (SQLException e) {
            fail(e.getMessage());
        }

        return wrapper;
    }
    
    /**
     * 
     * @return
     * @throws SQLException
     */
    public int getRow() throws SQLException
    {
        int product = ((ResultSet) product()).getRow();
        return product;
    }
    
    /**
     * 
     * @return
     * @throws SQLException
     */
    public boolean previous() throws SQLException
    {
     
        boolean validrow = ((ResultSet) product()).previous();

        if (_currentrow > 0)
        {
            _currentrow--;
        }     
        return (validrow);
    }
    
    /**
     * 
     * @throws SQLException
     */
    public void afterLast() throws SQLException
    {    
        ((ResultSet) product()).afterLast();
        _currentrow = DBTable.getTotalRows() + 1;
    }
    
    public boolean absolute(int x) throws SQLException
    {    
        boolean validrow = ((ResultSet) product()).absolute(x);   
        return validrow;
    }

    private static String parseByte(byte[] bytes) {
        StringBuffer parsedByte = new StringBuffer();
        parsedByte.append("0x");
        for (byte b : bytes) {
            parsedByte.append(String.format("%02X", b));
        }
        return parsedByte.toString();
    }
}
