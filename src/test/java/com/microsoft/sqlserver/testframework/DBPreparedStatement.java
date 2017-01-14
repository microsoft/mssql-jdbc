/**
 * 
 */
package com.microsoft.sqlserver.testframework;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import com.microsoft.sqlserver.testframework.sqlType.SqlDateTimeOffset;
/**
 * @author v-afrafi
 *
 */
public class DBPreparedStatement extends AbstractParentWrapper {

    PreparedStatement pstmt = null;
    DBResultSet dbresultSet = null;

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBPreparedStatement(AbstractParentWrapper parent, Object internal, String name) {
        super(parent, internal, name);
    }

    /**
     * @throws SQLException
     * 
     */
    DBPreparedStatement prepareStatement(String query) throws SQLException {
        pstmt = ( (Connection) product()).prepareStatement(query);
        setInternal(pstmt);
        return this;
    }

    @Override
    void setInternal(Object internal) {
        this.internal = internal;
    }

    /**
     * @param i
     * @param bigDecimal
     * @throws SQLException 
     */
    public void setObject(int parameterIndex, Object targetObject) throws SQLException {

        ((PreparedStatement) product()).setObject(parameterIndex, targetObject);

    }
    
    public DBResultSet     executeQuery() throws SQLException
    {
       ResultSet rs = null;
       rs = pstmt.executeQuery();
       dbresultSet = new DBResultSet(this, rs);
//        DBResultSet product =  ((PreparedStatement) product()).executeQuery();
       return dbresultSet;
      
    }
//    //Used to return the appropriate object given the framework wrapper
//    public Object getTargetObject(Object value)
//    {
//        if(value instanceof sqlDateTimeOffset)
//        {
//            return ((DBDateTimeOffset)value).product();
//        }
//        else
//        {
//            return value;
//        }
//    }

}
