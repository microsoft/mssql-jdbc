/**
 * 
 */
package com.microsoft.sqlserver.testframework;

import java.sql.ResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerResultSetMetaData;
import java.sql.SQLException;
import static org.junit.Assert.fail;


/**
 * @author v-afrafi
 *
 */
public class DBResultSetMetaData extends AbstractParentWrapper {

    /**
     * @param parent
     * @param internal
     * @param name
     */
    DBResultSetMetaData(AbstractParentWrapper parent, Object internal, String name) {
        super(parent, internal, name);
        // TODO Auto-generated constructor stub
    }

    public void verify() throws SQLException {
        // getColumnCount
        int columns = this.getColumnCount();

        // Loop through the columns
        for (int i = 1; i <= columns; i++) {
            // Note: Just calling these performs the verification, in each method
            this.getColumnName(i);
            this.getColumnType(i);
            this.getColumnTypeName(i);
            this.getScale(i);
            this.isCaseSensitive(i);
            this.isAutoIncrement(i);
            this.isCurrency(i);
            this.isNullable(i);
            this.isSigned(i);

        }
    }

    public int getColumnCount() throws SQLException {
        int current = 0;
        try {
            current = ( (SQLServerResultSetMetaData) product()).getColumnCount();
        }
        catch (SQLException e) {
           fail(e.toString());
        }
        return current;
    }

    public String getColumnName(int index) throws SQLException {
        String current = ( (SQLServerResultSetMetaData) product()).getColumnName(index);
        return current;
    }

    public int getColumnType(int index) throws SQLException {
        int current = ( (SQLServerResultSetMetaData) product()).getColumnType(index);
        return current;
    }

    public String getColumnTypeName(int index) throws SQLException {
        String current = ( (SQLServerResultSetMetaData) product()).getColumnTypeName(index);
        return current;
    }

    public int getPrecision(int x) throws SQLException {
        int current = ( (SQLServerResultSetMetaData) product()).getPrecision(x);
        return current;
    }

    public int getScale(int x) throws SQLException {
        int current = ( (SQLServerResultSetMetaData) product()).getScale(x);
        return current;
    }

    public boolean isCaseSensitive(int x) throws SQLException {
        boolean current = ( (SQLServerResultSetMetaData) product()).isCaseSensitive(x);
        return current;
    }

    public boolean isCurrency(int x) throws SQLException {
        boolean current = ( (SQLServerResultSetMetaData) product()).isCurrency(x);
        return current;
    }

    public boolean isAutoIncrement(int x) throws SQLException {
        boolean current = ( (SQLServerResultSetMetaData) product()).isAutoIncrement(x);
        return current;
    }

    public int isNullable(int x) throws SQLException {
        int current = ( (SQLServerResultSetMetaData) product()).isNullable(x);
        return current;
    }

    public boolean isSigned(int x) throws SQLException {
        boolean current = ( (SQLServerResultSetMetaData) product()).isSigned(x);
        return current;
    }
}
