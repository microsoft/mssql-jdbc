package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.Calendar;

import com.microsoft.sqlserver.jdbc.SQLServerConnection;
import com.microsoft.sqlserver.jdbc.SQLServerStatementColumnEncryptionSetting;

/**
 * Utility class for Always Encrypted testing
 */
public class Util {
	
	static PreparedStatement getPreparedStmt(Connection connection, String sql, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException
	{
		if (null == stmtColEncSetting)
		{
			return ((SQLServerConnection) connection).prepareStatement(sql);
		}
		else
		{
			return ((SQLServerConnection) connection).prepareStatement(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability(), stmtColEncSetting);
		}
	}
	
	//default getStatement assumes resultSet is type_forward_only and concur_read_only
	static Statement getStatement(Connection connection, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException
	{
		if (null == stmtColEncSetting)
		{
			return ((SQLServerConnection) connection).createStatement();
		}
		else
		{
			return ((SQLServerConnection) connection).createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability(), stmtColEncSetting);
		}
	}
	
	static Statement getScrollableStatement(Connection connection) throws SQLException
	{
			return ((SQLServerConnection) connection).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE);
	}

	static Statement getScrollableStatement(Connection connection, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException
	{
		return ((SQLServerConnection) connection).createStatement(ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.TYPE_SCROLL_SENSITIVE, ResultSet.CONCUR_UPDATABLE, stmtColEncSetting);
	}
	
	//overloaded getStatement allows setting resultSet type
	static Statement getStatement(Connection connection, SQLServerStatementColumnEncryptionSetting stmtColEncSetting, int rsScrollSensitivity, int rsConcurrence) throws SQLException
	{
		if (null == stmtColEncSetting)
		{
			return ((SQLServerConnection) connection).createStatement(rsScrollSensitivity, rsConcurrence, connection.getHoldability());
		}
		else
		{
			return ((SQLServerConnection) connection).createStatement(rsScrollSensitivity, rsConcurrence, connection.getHoldability(), stmtColEncSetting);
		}
	}
	
	static CallableStatement getCallableStmt(Connection connection, String sql, SQLServerStatementColumnEncryptionSetting stmtColEncSetting) throws SQLException
	{
		if (null == stmtColEncSetting)
		{
			return ((SQLServerConnection) connection).prepareCall(sql);
		}
		else
		{
			return ((SQLServerConnection) connection).prepareCall(sql, ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY, connection.getHoldability(), stmtColEncSetting);
		}
	}
	/*
	//new autogen methods
	static SQLServerPreparedStatement getAutoGenPreparedStmt(Connection connection, String sql, SQLServerStatementColumnEncryptionSetting stmtColEncSetting, AutoGen autoGenParam, int columnIndex, String columnName) throws SQLException
	{
		if (null == stmtColEncSetting)
		{
			if(AutoGen.AUTOGEN_NO_GENERATED_KEYS == autoGenParam){
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, Statement.NO_GENERATED_KEYS);
			}
			else if(AutoGen.AUTOGEN_RETURN_GENERATED_KEYS == autoGenParam){
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, Statement.RETURN_GENERATED_KEYS);
			}
			else if(AutoGen.COLUMN_INDEX == autoGenParam){
				int[] colIndex = {columnIndex}; 
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, colIndex);
			}
			else{//AutoGen.COLUMN_NAME
				String[] colName = {columnName};
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, colName);
			}
		}
		else
		{	
			if(AutoGen.AUTOGEN_NO_GENERATED_KEYS == autoGenParam){
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, Statement.NO_GENERATED_KEYS, stmtColEncSetting);
			}
			else if(AutoGen.AUTOGEN_RETURN_GENERATED_KEYS == autoGenParam){
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, Statement.RETURN_GENERATED_KEYS, stmtColEncSetting);
			}
			else if(AutoGen.COLUMN_INDEX == autoGenParam){
				int[] colIndex = {columnIndex}; 
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, colIndex, stmtColEncSetting);
			}
			else{//AutoGen.COLUMN_NAME
				String[] colName = {columnName};
				return (SQLServerPreparedStatement) ((SQLServerConnection) connection).prepareStatement(sql, colName, stmtColEncSetting);
			}	
		}
	}
	*/
	
	public static Object roundSmallDateTimeValue(Object value)
	{
		if(value == null)
		{
			return null;
		}

		Calendar cal;
		java.sql.Timestamp ts = null;
		int nanos = -1;

		if(value instanceof Calendar)
		{
			cal = (Calendar)value;
		}
		else
		{
			ts = (java.sql.Timestamp)value;
			cal = Calendar.getInstance();
			cal.setTimeInMillis(ts.getTime());
			nanos = ts.getNanos();
		}

		//round to the nearest minute
		double seconds = cal.get(Calendar.SECOND) + (nanos == -1 ? ((double)cal.get(Calendar.MILLISECOND) / 1000) : ((double)nanos / 1000000000));
		if(seconds > 29.998)
		{
			cal.set(Calendar.MINUTE, cal.get(Calendar.MINUTE) + 1);
		}
		cal.set(Calendar.SECOND, 0);
		cal.set(Calendar.MILLISECOND, 0);
		nanos = 0;

		//required to force computation
		cal.getTimeInMillis();

		//return appropriate value
		if(value instanceof Calendar)
		{
			return cal;
		}
		else
		{
			ts.setTime(cal.getTimeInMillis());
			ts.setNanos(nanos);
			return ts;
		}
	}
	
	public static Object roundDatetimeValue(Object value)
	{
		if(value == null)
			return null;
		Timestamp ts = value instanceof Timestamp ? (Timestamp)value : new Timestamp(((Calendar)value).getTimeInMillis());
		int millis = ts.getNanos() / 1000000;
		int lastDigit = (int)(millis % 10);
		switch(lastDigit)
		{
		//0, 1 -> 0
		case 1: ts.setNanos((millis - 1) * 1000000);
		break;

		//2, 3, 4 -> 3
		case 2: ts.setNanos((millis + 1) * 1000000);
		break;
		case 4: ts.setNanos((millis - 1) * 1000000);
		break;

		//5, 6, 7, 8 -> 7
		case 5: ts.setNanos((millis + 2) * 1000000);
		break;
		case 6: ts.setNanos((millis + 1) * 1000000);
		break;	
		case 8: ts.setNanos((millis - 1) * 1000000);
		break;

		//9 -> 0 with overflow
		case 9: ts.setNanos(0);
		ts.setTime(ts.getTime() + millis + 1);
		break;

		//default, i.e. 0, 3, 7 -> 0, 3, 7
		//don't change the millis but make sure that any 
		//sub-millisecond digits are zeroed out
		default: ts.setNanos((millis) * 1000000);
		}
		if(value instanceof Calendar)
		{
			((Calendar)value).setTimeInMillis(ts.getTime());
			((Calendar)value).getTimeInMillis();
			return value;
		}
		return ts;
	}
}
