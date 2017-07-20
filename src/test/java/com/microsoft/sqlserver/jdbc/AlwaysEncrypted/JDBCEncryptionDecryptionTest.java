/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) Microsoft Corporation All rights reserved.
 * 
 * This program is made available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */
package com.microsoft.sqlserver.jdbc.AlwaysEncrypted;

import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.JDBCType;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.LinkedList;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Test;
import org.junit.platform.runner.JUnitPlatform;
import org.junit.runner.RunWith;
import org.opentest4j.TestAbortedException;

import com.microsoft.sqlserver.jdbc.SQLServerException;
import com.microsoft.sqlserver.jdbc.SQLServerPreparedStatement;
import com.microsoft.sqlserver.jdbc.SQLServerResultSet;
import com.microsoft.sqlserver.jdbc.SQLServerStatement;

import microsoft.sql.DateTimeOffset;

/**
 * Tests Decryption and encryption of values
 *
 */
@RunWith(JUnitPlatform.class)
public class JDBCEncryptionDecryptionTest extends AESetup {
    private SQLServerPreparedStatement pstmt = null;

	private boolean nullable = false;
    private String[] numericValues = null;
    private String[] numericValues2 = null;
	private String[] numericValuesNull = null;
	private String[] numericValuesNull2 = null;
	private String[] charValues  = null;
	
	private LinkedList<byte[]> byteValuesSetObject = null;
	private LinkedList<byte[]> byteValuesNull = null;
	
	private LinkedList<Object> dateValues = null;
    
    /**
     * Junit test case for char set string for string values
     * @throws SQLException
     */
    @Test
	public void testChar_SpecificSetter() throws SQLException {
		charValues = createCharValues();
		dropTables();
		createCharTable();
		populateCharNormalCase(charValues);
		testChar(stmt, charValues);
		testChar(null, charValues);
	}
    
    /**
     * Junit test case for char set object for string values
     * @throws SQLException
     */
    @Test
	public void testChar_SetObject() throws SQLException {
		charValues = createCharValues();
		dropTables();
		createCharTable();
		populateCharSetObject(charValues);
		testChar(stmt, charValues);
		testChar(null, charValues);
	}
    
    /**
     * Junit test case for char set object for jdbc string values
     * @throws SQLException
     */
    @Test
	public void testChar_SetObject_WithJDBCTypes() throws SQLException {
    	skipTestForJava7();
    	
		charValues = createCharValues();
		dropTables();
		createCharTable();
		populateCharSetObjectWithJDBCTypes(charValues);
		testChar(stmt, charValues);
		testChar(null, charValues);
	}
    
    /**
     * Junit test case for char set string for null values
     * @throws SQLException
     */
    @Test
	public void testChar_SpecificSetter_Null() throws SQLException {
		String[] charValuesNull = { null, null, null, null, null, null, null, null, null };
		dropTables();
		createCharTable();
		populateCharNormalCase(charValuesNull);
		testChar(stmt, charValuesNull);
		testChar(null, charValuesNull);
	}
    
    /**
     * Junit test case for char set object for null values
     * @throws SQLException
     */
    @Test
	public void testChar_SetObject_Null() throws SQLException {
		String[] charValuesNull = { null, null, null, null, null, null, null, null, null };
		dropTables();
		createCharTable();
		populateCharSetObject(charValuesNull);
		testChar(stmt, charValuesNull);
		testChar(null, charValuesNull);
	}
    
    /**
     * Junit test case for char set null for null values
     * @throws SQLException
     */
    @Test
	public void testChar_SetNull() throws SQLException {
		String[] charValuesNull = { null, null, null, null, null, null, null, null, null };
		dropTables();
		createCharTable();
		populateCharNullCase();
		testChar(stmt, charValuesNull);
		testChar(null, charValuesNull);
	}
    
    /**
     * Junit test case for binary set binary for binary values
     * @throws SQLException
     */
    @Test
	public void testBinary_SpecificSetter() throws SQLException {
		LinkedList<byte[]> byteValues = createbinaryValues(false);
		dropTables();
		createBinaryTable();
		populateBinaryNormalCase(byteValues);
		testBinary(stmt, byteValues);
		testBinary(null, byteValues);
	}
    
    /**
     * Junit test case for binary set object for binary values
     * @throws SQLException
     */
    @Test
	public void testBinary_Setobject() throws SQLException {
		byteValuesSetObject = createbinaryValues(false);
		dropTables();
		createBinaryTable();
		populateBinarySetObject(byteValuesSetObject);
		testBinary(stmt, byteValuesSetObject);
		testBinary(null, byteValuesSetObject);
	}
    
    /**
     * Junit test case for binary set null for binary values
     * @throws SQLException
     */
    @Test
	public void testBinary_SetNull() throws SQLException {
		byteValuesNull = createbinaryValues(true);
		dropTables();
		createBinaryTable();
		populateBinaryNullCase();
		testBinary(stmt, byteValuesNull);
		testBinary(null, byteValuesNull);
	}
    
    /**
     * Junit test case for binary set binary for null values
     * @throws SQLException
     */
    @Test
	public void testBinary_SpecificSetter_Null() throws SQLException {
		byteValuesNull = createbinaryValues(true);
		dropTables();
		createBinaryTable();
		populateBinaryNormalCase(null);
		testBinary(stmt, byteValuesNull);
		testBinary(null, byteValuesNull);
	}
    
    /**
     * Junit test case for binary set object for null values
     * @throws SQLException
     */
    @Test
	public void testBinary_setObject_Null() throws SQLException {
		byteValuesNull = createbinaryValues(true);
		dropTables();
		createBinaryTable();
		populateBinarySetObject(null);
		testBinary(stmt, byteValuesNull);
		testBinary(null, byteValuesNull);
	}
    
    /**
     * Junit test case for binary set object for jdbc type binary values
     * @throws SQLException
     */
    @Test
	public void testBinary_SetObject_WithJDBCTypes() throws SQLException {
    	skipTestForJava7();
    	
		byteValuesSetObject = createbinaryValues(false);
		dropTables();
		createBinaryTable();
		populateBinarySetObjectWithJDBCType(byteValuesSetObject);
		testBinary(stmt, byteValuesSetObject);
		testBinary(null, byteValuesSetObject);
	}
    
    /**
     * Junit test case for date set date for date values
     * @throws SQLException
     */
    @Test
	public void testDate_SpecificSetter() throws SQLException {
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateNormalCase(dateValues);
		testDate(stmt, dateValues);
		testDate(null, dateValues);
	}
    
    /**
     * Junit test case for date set object for date values
     * @throws SQLException
     */
    @Test
	public void testDate_setObject() throws SQLException {
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateSetObject(dateValues, "");
		testDate(stmt, dateValues);
		testDate(null, dateValues);
	}
    
    /**
     * Junit test case for date set object for java date values
     * @throws SQLException
     */
    @Test
	public void testDate_setObject_withJavaType() throws SQLException {
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateSetObject(dateValues, "setwithJavaType");
		testDate(stmt, dateValues);
		testDate(null, dateValues);
	}
    
    /**
     * Junit test case for date set object for jdbc date values
     * @throws SQLException
     */
    @Test
	public void testDate_setObject_withJDBCType() throws SQLException {
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateSetObject(dateValues, "setwithJDBCType");
		testDate(stmt, dateValues);
		testDate(null, dateValues);
	}
    
    /**
     * Junit test case for date set date for min/max date values
     * @throws SQLException
     */
    @Test
	public void testDate_SpecificSetter_MinMaxValue() throws SQLException {
		RandomData.returnMinMax = true;
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateNormalCase(dateValues);
		testDate(stmt, dateValues);
		testDate(null, dateValues);
	}
    
    /**
     * Junit test case for date set date for null values
     * @throws SQLException
     */
    @Test
	public void testDate_SetNull() throws SQLException {
		RandomData.returnNull = true;
		nullable = true;
		
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateNullCase();
		testDate(stmt, dateValues);
		testDate(null, dateValues);
		
		nullable = false;
		RandomData.returnNull = false;
	}
    
    /**
     * Junit test case for date set object for null values
     * @throws SQLException
     */
    @Test
	public void testDate_SetObject_Null() throws SQLException {
		RandomData.returnNull = true;
		nullable = true;
		
		dateValues = createTemporalTypes();
		dropTables();
		createDateTable();
		populateDateSetObjectNull();
		testDate(stmt, dateValues);
		testDate(null, dateValues);
		
		nullable = false;
		RandomData.returnNull = false;
	}

    /**
     * Junit test case for numeric set numeric for numeric values
     * @throws SQLException
     */
    @Test
    public void testNumeric_SpecificSetter() throws TestAbortedException, Exception {
		numericValues = createNumericValues();
		numericValues2 = new String[numericValues.length];
		System.arraycopy(numericValues, 0, numericValues2, 0, numericValues.length);

		dropTables();
    	createNumericTable();
        populateNumeric(numericValues);
		testNumeric(stmt, numericValues, false);
		testNumeric(null, numericValues2, false);
    }

    /**
     * Junit test case for numeric set object for numeric values
     * @throws SQLException
     */
    @Test
	public void testNumeric_SetObject() throws SQLException {
		numericValues = createNumericValues();
		numericValues2 = new String[numericValues.length];
		System.arraycopy(numericValues, 0, numericValues2, 0, numericValues.length);
		
		dropTables();
		createNumericTable();
		populateNumericSetObject(numericValues);
		testNumeric(null, numericValues, false);
		testNumeric(stmt, numericValues2, false);
	}

    /**
     * Junit test case for numeric set object for jdbc type numeric values
     * @throws SQLException
     */
    @Test
	public void testNumeric_SetObject_With_JDBCTypes() throws SQLException {
    	skipTestForJava7();
    	
		numericValues = createNumericValues();
		numericValues2 = new String[numericValues.length];
		System.arraycopy(numericValues, 0, numericValues2, 0, numericValues.length);
		
		dropTables();
		createNumericTable();
		populateNumericSetObjectWithJDBCTypes(numericValues);
		testNumeric(stmt, numericValues, false);
		testNumeric(null, numericValues2, false);
	}

    /**
     * Junit test case for numeric set numeric for max numeric values
     * @throws SQLException
     */
    @Test
	public void testNumeric_SpecificSetter_MaxValue() throws SQLException {
		String[] numericValuesBoundaryPositive = { "true", "255", "32767", "2147483647", "9223372036854775807",
				"1.79E308", "1.123", "3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
				"567812.78", "214748.3647", "922337203685477.5807", "999999999999999999999999.9999",
		"999999999999999999999999.9999" };
		String[] numericValuesBoundaryPositive2 = { "true", "255", "32767", "2147483647", "9223372036854775807",
				"1.79E308", "1.123", "3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
				"567812.78", "214748.3647", "922337203685477.5807", "999999999999999999999999.9999",
		"999999999999999999999999.9999" };
		
		dropTables();
		createNumericTable();
		populateNumeric(numericValuesBoundaryPositive);
		testNumeric(stmt, numericValuesBoundaryPositive, false);
		testNumeric(null, numericValuesBoundaryPositive2, false);
	}
    
    /**
     * Junit test case for numeric set numeric for min numeric values
     * @throws SQLException
     */
    @Test
	public void testNumeric_SpecificSetter_MinValue() throws SQLException {
		String[] numericValuesBoundaryNegtive = { "false", "0", "-32768", "-2147483648", "-9223372036854775808",
				"-1.79E308", "1.123", "-3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
				"567812.78", "-214748.3648", "-922337203685477.5808", "999999999999999999999999.9999",
		"999999999999999999999999.9999" };
		String[] numericValuesBoundaryNegtive2 = { "false", "0", "-32768", "-2147483648", "-9223372036854775808",
				"-1.79E308", "1.123", "-3.4E38", "999999999999999999", "12345.12345", "999999999999999999",
				"567812.78", "-214748.3648", "-922337203685477.5808", "999999999999999999999999.9999",
		"999999999999999999999999.9999" };
		
		dropTables();
		createNumericTable();
		populateNumeric(numericValuesBoundaryNegtive);
		testNumeric(stmt, numericValuesBoundaryNegtive, false);
		testNumeric(null, numericValuesBoundaryNegtive2, false);
	}
    
    /**
     * Junit test case for numeric set numeric for null values
     * @throws SQLException
     */
    @Test
	public void testNumeric_SpecificSetter_Null() throws SQLException {
		nullable = true;
		RandomData.returnNull = true;
		numericValuesNull = createNumericValues();
		numericValuesNull2 = new String[numericValuesNull.length];
		System.arraycopy(numericValuesNull, 0, numericValuesNull2, 0, numericValuesNull.length);

		dropTables();
		createNumericTable();
		populateNumericNullCase(numericValuesNull);
		testNumeric(stmt, numericValuesNull, true);
		testNumeric(null, numericValuesNull2, true);

		nullable = false;
		RandomData.returnNull = false;
	}
    
    /**
     * Junit test case for numeric set object for null values
     * @throws SQLException
     */
    @Test
	public void testNumeric_SpecificSetter_SetObject_Null() throws SQLException {
		nullable = true;
		RandomData.returnNull = true;
		numericValuesNull = createNumericValues();
		numericValuesNull2 = new String[numericValuesNull.length];
		System.arraycopy(numericValuesNull, 0, numericValuesNull2, 0, numericValuesNull.length);

		dropTables();
		createNumericTable();
		populateNumericSetObjectNull();
		testNumeric(stmt, numericValuesNull, true);
		testNumeric(null, numericValuesNull2, true);

		nullable = false;
		RandomData.returnNull = false;
	}
    
    /**
     * Junit test case for numeric set numeric for null normalization values
     * @throws SQLException
     */
    @Test
	public void testNumeric_Normalization() throws SQLException {
		String[] numericValuesNormalization = { "true", "1", "127", "100", "100", "1.123", "1.123", "1.123",
				"123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
				"999999999999999999999999.9999", "999999999999999999999999.9999" };
		String[] numericValuesNormalization2 = { "true", "1", "127", "100", "100", "1.123", "1.123", "1.123",
				"123456789123456789", "12345.12345", "987654321123456789", "567812.78", "7812.7812", "7812.7812",
				"999999999999999999999999.9999", "999999999999999999999999.9999" };
		dropTables();
		createNumericTable();
		populateNumericNormalization(numericValuesNormalization);
		testNumeric(stmt, numericValuesNormalization, false);
		testNumeric(null, numericValuesNormalization2, false);
	}
    
    /**
     * Dropping all CMKs and CEKs and any open resources.
     * 
     * @throws SQLServerException
     * @throws SQLException
     */
    @AfterAll
    static void dropAll() throws SQLServerException, SQLException {
    	dropTables();
        dropCEK();
        dropCMK();
        if (null != stmt) {
            stmt.close();
        }
        if (null != con) {
            con.close();
        }
    }

	private void populateBinaryNormalCase(LinkedList<byte[]> byteValues) throws SQLException {
		String sql = "insert into " + binaryTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?"
				+ ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// binary20
		for (int i = 1; i <= 3; i++) {
			if (null == byteValues) {
				pstmt.setBytes(i, null);
			} else {
				pstmt.setBytes(i, byteValues.get(0));
			}
		}

		// varbinary50
		for (int i = 4; i <= 6; i++) {
			if (null == byteValues) {
				pstmt.setBytes(i, null);
			} else {
				pstmt.setBytes(i, byteValues.get(1));
			}
		}

		// varbinary(max)
		for (int i = 7; i <= 9; i++) {
			if (null == byteValues) {
				pstmt.setBytes(i, null);
			} else {
				pstmt.setBytes(i, byteValues.get(2));
			}
		}

		// binary(512)
		for (int i = 10; i <= 12; i++) {
			if (null == byteValues) {
				pstmt.setBytes(i, null);
			} else {
				pstmt.setBytes(i, byteValues.get(3));
			}
		}

		// varbinary(8000)
		for (int i = 13; i <= 15; i++) {
			if (null == byteValues) {
				pstmt.setBytes(i, null);
			} else {
				pstmt.setBytes(i, byteValues.get(4));
			}
		}

		pstmt.execute();
	}

	private void populateBinarySetObject(LinkedList<byte[]> byteValues) throws SQLException {
		String sql = "insert into " + binaryTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?"
				+ ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// binary(20)
		for (int i = 1; i <= 3; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, java.sql.Types.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(0));
			}
		}

		// varbinary(50)
		for (int i = 4; i <= 6; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, java.sql.Types.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(1));
			}
		}

		// varbinary(max)
		for (int i = 7; i <= 9; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, java.sql.Types.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(2));
			}
		}

		// binary(512)
		for (int i = 10; i <= 12; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, java.sql.Types.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(3));
			}
		}

		// varbinary(8000)
		for (int i = 13; i <= 15; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, java.sql.Types.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(4));
			}
		}

		pstmt.execute();
	}

	private void populateBinarySetObjectWithJDBCType(LinkedList<byte[]> byteValues) throws SQLException {		
		String sql = "insert into " + binaryTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?"
				+ ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// binary(20)
		for (int i = 1; i <= 3; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, JDBCType.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(0), JDBCType.BINARY);
			}
		}

		// varbinary(50)
		for (int i = 4; i <= 6; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, JDBCType.VARBINARY);
			} else {
				pstmt.setObject(i, byteValues.get(1), JDBCType.VARBINARY);
			}
		}

		// varbinary(max)
		for (int i = 7; i <= 9; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, JDBCType.VARBINARY);
			} else {
				pstmt.setObject(i, byteValues.get(2), JDBCType.VARBINARY);
			}
		}

		// binary(512)
		for (int i = 10; i <= 12; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, JDBCType.BINARY);
			} else {
				pstmt.setObject(i, byteValues.get(3), JDBCType.BINARY);
			}
		}

		// varbinary(8000)
		for (int i = 13; i <= 15; i++) {
			if (null == byteValues) {
				pstmt.setObject(i, null, JDBCType.VARBINARY);
			} else {
				pstmt.setObject(i, byteValues.get(4), JDBCType.VARBINARY);
			}
		}

		pstmt.execute();
	}

	private void populateBinaryNullCase() throws SQLException {
		String sql = "insert into " + binaryTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?"
				+ ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// binary
		for (int i = 1; i <= 3; i++) {
			pstmt.setNull(i, java.sql.Types.BINARY);
		}

		// varbinary, varbinary(max)
		for (int i = 4; i <= 9; i++) {
			pstmt.setNull(i, java.sql.Types.VARBINARY);
		}

		// binary512
		for (int i = 10; i <= 12; i++) {
			pstmt.setNull(i, java.sql.Types.BINARY);
		}

		// varbinary(8000)
		for (int i = 13; i <= 15; i++) {
			pstmt.setNull(i, java.sql.Types.VARBINARY);
		}

		pstmt.execute();
	}

	private void populateCharNormalCase(String[] charValues) throws SQLException {
		String sql = "insert into " + charTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// char
		for (int i = 1; i <= 3; i++) {
			pstmt.setString(i, charValues[0]);
		}

		// varchar
		for (int i = 4; i <= 6; i++) {
			pstmt.setString(i, charValues[1]);
		}

		// varchar(max)
		for (int i = 7; i <= 9; i++) {
			pstmt.setString(i, charValues[2]);
		}

		// nchar
		for (int i = 10; i <= 12; i++) {
			pstmt.setNString(i, charValues[3]);
		}

		// nvarchar
		for (int i = 13; i <= 15; i++) {
			pstmt.setNString(i, charValues[4]);
		}

		// varchar(max)
		for (int i = 16; i <= 18; i++) {
			pstmt.setNString(i, charValues[5]);
		}

		// uniqueidentifier
		for (int i = 19; i <= 21; i++) {
			if (null == charValues[6]) {
				pstmt.setUniqueIdentifier(i, null);
			} else {
				pstmt.setUniqueIdentifier(i, uid);
			}
		}

		// varchar8000
		for (int i = 22; i <= 24; i++) {
			pstmt.setString(i, charValues[7]);
		}

		// nvarchar4000
		for (int i = 25; i <= 27; i++) {
			pstmt.setNString(i, charValues[8]);
		}

		pstmt.execute();
		pstmt.close();
	}

	private void populateCharSetObject(String[] charValues) throws SQLException {
		String sql = "insert into " + charTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// char
		for (int i = 1; i <= 3; i++) {
			pstmt.setObject(i, charValues[0]);
		}

		// varchar
		for (int i = 4; i <= 6; i++) {
			pstmt.setObject(i, charValues[1]);
		}

		// varchar(max)
		for (int i = 7; i <= 9; i++) {
			pstmt.setObject(i, charValues[2], java.sql.Types.LONGVARCHAR);
		}

		// nchar
		for (int i = 10; i <= 12; i++) {
			pstmt.setObject(i, charValues[3], java.sql.Types.NCHAR);
		}

		// nvarchar
		for (int i = 13; i <= 15; i++) {
			pstmt.setObject(i, charValues[4], java.sql.Types.NCHAR);
		}

		// nvarchar(max)
		for (int i = 16; i <= 18; i++) {
			pstmt.setObject(i, charValues[5], java.sql.Types.LONGNVARCHAR);
		}

		// uniqueidentifier
		for (int i = 19; i <= 21; i++) {
			pstmt.setObject(i, charValues[6], microsoft.sql.Types.GUID);
		}

		// varchar8000
		for (int i = 22; i <= 24; i++) {
			pstmt.setObject(i, charValues[7]);
		}

		// nvarchar4000
		for (int i = 25; i <= 27; i++) {
			pstmt.setObject(i, charValues[8], java.sql.Types.NCHAR);
		}

		pstmt.execute();
		pstmt.close();
	}

	private void populateCharSetObjectWithJDBCTypes(String[] charValues) throws SQLException {		
		String sql = "insert into " + charTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// char
		for (int i = 1; i <= 3; i++) {
			pstmt.setObject(i, charValues[0], JDBCType.CHAR);
		}

		// varchar
		for (int i = 4; i <= 6; i++) {
			pstmt.setObject(i, charValues[1], JDBCType.VARCHAR);
		}

		// varchar(max)
		for (int i = 7; i <= 9; i++) {
			pstmt.setObject(i, charValues[2], JDBCType.LONGVARCHAR);
		}

		// nchar
		for (int i = 10; i <= 12; i++) {
			pstmt.setObject(i, charValues[3], JDBCType.NCHAR);
		}

		// nvarchar
		for (int i = 13; i <= 15; i++) {
			pstmt.setObject(i, charValues[4], JDBCType.NVARCHAR);
		}

		// nvarchar(max)
		for (int i = 16; i <= 18; i++) {
			pstmt.setObject(i, charValues[5], JDBCType.LONGNVARCHAR);
		}

		// uniqueidentifier
		for (int i = 19; i <= 21; i++) {
			pstmt.setObject(i, charValues[6], microsoft.sql.Types.GUID);
		}

		// varchar8000
		for (int i = 22; i <= 24; i++) {
			pstmt.setObject(i, charValues[7], JDBCType.VARCHAR);
		}

		// vnarchar4000
		for (int i = 25; i <= 27; i++) {
			pstmt.setObject(i, charValues[8], JDBCType.NVARCHAR);
		}

		pstmt.execute();
		pstmt.close();
	}

	private void populateCharNullCase() throws SQLException {
		String sql = "insert into " + charTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// char
		for (int i = 1; i <= 3; i++) {
			pstmt.setNull(i, java.sql.Types.CHAR);
		}

		// varchar, varchar(max)
		for (int i = 4; i <= 9; i++) {
			pstmt.setNull(i, java.sql.Types.VARCHAR);
		}

		// nchar
		for (int i = 10; i <= 12; i++) {
			pstmt.setNull(i, java.sql.Types.NCHAR);
		}

		// nvarchar, varchar(max)
		for (int i = 13; i <= 18; i++) {
			pstmt.setNull(i, java.sql.Types.NVARCHAR);
		}

		// uniqueidentifier
		for (int i = 19; i <= 21; i++) {
			pstmt.setNull(i, microsoft.sql.Types.GUID);

		}

		// varchar8000
		for (int i = 22; i <= 24; i++) {
			pstmt.setNull(i, java.sql.Types.VARCHAR);
		}

		// nvarchar4000
		for (int i = 25; i <= 27; i++) {
			pstmt.setNull(i, java.sql.Types.NVARCHAR);
		}

		pstmt.execute();
		pstmt.close();
	}

	private void populateDateNormalCase(LinkedList<Object> dateValues) throws SQLException {
		String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?" + ")";

		SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);

		// date
		for (int i = 1; i <= 3; i++) {
			sqlPstmt.setDate(i, (Date) dateValues.get(0));
		}

		// datetime2 default
		for (int i = 4; i <= 6; i++) {
			sqlPstmt.setTimestamp(i, (Timestamp) dateValues.get(1));
		}

		// datetimeoffset default
		for (int i = 7; i <= 9; i++) {
			sqlPstmt.setDateTimeOffset(i, (DateTimeOffset) dateValues.get(2));
		}

		// time default
		for (int i = 10; i <= 12; i++) {
			sqlPstmt.setTime(i, (Time) dateValues.get(3));
		}

		// datetime
		for (int i = 13; i <= 15; i++) {
			sqlPstmt.setDateTime(i, (Timestamp) dateValues.get(4));
		}

		// smalldatetime
		for (int i = 16; i <= 18; i++) {
			sqlPstmt.setSmallDateTime(i, (Timestamp) dateValues.get(5));
		}

		sqlPstmt.execute();
	}

	private void populateDateSetObject(LinkedList<Object> dateValues, String setter) throws SQLException {
		if(setter.equalsIgnoreCase("setwithJDBCType")){
			skipTestForJava7();
		}

		String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?" + ")";

		SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);

		// date
		for (int i = 1; i <= 3; i++) {
			if (setter.equalsIgnoreCase("setwithJavaType"))
				sqlPstmt.setObject(i, (Date) dateValues.get(0), java.sql.Types.DATE);
			else if (setter.equalsIgnoreCase("setwithJDBCType"))
				sqlPstmt.setObject(i, (Date) dateValues.get(0), JDBCType.DATE);
			else
				sqlPstmt.setObject(i, (Date) dateValues.get(0));
		}

		// datetime2 default
		for (int i = 4; i <= 6; i++) {
			if (setter.equalsIgnoreCase("setwithJavaType"))
				sqlPstmt.setObject(i, (Timestamp) dateValues.get(1), java.sql.Types.TIMESTAMP);
			else if (setter.equalsIgnoreCase("setwithJDBCType"))
				sqlPstmt.setObject(i, (Timestamp) dateValues.get(1), JDBCType.TIMESTAMP);
			else
				sqlPstmt.setObject(i, (Timestamp) dateValues.get(1));
		}

		// datetimeoffset default
		for (int i = 7; i <= 9; i++) {
			if (setter.equalsIgnoreCase("setwithJavaType"))
				sqlPstmt.setObject(i, (DateTimeOffset) dateValues.get(2), microsoft.sql.Types.DATETIMEOFFSET);
			else if (setter.equalsIgnoreCase("setwithJDBCType"))
				sqlPstmt.setObject(i, (DateTimeOffset) dateValues.get(2), microsoft.sql.Types.DATETIMEOFFSET);
			else
				sqlPstmt.setObject(i, (DateTimeOffset) dateValues.get(2));
		}

		// time default
		for (int i = 10; i <= 12; i++) {
			if (setter.equalsIgnoreCase("setwithJavaType"))
				sqlPstmt.setObject(i, (Time) dateValues.get(3), java.sql.Types.TIME);
			else if (setter.equalsIgnoreCase("setwithJDBCType"))
				sqlPstmt.setObject(i, (Time) dateValues.get(3), JDBCType.TIME);
			else
				sqlPstmt.setObject(i, (Time) dateValues.get(3));
		}

		// datetime
		for (int i = 13; i <= 15; i++) {
			sqlPstmt.setObject(i, (Timestamp) dateValues.get(4), microsoft.sql.Types.DATETIME);
		}

		// smalldatetime
		for (int i = 16; i <= 18; i++) {
			sqlPstmt.setObject(i, (Timestamp) dateValues.get(5), microsoft.sql.Types.SMALLDATETIME);
		}

		sqlPstmt.execute();
	}

	private void populateDateSetObjectNull() throws SQLException {
		String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?" + ")";

		SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);

		// date
		for (int i = 1; i <= 3; i++) {
			sqlPstmt.setObject(i, null, java.sql.Types.DATE);
		}

		// datetime2 default
		for (int i = 4; i <= 6; i++) {
			sqlPstmt.setObject(i, null, java.sql.Types.TIMESTAMP);
		}

		// datetimeoffset default
		for (int i = 7; i <= 9; i++) {
			sqlPstmt.setObject(i, null, microsoft.sql.Types.DATETIMEOFFSET);
		}

		// time default
		for (int i = 10; i <= 12; i++) {
			sqlPstmt.setObject(i, null, java.sql.Types.TIME);
		}

		// datetime
		for (int i = 13; i <= 15; i++) {
			sqlPstmt.setObject(i, null, microsoft.sql.Types.DATETIME);
		}

		// smalldatetime
		for (int i = 16; i <= 18; i++) {
			sqlPstmt.setObject(i, null, microsoft.sql.Types.SMALLDATETIME);
		}

		sqlPstmt.execute();
	}

	private void populateDateNullCase() throws SQLException {
		String sql = "insert into " + dateTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?" + ")";

		SQLServerPreparedStatement sqlPstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);

		// date
		for (int i = 1; i <= 3; i++) {
			sqlPstmt.setNull(i, java.sql.Types.DATE);
		}

		// datetime2 default
		for (int i = 4; i <= 6; i++) {
			sqlPstmt.setNull(i, java.sql.Types.TIMESTAMP);
		}

		// datetimeoffset default
		for (int i = 7; i <= 9; i++) {
			sqlPstmt.setNull(i, microsoft.sql.Types.DATETIMEOFFSET);
		}

		// time default
		for (int i = 10; i <= 12; i++) {
			sqlPstmt.setNull(i, java.sql.Types.TIME);
		}

		// datetime
		for (int i = 13; i <= 15; i++) {
			sqlPstmt.setNull(i, microsoft.sql.Types.DATETIME);
		}

		// smalldatetime
		for (int i = 16; i <= 18; i++) {
			sqlPstmt.setNull(i, microsoft.sql.Types.SMALLDATETIME);
		}

		sqlPstmt.execute();
	}
	
    /**
     * Populating the table
     * 
     * @param values
     * @throws SQLException
     */
	private void populateNumeric(String[] values) throws SQLException {
		String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// bit
		for (int i = 1; i <= 3; i++) {
			if (values[0].equalsIgnoreCase("true")) {
				pstmt.setBoolean(i, true);
			} else {
				pstmt.setBoolean(i, false);
			}
		}

		// tinyint
		for (int i = 4; i <= 6; i++) {
			pstmt.setShort(i, Short.valueOf(values[1]));
		}

		// smallint
		for (int i = 7; i <= 9; i++) {
			pstmt.setShort(i, Short.valueOf(values[2]));
		}

		// int
		for (int i = 10; i <= 12; i++) {
			pstmt.setInt(i, Integer.valueOf(values[3]));
		}

		// bigint
		for (int i = 13; i <= 15; i++) {
			pstmt.setLong(i, Long.valueOf(values[4]));
		}

		// float default
		for (int i = 16; i <= 18; i++) {
			pstmt.setDouble(i, Double.valueOf(values[5]));
		}

		// float(30)
		for (int i = 19; i <= 21; i++) {
			pstmt.setDouble(i, Double.valueOf(values[6]));
		}

		// real
		for (int i = 22; i <= 24; i++) {
			pstmt.setFloat(i, Float.valueOf(values[7]));
		}

		// decimal default
		for (int i = 25; i <= 27; i++) {
			if (values[8].equalsIgnoreCase("0"))
				pstmt.setBigDecimal(i, new BigDecimal(values[8]), 18, 0);
			else
				pstmt.setBigDecimal(i, new BigDecimal(values[8]));
		}

		// decimal(10,5)
		for (int i = 28; i <= 30; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(values[9]), 10, 5);
		}

		// numeric
		for (int i = 31; i <= 33; i++) {
			if (values[10].equalsIgnoreCase("0"))
				pstmt.setBigDecimal(i, new BigDecimal(values[10]), 18, 0);
			else
				pstmt.setBigDecimal(i, new BigDecimal(values[10]));
		}

		// numeric(8,2)
		for (int i = 34; i <= 36; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(values[11]), 8, 2);
		}

		// small money
		for (int i = 37; i <= 39; i++) {
			pstmt.setSmallMoney(i, new BigDecimal(values[12]));
		}

		// money
		for (int i = 40; i <= 42; i++) {
			pstmt.setMoney(i, new BigDecimal(values[13]));
		}

		// decimal(28,4)
		for (int i = 43; i <= 45; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(values[14]), 28, 4);
		}

		// numeric(28,4)
		for (int i = 46; i <= 48; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(values[15]), 28, 4);
		}

		pstmt.execute();
	}
	
	private void populateNumericSetObject(String[] values) throws SQLException {
		String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// bit
		for (int i = 1; i <= 3; i++) {
			if (values[0].equalsIgnoreCase("true")) {
				pstmt.setObject(i, true);
			} else {
				pstmt.setObject(i, false);
			}
		}

		// tinyint
		for (int i = 4; i <= 6; i++) {
			pstmt.setObject(i, Short.valueOf(values[1]));
		}

		// smallint
		for (int i = 7; i <= 9; i++) {
			pstmt.setObject(i, Short.valueOf(values[2]));
		}

		// int
		for (int i = 10; i <= 12; i++) {
			pstmt.setObject(i, Integer.valueOf(values[3]));
		}

		// bigint
		for (int i = 13; i <= 15; i++) {
			pstmt.setObject(i, Long.valueOf(values[4]));
		}

		// float default
		for (int i = 16; i <= 18; i++) {
			pstmt.setObject(i, Double.valueOf(values[5]));
		}

		// float(30)
		for (int i = 19; i <= 21; i++) {
			pstmt.setObject(i, Double.valueOf(values[6]));
		}

		// real
		for (int i = 22; i <= 24; i++) {
			pstmt.setObject(i, Float.valueOf(values[7]));
		}

		// decimal default
		for (int i = 25; i <= 27; i++) {
			if(RandomData.returnZero)
				pstmt.setObject(i, new BigDecimal(values[8]),java.sql.Types.DECIMAL, 18, 0);
			else 
				pstmt.setObject(i, new BigDecimal(values[8]));
		}

		// decimal(10,5)
		for (int i = 28; i <= 30; i++) {
			pstmt.setObject(i, new BigDecimal(values[9]), java.sql.Types.DECIMAL, 10, 5);
		}

		// numeric
		for (int i = 31; i <= 33; i++) {
			if(RandomData.returnZero)
				pstmt.setObject(i, new BigDecimal(values[10]), java.sql.Types.NUMERIC, 18, 0);
			else 
				pstmt.setObject(i, new BigDecimal(values[10]));
		}

		// numeric(8,2)
		for (int i = 34; i <= 36; i++) {
			pstmt.setObject(i, new BigDecimal(values[11]), java.sql.Types.NUMERIC, 8, 2);
		}

		// small money
		for (int i = 37; i <= 39; i++) {
			pstmt.setObject(i, new BigDecimal(values[12]), microsoft.sql.Types.SMALLMONEY);
		}

		// money
		for (int i = 40; i <= 42; i++) {
			pstmt.setObject(i, new BigDecimal(values[13]), microsoft.sql.Types.MONEY);
		}

		// decimal(28,4)
		for (int i = 43; i <= 45; i++) {
			pstmt.setObject(i, new BigDecimal(values[14]), java.sql.Types.DECIMAL, 28, 4);
		}

		// numeric
		for (int i = 46; i <= 48; i++) {
			pstmt.setObject(i, new BigDecimal(values[15]), java.sql.Types.NUMERIC, 28, 4);
		}

		pstmt.execute();
	}
	
	private void populateNumericSetObjectWithJDBCTypes(String[] values) throws SQLException {
		String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// bit
		for (int i = 1; i <= 3; i++) {
			if (values[0].equalsIgnoreCase("true")) {
				pstmt.setObject(i, true);
			} else {
				pstmt.setObject(i, false);
			}
		}

		// tinyint
		for (int i = 4; i <= 6; i++) {
			pstmt.setObject(i, Short.valueOf(values[1]), JDBCType.TINYINT);
		}

		// smallint
		for (int i = 7; i <= 9; i++) {
			pstmt.setObject(i, Short.valueOf(values[2]), JDBCType.SMALLINT);
		}

		// int
		for (int i = 10; i <= 12; i++) {
			pstmt.setObject(i, Integer.valueOf(values[3]), JDBCType.INTEGER);
		}

		// bigint
		for (int i = 13; i <= 15; i++) {
			pstmt.setObject(i, Long.valueOf(values[4]), JDBCType.BIGINT);
		}

		// float default
		for (int i = 16; i <= 18; i++) {
			pstmt.setObject(i, Double.valueOf(values[5]), JDBCType.DOUBLE);
		}

		// float(30)
		for (int i = 19; i <= 21; i++) {
			pstmt.setObject(i, Double.valueOf(values[6]), JDBCType.DOUBLE);
		}

		// real
		for (int i = 22; i <= 24; i++) {
			pstmt.setObject(i, Float.valueOf(values[7]), JDBCType.REAL);
		}

		// decimal default
		for (int i = 25; i <= 27; i++) {
			if(RandomData.returnZero)
				pstmt.setObject(i, new BigDecimal(values[8]),java.sql.Types.DECIMAL, 18, 0);
			else 
				pstmt.setObject(i, new BigDecimal(values[8]));
		}

		// decimal(10,5)
		for (int i = 28; i <= 30; i++) {
			pstmt.setObject(i, new BigDecimal(values[9]), java.sql.Types.DECIMAL, 10, 5);
		}

		// numeric
		for (int i = 31; i <= 33; i++) {
			if(RandomData.returnZero)
				pstmt.setObject(i, new BigDecimal(values[10]),java.sql.Types.NUMERIC, 18, 0);
			else 
				pstmt.setObject(i, new BigDecimal(values[10]));
		}

		// numeric(8,2)
		for (int i = 34; i <= 36; i++) {
			pstmt.setObject(i, new BigDecimal(values[11]), java.sql.Types.NUMERIC, 8, 2);
		}

		// small money
		for (int i = 37; i <= 39; i++) {
			pstmt.setObject(i, new BigDecimal(values[12]), microsoft.sql.Types.SMALLMONEY);
		}

		// money
		for (int i = 40; i <= 42; i++) {
			pstmt.setObject(i, new BigDecimal(values[13]), microsoft.sql.Types.MONEY);
		}

		// decimal(28,4)
		for (int i = 43; i <= 45; i++) {
			pstmt.setObject(i, new BigDecimal(values[14]), java.sql.Types.DECIMAL, 28, 4);
		}

		// numeric
		for (int i = 46; i <= 48; i++) {
			pstmt.setObject(i, new BigDecimal(values[15]), java.sql.Types.NUMERIC, 28, 4);
		}

		pstmt.execute();
	}

	private void populateNumericSetObjectNull() throws SQLException {
		String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?" + ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// bit
		for (int i = 1; i <= 3; i++) {
			pstmt.setObject(i, null, java.sql.Types.BIT);
		}

		// tinyint
		for (int i = 4; i <= 6; i++) {
			pstmt.setObject(i, null, java.sql.Types.TINYINT);
		}

		// smallint
		for (int i = 7; i <= 9; i++) {
			pstmt.setObject(i, null, java.sql.Types.SMALLINT);
		}

		// int
		for (int i = 10; i <= 12; i++) {
			pstmt.setObject(i, null, java.sql.Types.INTEGER);
		}

		// bigint
		for (int i = 13; i <= 15; i++) {
			pstmt.setObject(i, null, java.sql.Types.BIGINT);
		}

		// float default
		for (int i = 16; i <= 18; i++) {
			pstmt.setObject(i, null, java.sql.Types.DOUBLE);
		}

		// float(30)
		for (int i = 19; i <= 21; i++) {
			pstmt.setObject(i, null, java.sql.Types.DOUBLE);
		}

		// real
		for (int i = 22; i <= 24; i++) {
			pstmt.setObject(i, null, java.sql.Types.REAL);
		}

		// decimal default
		for (int i = 25; i <= 27; i++) {
			pstmt.setObject(i, null, java.sql.Types.DECIMAL);
		}

		// decimal(10,5)
		for (int i = 28; i <= 30; i++) {
			pstmt.setObject(i, null, java.sql.Types.DECIMAL, 10, 5);
		}

		// numeric
		for (int i = 31; i <= 33; i++) {
			pstmt.setObject(i, null, java.sql.Types.NUMERIC);
		}

		// numeric(8,2)
		for (int i = 34; i <= 36; i++) {
			pstmt.setObject(i, null, java.sql.Types.NUMERIC, 8, 2);
		}

		// small money
		for (int i = 37; i <= 39; i++) {
			pstmt.setObject(i, null, microsoft.sql.Types.SMALLMONEY);
		}

		// money
		for (int i = 40; i <= 42; i++) {
			pstmt.setObject(i, null, microsoft.sql.Types.MONEY);
		}

		// decimal(28,4)
		for (int i = 43; i <= 45; i++) {
			pstmt.setObject(i, null, java.sql.Types.DECIMAL, 28, 4);
		}

		// numeric
		for (int i = 46; i <= 48; i++) {
			pstmt.setObject(i, null, java.sql.Types.NUMERIC, 28, 4);
		}

		pstmt.execute();
	}

	private void populateNumericNullCase(String[] values) throws SQLException {
		String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?"

				+ ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// bit
		for (int i = 1; i <= 3; i++) {
			pstmt.setNull(i, java.sql.Types.BIT);
		}

		// tinyint
		for (int i = 4; i <= 6; i++) {
			pstmt.setNull(i, java.sql.Types.TINYINT);
		}

		// smallint
		for (int i = 7; i <= 9; i++) {
			pstmt.setNull(i, java.sql.Types.SMALLINT);
		}

		// int
		for (int i = 10; i <= 12; i++) {
			pstmt.setNull(i, java.sql.Types.INTEGER);
		}

		// bigint
		for (int i = 13; i <= 15; i++) {
			pstmt.setNull(i, java.sql.Types.BIGINT);
		}

		// float default
		for (int i = 16; i <= 18; i++) {
			pstmt.setNull(i, java.sql.Types.DOUBLE);
		}

		// float(30)
		for (int i = 19; i <= 21; i++) {
			pstmt.setNull(i, java.sql.Types.DOUBLE);
		}

		// real
		for (int i = 22; i <= 24; i++) {
			pstmt.setNull(i, java.sql.Types.REAL);
		}

		// decimal default
		for (int i = 25; i <= 27; i++) {
			pstmt.setBigDecimal(i, null);
		}

		// decimal(10,5)
		for (int i = 28; i <= 30; i++) {
			pstmt.setBigDecimal(i, null, 10, 5);
		}

		// numeric
		for (int i = 31; i <= 33; i++) {
			pstmt.setBigDecimal(i, null);
		}

		// numeric(8,2)
		for (int i = 34; i <= 36; i++) {
			pstmt.setBigDecimal(i, null, 8, 2);
		}

		// small money
		for (int i = 37; i <= 39; i++) {
			pstmt.setSmallMoney(i, null);
		}

		// money
		for (int i = 40; i <= 42; i++) {
			pstmt.setMoney(i, null);
		}

		// decimal(28,4)
		for (int i = 43; i <= 45; i++) {
			pstmt.setBigDecimal(i, null, 28, 4);
		}

		// decimal(28,4)
		for (int i = 46; i <= 48; i++) {
			pstmt.setBigDecimal(i, null, 28, 4);
		}
		pstmt.execute();
	}

	private void populateNumericNormalization(String[] numericValues) throws SQLException {
		String sql = "insert into " + numericTable + " values( " + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?," + "?,?,?,"
				+ "?,?,?," + "?,?,?"

				+ ")";

		pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql, stmtColEncSetting);

		// bit
		for (int i = 1; i <= 3; i++) {
			if (numericValues[0].equalsIgnoreCase("true")) {
				pstmt.setBoolean(i, true);
			} else {
				pstmt.setBoolean(i, false);
			}
		}

		// tinyint
		for (int i = 4; i <= 6; i++) {
			if (1 == Integer.valueOf(numericValues[1])) {
				pstmt.setBoolean(i, true);
			} else {
				pstmt.setBoolean(i, false);
			}
		}

		// smallint
		for (int i = 7; i <= 9; i++) {
			if (numericValues[2].equalsIgnoreCase("255")) {
				pstmt.setByte(i, (byte) 255);
			} else {
				pstmt.setByte(i, Byte.valueOf(numericValues[2]));
			}
		}

		// int
		for (int i = 10; i <= 12; i++) {
			pstmt.setShort(i, Short.valueOf(numericValues[3]));
		}

		// bigint
		for (int i = 13; i <= 15; i++) {
			pstmt.setInt(i, Integer.valueOf(numericValues[4]));
		}

		// float default
		for (int i = 16; i <= 18; i++) {
			pstmt.setDouble(i, Double.valueOf(numericValues[5]));
		}

		// float(30)
		for (int i = 19; i <= 21; i++) {
			pstmt.setDouble(i, Double.valueOf(numericValues[6]));
		}

		// real
		for (int i = 22; i <= 24; i++) {
			pstmt.setFloat(i, Float.valueOf(numericValues[7]));
		}

		// decimal default
		for (int i = 25; i <= 27; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(numericValues[8]));
		}

		// decimal(10,5)
		for (int i = 28; i <= 30; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(numericValues[9]), 10, 5);
		}

		// numeric
		for (int i = 31; i <= 33; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(numericValues[10]));
		}

		// numeric(8,2)
		for (int i = 34; i <= 36; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(numericValues[11]), 8, 2);
		}

		// small money
		for (int i = 37; i <= 39; i++) {
			pstmt.setSmallMoney(i, new BigDecimal(numericValues[12]));
		}

		// money
		for (int i = 40; i <= 42; i++) {
			pstmt.setSmallMoney(i, new BigDecimal(numericValues[13]));
		}

		// decimal(28,4)
		for (int i = 43; i <= 45; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(numericValues[14]), 28, 4);
		}

		// numeric
		for (int i = 46; i <= 48; i++) {
			pstmt.setBigDecimal(i, new BigDecimal(numericValues[15]), 28, 4);
		}

		pstmt.execute();
	}
	
	private void testChar(SQLServerStatement stmt, String[] values) throws SQLException {
		String sql = "select * from " + charTable;
		SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);
		ResultSet rs = null;
		if (stmt == null) {
			rs = pstmt.executeQuery();
		} else {
			rs = stmt.executeQuery(sql);
		}
		int numberOfColumns = rs.getMetaData().getColumnCount();

		while (rs.next()) {
			testGetString(rs, numberOfColumns, values);
			testGetObject(rs, numberOfColumns, values);
		}

		if (null != rs) {
			rs.close();
		}
	}

	private void testBinary(SQLServerStatement stmt, LinkedList<byte[]> values) throws SQLException {
		String sql = "select * from " + binaryTable;
		SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);
		ResultSet rs = null;
		if (stmt == null) {
			rs = pstmt.executeQuery();
		} else {
			rs = stmt.executeQuery(sql);
		}
		int numberOfColumns = rs.getMetaData().getColumnCount();

		while (rs.next()) {
			testGetStringForBinary(rs, numberOfColumns, values);
			testGetBytes(rs, numberOfColumns, values);
			testGetObjectForBinary(rs, numberOfColumns, values);
		}

		if (null != rs) {
			rs.close();
		}
	}

	private void testDate(SQLServerStatement stmt, LinkedList<Object> values1) throws SQLException {

		String sql = "select * from " + dateTable;
		SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);
		ResultSet rs = null;
		if (stmt == null) {
			rs = pstmt.executeQuery();
		} else {
			rs = stmt.executeQuery(sql);
		}
		int numberOfColumns = rs.getMetaData().getColumnCount();

		while (rs.next()) {
			//testGetStringForDate(rs, numberOfColumns, values1); //TODO: Disabling, since getString throws verification error for zero temporal types
			testGetObjectForTemporal(rs, numberOfColumns, values1);
			testGetDate(rs, numberOfColumns, values1);
		}

		if (null != rs) {
			rs.close();
		}
	}

	private void testGetObject(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {
		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			try {
				String objectValue1 = ("" + rs.getObject(i)).trim();
				String objectValue2 = ("" + rs.getObject(i + 1)).trim();
				String objectValue3 = ("" + rs.getObject(i + 2)).trim();

				boolean matches = objectValue1.equalsIgnoreCase("" + values[index])
						&& objectValue2.equalsIgnoreCase("" + values[index])
						&& objectValue3.equalsIgnoreCase("" + values[index]);
				
				if(("" + values[index]).length() >= 1000){
					assertTrue(matches, "\nDecryption failed with getObject() at index: " + i + ", " + (i + 1) + ", "
									+ (i + 2) + ".\nExpected Value at index: " + index);
				}
				else{
					assertTrue(matches,"\nDecryption failed with getObject(): " + objectValue1 + ", " + objectValue2 + ", "
							+ objectValue3 + ".\nExpected Value: " + values[index]);
				}
			} finally {
				index++;
			}
		}
	}

	private void testGetObjectForTemporal(ResultSet rs, int numberOfColumns, LinkedList<Object> values)
			throws SQLException {
		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			try {
				String objectValue1 = ("" + rs.getObject(i)).trim();
				String objectValue2 = ("" + rs.getObject(i + 1)).trim();
				String objectValue3 = ("" + rs.getObject(i + 2)).trim();

				Object expected = null;
				if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("smalldatetime")) {
					expected = Util.roundSmallDateTimeValue(values.get(index));
				} else if (rs.getMetaData().getColumnTypeName(i).equalsIgnoreCase("datetime")) {
					expected = Util.roundDatetimeValue(values.get(index));
				} else {
					expected = values.get(index);
				}
				assertTrue(
						objectValue1.equalsIgnoreCase("" + expected) && objectValue2.equalsIgnoreCase("" + expected)
						&& objectValue3.equalsIgnoreCase("" + expected),
						"\nDecryption failed with getObject(): " + objectValue1 + ", " + objectValue2 + ", "
								+ objectValue3 + ".\nExpected Value: " + expected);
			} finally {
				index++;
			}
		}
	}

	private void testGetObjectForBinary(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values)
			throws SQLException {
		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			byte[] objectValue1 = (byte[]) rs.getObject(i);
			byte[] objectValue2 = (byte[]) rs.getObject(i + 1);
			byte[] objectValue3 = (byte[]) rs.getObject(i + 2);

			byte[] expectedBytes = null;

			if (null != values.get(index)) {
				expectedBytes = values.get(index);
			}

			try {
				if (null != values.get(index)) {
					for (int j = 0; j < expectedBytes.length; j++) {
						assertTrue(
								expectedBytes[j] == objectValue1[j] && expectedBytes[j] == objectValue2[j]
										&& expectedBytes[j] == objectValue3[j],
										"Decryption failed with getObject(): " + objectValue1 + ", " + objectValue2 + ", "
												+ objectValue3 + ".\n");
					}
				}
			} finally {
				index++;
			}
		}
	}

	private void testGetBigDecimal(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {

			String decimalValue1 = "" + rs.getBigDecimal(i);
			String decimalValue2 = "" + rs.getBigDecimal(i + 1);
			String decimalValue3 = "" + rs.getBigDecimal(i + 2);

			if (decimalValue1.equalsIgnoreCase("0")
					&& (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
				decimalValue1 = "false";
				decimalValue2 = "false";
				decimalValue3 = "false";
			} else if (decimalValue1.equalsIgnoreCase("1")
					&& (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
				decimalValue1 = "true";
				decimalValue2 = "true";
				decimalValue3 = "true";
			}

			if (null != values[index]) {
				if (values[index].equalsIgnoreCase("1.79E308")) {
					values[index] = "1.79E+308";
				} else if (values[index].equalsIgnoreCase("3.4E38")) {
					values[index] = "3.4E+38";
				}

				if (values[index].equalsIgnoreCase("-1.79E308")) {
					values[index] = "-1.79E+308";
				} else if (values[index].equalsIgnoreCase("-3.4E38")) {
					values[index] = "-3.4E+38";
				}
			}

			try {
				assertTrue(
						decimalValue1.equalsIgnoreCase("" + values[index])
						&& decimalValue2.equalsIgnoreCase("" + values[index])
						&& decimalValue3.equalsIgnoreCase("" + values[index]),
						"\nDecryption failed with getBigDecimal(): " + decimalValue1 + ", " + decimalValue2 + ", "
								+ decimalValue3 + ".\nExpected Value: " + values[index]);
			} finally {
				index++;
			}
		}
	}

	private void testGetString(ResultSet rs, int numberOfColumns, String[] values) throws SQLException {

		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			String stringValue1 = ("" + rs.getString(i)).trim();
			String stringValue2 = ("" + rs.getString(i + 1)).trim();
			String stringValue3 = ("" + rs.getString(i + 2)).trim();

			if (stringValue1.equalsIgnoreCase("0")
					&& (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
				stringValue1 = "false";
				stringValue2 = "false";
				stringValue3 = "false";
			} else if (stringValue1.equalsIgnoreCase("1")
					&& (values[index].equalsIgnoreCase("true") || values[index].equalsIgnoreCase("false"))) {
				stringValue1 = "true";
				stringValue2 = "true";
				stringValue3 = "true";
			}
			try {
				
				boolean matches = stringValue1.equalsIgnoreCase("" + values[index])
						&& stringValue2.equalsIgnoreCase("" + values[index])
						&& stringValue3.equalsIgnoreCase("" + values[index]);
				
				if(("" + values[index]).length() >= 1000){
					assertTrue(
							matches,
							"\nDecryption failed with getString() at index: " + i + ", " + (i + 1) + ", "
									+ (i + 2) + ".\nExpected Value at index: " + index);
					
				}
				else{
					assertTrue(
							matches,
							"\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", "
									+ stringValue3 + ".\nExpected Value: " + values[index]);
				}
			} finally {
				index++;
			}
		}
	}

	// not testing this for now.
	@SuppressWarnings("unused")
	private void testGetStringForDate(ResultSet rs, int numberOfColumns, LinkedList<Object> values)
			throws SQLException {

		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			String stringValue1 = ("" + rs.getString(i)).trim();
			String stringValue2 = ("" + rs.getString(i + 1)).trim();
			String stringValue3 = ("" + rs.getString(i + 2)).trim();

			try {
				if (index == 3) {
					assertTrue(
							stringValue1.contains("" + values.get(index))
							&& stringValue2.contains("" + values.get(index))
							&& stringValue3.contains("" + values.get(index)),
							"\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", "
									+ stringValue3 + ".\nExpected Value: " + values.get(index));
				} else if (index == 4) // round value for datetime
				{
					Object datetimeValue = "" + Util.roundDatetimeValue(values.get(index));
					assertTrue(
							stringValue1.equalsIgnoreCase("" + datetimeValue)
							&& stringValue2.equalsIgnoreCase("" + datetimeValue)
							&& stringValue3.equalsIgnoreCase("" + datetimeValue),
							"\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", "
									+ stringValue3 + ".\nExpected Value: " + datetimeValue);
				} else if (index == 5) // round value for smalldatetime
				{
					Object smalldatetimeValue = "" + Util.roundSmallDateTimeValue(values.get(index));
					assertTrue(
							stringValue1.equalsIgnoreCase("" + smalldatetimeValue)
							&& stringValue2.equalsIgnoreCase("" + smalldatetimeValue)
							&& stringValue3.equalsIgnoreCase("" + smalldatetimeValue),
							"\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", "
									+ stringValue3 + ".\nExpected Value: " + smalldatetimeValue);
				} else {
					assertTrue(
							stringValue1.contains("" + values.get(index))
							&& stringValue2.contains("" + values.get(index))
							&& stringValue3.contains("" + values.get(index)),
							"\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", "
									+ stringValue3 + ".\nExpected Value: " + values.get(index));
				}
			} finally {
				index++;
			}
		}
	}

	private void testGetBytes(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values) throws SQLException {
		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			byte[] b1 = rs.getBytes(i);
			byte[] b2 = rs.getBytes(i + 1);
			byte[] b3 = rs.getBytes(i + 2);

			byte[] expectedBytes = null;

			if (null != values.get(index)) {
				expectedBytes = values.get(index);
			}

			try {
				if (null != values.get(index)) {
					for (int j = 0; j < expectedBytes.length; j++) {
						assertTrue(
								expectedBytes[j] == b1[j] && expectedBytes[j] == b2[j] && expectedBytes[j] == b3[j],
								"Decryption failed with getObject(): " + b1 + ", " + b2 + ", " + b3 + ".\n");
					}
				}
			} finally {
				index++;
			}
		}
	}

	private void testGetStringForBinary(ResultSet rs, int numberOfColumns, LinkedList<byte[]> values)
			throws SQLException {

		int index = 0;
		for (int i = 1; i <= numberOfColumns; i = i + 3) {
			String stringValue1 = ("" + rs.getString(i)).trim();
			String stringValue2 = ("" + rs.getString(i + 1)).trim();
			String stringValue3 = ("" + rs.getString(i + 2)).trim();

			StringBuffer expected = new StringBuffer();
			String expectedStr = null;

			if (null != values.get(index)) {
				for (byte b : values.get(index)) {
					expected.append(String.format("%02X", b));
				}
				expectedStr = "" + expected.toString();
			} else {
				expectedStr = "null";
			}

			try {
				assertTrue(
						stringValue1.startsWith(expectedStr) && stringValue2.startsWith(expectedStr)
						&& stringValue3.startsWith(expectedStr),
						"\nDecryption failed with getString(): " + stringValue1 + ", " + stringValue2 + ", "
								+ stringValue3 + ".\nExpected Value: " + expectedStr);
			} finally {
				index++;
			}
		}
	}

	private void testGetDate(ResultSet rs, int numberOfColumns, LinkedList<Object> values) throws SQLException {
		for (int i = 1; i <= numberOfColumns; i = i + 3) {

			if (rs instanceof SQLServerResultSet) {

				String stringValue1 = null;
				String stringValue2 = null;
				String stringValue3 = null;
				String expected = null;

				switch (i) {

				case 1:
					stringValue1 = "" + ((SQLServerResultSet) rs).getDate(i);
					stringValue2 = "" + ((SQLServerResultSet) rs).getDate(i + 1);
					stringValue3 = "" + ((SQLServerResultSet) rs).getDate(i + 2);
					expected = "" + values.get(0);
					break;

				case 4:
					stringValue1 = "" + ((SQLServerResultSet) rs).getTimestamp(i);
					stringValue2 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 1);
					stringValue3 = "" + ((SQLServerResultSet) rs).getTimestamp(i + 2);
					expected = "" + values.get(1);
					break;

				case 7:
					stringValue1 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i);
					stringValue2 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 1);
					stringValue3 = "" + ((SQLServerResultSet) rs).getDateTimeOffset(i + 2);
					expected = "" + values.get(2);
					break;

				case 10:
					stringValue1 = "" + ((SQLServerResultSet) rs).getTime(i);
					stringValue2 = "" + ((SQLServerResultSet) rs).getTime(i + 1);
					stringValue3 = "" + ((SQLServerResultSet) rs).getTime(i + 2);
					expected = "" + values.get(3);
					break;

				case 13:
					stringValue1 = "" + ((SQLServerResultSet) rs).getDateTime(i);
					stringValue2 = "" + ((SQLServerResultSet) rs).getDateTime(i + 1);
					stringValue3 = "" + ((SQLServerResultSet) rs).getDateTime(i + 2);
					expected = "" + Util.roundDatetimeValue(values.get(4));
					break;

				case 16:
					stringValue1 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i);
					stringValue2 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i + 1);
					stringValue3 = "" + ((SQLServerResultSet) rs).getSmallDateTime(i + 2);
					expected = "" + Util.roundSmallDateTimeValue(values.get(5));
					break;

				default:
					fail("Switch case is not matched with data");
				}

				assertTrue(
						stringValue1.equalsIgnoreCase(expected) && stringValue2.equalsIgnoreCase(expected)
						&& stringValue3.equalsIgnoreCase(expected),
						"\nDecryption failed with testGetDate: " + stringValue1 + ", " + stringValue2 + ", "
								+ stringValue3 + ".\nExpected Value: " + expected);
			}

			else {
				fail("Result set is not instance of SQLServerResultSet");
			}
		}
	}
	
	private void testNumeric(Statement stmt, String[] numericValues, boolean isNull) throws SQLException {
		String sql = "select * from " + numericTable;
		SQLServerPreparedStatement pstmt = (SQLServerPreparedStatement) Util.getPreparedStmt(con, sql,
				stmtColEncSetting);
		SQLServerResultSet rs = null;
		if (stmt == null) {
			rs = (SQLServerResultSet) pstmt.executeQuery();
		} else {
			rs = (SQLServerResultSet) stmt.executeQuery(sql);
		}
		int numberOfColumns = rs.getMetaData().getColumnCount();

		while (rs.next()) {
			testGetString(rs, numberOfColumns, numericValues);
			testGetObject(rs, numberOfColumns, numericValues);
			testGetBigDecimal(rs, numberOfColumns, numericValues);
			if (!isNull)
				testWithSpecifiedtype(rs, numberOfColumns, numericValues);
			else {
				String[] nullNumericValues = { "false", "0", "0", "0", "0", "0.0", "0.0", "0.0", null, null, null, null,
						null, null, null, null };
				testWithSpecifiedtype(rs, numberOfColumns, nullNumericValues);
			}
		}

		if (null != rs) {
			rs.close();
		}
	}
	
	private void testWithSpecifiedtype(SQLServerResultSet rs, int numberOfColumns, String[] values)
			throws SQLException {

		String value1, value2, value3, expectedValue = null;
		int index = 0;

		// bit
		value1 = "" + rs.getBoolean(1);
		value2 = "" + rs.getBoolean(2);
		value3 = "" + rs.getBoolean(3);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// tiny
		value1 = "" + rs.getShort(4);
		value2 = "" + rs.getShort(5);
		value3 = "" + rs.getShort(6);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// smallint
		value1 = "" + rs.getShort(7);
		value2 = "" + rs.getShort(8);
		value3 = "" + rs.getShort(8);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// int
		value1 = "" + rs.getInt(10);
		value2 = "" + rs.getInt(11);
		value3 = "" + rs.getInt(12);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// bigint
		value1 = "" + rs.getLong(13);
		value2 = "" + rs.getLong(14);
		value3 = "" + rs.getLong(15);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// float
		value1 = "" + rs.getDouble(16);
		value2 = "" + rs.getDouble(17);
		value3 = "" + rs.getDouble(18);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// float(30)
		value1 = "" + rs.getDouble(19);
		value2 = "" + rs.getDouble(20);
		value3 = "" + rs.getDouble(21);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// real
		value1 = "" + rs.getFloat(22);
		value2 = "" + rs.getFloat(23);
		value3 = "" + rs.getFloat(24);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// decimal
		value1 = "" + rs.getBigDecimal(25);
		value2 = "" + rs.getBigDecimal(26);
		value3 = "" + rs.getBigDecimal(27);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// decimal (10,5)
		value1 = "" + rs.getBigDecimal(28);
		value2 = "" + rs.getBigDecimal(29);
		value3 = "" + rs.getBigDecimal(30);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// numeric
		value1 = "" + rs.getBigDecimal(31);
		value2 = "" + rs.getBigDecimal(32);
		value3 = "" + rs.getBigDecimal(33);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// numeric (8,2)
		value1 = "" + rs.getBigDecimal(34);
		value2 = "" + rs.getBigDecimal(35);
		value3 = "" + rs.getBigDecimal(36);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// smallmoney
		value1 = "" + rs.getSmallMoney(37);
		value2 = "" + rs.getSmallMoney(38);
		value3 = "" + rs.getSmallMoney(39);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// money
		value1 = "" + rs.getMoney(40);
		value2 = "" + rs.getMoney(41);
		value3 = "" + rs.getMoney(42);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// decimal(28,4)
		value1 = "" + rs.getBigDecimal(43);
		value2 = "" + rs.getBigDecimal(44);
		value3 = "" + rs.getBigDecimal(45);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;

		// numeric(28,4)
		value1 = "" + rs.getBigDecimal(46);
		value2 = "" + rs.getBigDecimal(47);
		value3 = "" + rs.getBigDecimal(48);

		expectedValue = values[index];
		Compare(expectedValue, value1, value2, value3);
		index++;
	}
	
	private void Compare(String expectedValue, String value1, String value2, String value3) {

		if (null != expectedValue) {
			if (expectedValue.equalsIgnoreCase("1.79E+308")) {
				expectedValue = "1.79E308";
			} else if (expectedValue.equalsIgnoreCase("3.4E+38")) {
				expectedValue = "3.4E38";
			}

			if (expectedValue.equalsIgnoreCase("-1.79E+308")) {
				expectedValue = "-1.79E308";
			} else if (expectedValue.equalsIgnoreCase("-3.4E+38")) {
				expectedValue = "-3.4E38";
			}
		}
		
		assertTrue(
				value1.equalsIgnoreCase("" + expectedValue) && value2.equalsIgnoreCase("" + expectedValue)
				&& value3.equalsIgnoreCase("" + expectedValue),
				"\nDecryption failed with getBigDecimal(): " + value1 + ", " + value2 + ", " + value3
				+ ".\nExpected Value: " + expectedValue);
	}

	private String[] createCharValues() {

		boolean encrypted = true;
		String char20 = RandomData.generateCharTypes("20", nullable, encrypted);
		String varchar50 = RandomData.generateCharTypes("50", nullable, encrypted);
		String varcharmax = RandomData.generateCharTypes("max", nullable, encrypted);
		String nchar30 = RandomData.generateNCharTypes("30", nullable, encrypted);
		String nvarchar60 = RandomData.generateNCharTypes("60", nullable, encrypted);
		String nvarcharmax = RandomData.generateNCharTypes("max", nullable, encrypted);
		String varchar8000 = RandomData.generateCharTypes("8000", nullable, encrypted);
		String nvarchar4000 = RandomData.generateNCharTypes("4000", nullable, encrypted);

		String[] values = { char20.trim(), varchar50, varcharmax, nchar30, nvarchar60, nvarcharmax, uid, varchar8000,
				nvarchar4000 };

		return values;
	}
	
	private LinkedList<byte[]> createbinaryValues(boolean nullable) {

		boolean encrypted = true;
		RandomData.returnNull = nullable;

		byte[] binary20 = RandomData.generateBinaryTypes("20", nullable, encrypted);
		byte[] varbinary50 = RandomData.generateBinaryTypes("50", nullable, encrypted);
		byte[] varbinarymax = RandomData.generateBinaryTypes("max", nullable, encrypted);
		byte[] binary512 = RandomData.generateBinaryTypes("512", nullable, encrypted);
		byte[] varbinary8000 = RandomData.generateBinaryTypes("8000", nullable, encrypted);

		LinkedList<byte[]> list = new LinkedList<>();
		list.add(binary20);
		list.add(varbinary50);
		list.add(varbinarymax);
		list.add(binary512);
		list.add(varbinary8000);

		return list;
	}
	
	private String[] createNumericValues() {

		Boolean boolValue = RandomData.generateBoolean(nullable);
		Short tinyIntValue = RandomData.generateTinyint(nullable);
		Short smallIntValue = RandomData.generateSmallint(nullable);
		Integer intValue = RandomData.generateInt(nullable);
		Long bigintValue = RandomData.generateLong(nullable);
		Double floatValue = RandomData.generateFloat(24, nullable);
		Double floatValuewithPrecision = RandomData.generateFloat(53, nullable);
		Float realValue = RandomData.generateReal(nullable);
		BigDecimal decimal = RandomData.generateDecimalNumeric(18, 0, nullable); 
		BigDecimal decimalPrecisionScale = RandomData.generateDecimalNumeric(10, 5, nullable);
		BigDecimal numeric = RandomData.generateDecimalNumeric(18, 0, nullable); 
		BigDecimal numericPrecisionScale = RandomData.generateDecimalNumeric(8, 2, nullable); 
		BigDecimal smallMoney = RandomData.generateSmallMoney(nullable);
		BigDecimal money = RandomData.generateMoney(nullable);
		BigDecimal decimalPrecisionScale2 = RandomData.generateDecimalNumeric(28, 4, nullable);
		BigDecimal numericPrecisionScale2 = RandomData.generateDecimalNumeric(28, 4, nullable);

		String[] numericValues = { "" + boolValue, "" + tinyIntValue, "" + smallIntValue, "" + intValue,
				"" + bigintValue, "" + floatValue, "" + floatValuewithPrecision, "" + realValue, "" + decimal,
				"" + decimalPrecisionScale, "" + numeric, "" + numericPrecisionScale, "" + smallMoney, "" + money,
				"" + decimalPrecisionScale2, "" + numericPrecisionScale2 };

		return numericValues;
	}
	
	private LinkedList<Object> createTemporalTypes() {

		Date date = RandomData.generateDate(nullable);
		Timestamp datetime2 = RandomData.generateDatetime2(7, nullable);
		DateTimeOffset datetimeoffset = RandomData.generateDatetimeoffset(7, nullable);
		Time time = RandomData.generateTime(7, nullable);
		Timestamp datetime = RandomData.generateDatetime(nullable);
		Timestamp smalldatetime = RandomData.generateSmalldatetime(nullable);

		LinkedList<Object> list = new LinkedList<>();
		list.add(date);
		list.add(datetime2);
		list.add(datetimeoffset);
		list.add(time);
		list.add(datetime);
		list.add(smalldatetime);

		return list;
	}
	
	private void skipTestForJava7() {
    	assumeTrue(com.microsoft.sqlserver.jdbc.Util.use42Wrapper()); // With Java 7, skip tests for JDBCType.
	}
}
