/*
File: bvt_ResultSet.java
Contents: Performs the verification on the retrieved resultSet.

Microsoft JDBC Driver for SQL Server
Copyright(c) Microsoft Corporation
All rights reserved.
MIT License
Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the "Software"), 
to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, 
and / or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
THE SOFTWARE IS PROVIDED *AS IS*, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, 
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER 
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS 
IN THE SOFTWARE.
*/

import java.math.BigDecimal;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import static org.junit.Assert.*;

public class bvt_ResultSet {

	private ResultSet rs;
	private ResultSetMetaData metadata = null;
	private int index;

	public bvt_ResultSet(ResultSet rs) {
		this.rs = rs;
		index = 0;
	}

	public void setIndex(int index) {
		this.index = index;
	}

	public boolean next() throws SQLException {
		boolean validrow = rs.next();
		if (validrow)
			index++;
		return (validrow);
	}

	public boolean previous() throws SQLException {
		boolean validrow = rs.previous();
		if (validrow)
			index--;
		return (validrow);
	}

	public void afterLast() throws SQLException {
		rs.afterLast();
		index = Values.getRowNumbers() + 1;
	}

	public void close() throws SQLException {
		rs.close();
	}

	public void absolute(int x) throws SQLException {
		rs.absolute(x);
		index = x;
	}

	public void verify() throws SQLException {

		metadata = rs.getMetaData();
		metaData_Verify();

		// Verify the data
		while (next()) {
			verifyCurrentRow();
		}
	}

	private void metaData_Verify() {

		// getColumnCount
		int columns;
		try {
			columns = metadata.getColumnCount();

			// Loop through the columns
			for (int i = 1; i <= columns; i++) {
				// Note: Just calling these performs the verification, in each
				// method
				metadata.getColumnName(i);
				metadata.getColumnType(i);
				metadata.getColumnTypeName(i);

				// MetaData
				metadata.getScale(i);
				metadata.isCaseSensitive(i);
				metadata.isAutoIncrement(i);
				metadata.isCurrency(i);
				metadata.isNullable(i);
				metadata.isSigned(i);

			}
		} catch (SQLException e) {
			fail(e.toString());
		}

	}

	public void verifyCurrentRow() {
		Class coercion = Object.class;

		ArrayList<Object> currentRow = Values.getTableValues(index - 1);
		Object backendData = null;

		for (int i = 1; i <= currentRow.size(); i++) {
			backendData = currentRow.get(i - 1); // currentRow is zero based
			verifyData(i, coercion, backendData);
		}

	}

	private void verifyData(int idx, Class coercion, Object backendData) {

		try {
			// getXXX - default mapping
			Object actual = getXXX(idx, coercion);

			// Verify
			verifydata(backendData, actual, idx);

		} catch (SQLException e) {
			fail(e.toString());
		}

	}

	private Object getXXX(int idx, Class coercion) throws SQLException {
		if (coercion == Object.class) {
			return rs.getObject(idx);
		}
		return null;
	}

	private static void verifydata(Object backendData, Object actual, int idx) {
		if (backendData != null) {
			if (actual instanceof BigDecimal) {
				if (((BigDecimal) actual).compareTo(new BigDecimal("" + backendData)) != 0)
					fail(" Verification failed at index: " + idx + " , retrieved value: " + actual
							+ " , inserted value is " + backendData);
			} else if (actual instanceof Float) {
				if (Float.compare(new Float("" + backendData), (float) actual) != 0)
					fail(" Verification failed at index: " + idx + " , retrieved value: " + actual
							+ " ,inserted value is " + backendData);
			} else if (actual instanceof Double) {
				if (Double.compare(new Double("" + backendData), (double) actual) != 0)
					fail(" Verification failed at index: " + idx + " , retrieved value: " + actual
							+ " , inserted value is " + backendData);
			} else if (actual instanceof byte[]) {
				if (!parseByte((byte[]) actual).contains("" + backendData))
					fail(" Verification failed at index: " + idx + " , retrieved value: " + actual
							+ " , inserted value is " + backendData);
			} else if (actual instanceof String) {
				if (!(((String) actual).trim()).equalsIgnoreCase(((String) backendData).trim()))
					fail(" Verification failed at index: " + idx + " , retrieved value: " + actual
							+ " , inserted value is " + backendData);
			} else if (!(("" + actual).equalsIgnoreCase("" + backendData)))
				fail(" Verification failed at index: " + idx + " , retrieved value: " + actual + " , inserted value is "
						+ backendData);
		}
		// if data is null
		else {
			if (actual != backendData)
				fail(" Verification failed at index: " + idx + " , retrieved value: " + actual + " , inserted value is "
						+ backendData);
		}
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
