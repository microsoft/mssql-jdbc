/*
 * Microsoft JDBC Driver for SQL Server
 * 
 * Copyright(c) 2016 Microsoft Corporation
 * All rights reserved.
 * 
 * This program is made available under the terms of the MIT License.
 * See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc.bvt;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Random;
import java.util.UUID;

import microsoft.sql.DateTimeOffset;

public class Values {

	private static ArrayList<Object> valuesPerRow;
	private static ArrayList<ArrayList<Object>> tableValues;
	private static String normalCharSet = "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
	private static int rowNumbers;
	private static long max;
	private static long min;
	private static long avg;
	private static final char[] hexdigits = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9', 'A', 'B', 'C', 'D', 'E',
			'F' };
	private static Random r = new Random();

	public static int getRowNumbers() {
		return rowNumbers;
	}

	public static void createData() {

		String[] char512 = { generateCharTypes("512", false), generateCharTypes("1", false),
				generateCharTypes("225", false) };

		Date[] date = { Date.valueOf("9999-12-31"), Date.valueOf("0001-01-01"), Date.valueOf("2011-01-11") };

		BigDecimal[] decimal = { new BigDecimal("999999999999999999999999.9999"),
				new BigDecimal("-999999999999999999999999.9999"), new BigDecimal("1234.1234") };

		BigDecimal[] money = { new BigDecimal("922337203685477.5807"), new BigDecimal("-922337203685477.5808"),
				new BigDecimal("-522337203685477.1234") };

		max = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
		min = Timestamp.valueOf("0001-01-01 00:00:00").getTime();
		avg = Timestamp.valueOf("2011-01-11 00:00:00").getTime();
		Time[] time = { Time.valueOf("23:59:59"), Time.valueOf("00:00:00"), Time.valueOf("05:23:00") };

		String[] guid = { "" + UUID.randomUUID(), "" + UUID.randomUUID(), "" + UUID.randomUUID() };

		String[] varcharMax = { generateCharTypes("max", false), generateCharTypes("1", false),
				generateCharTypes("225", false) };

		String[] nVarcharMax = { generateNCharTypes("max", false), generateNCharTypes("1", false),
				generateNCharTypes("225", false) };

		Short[] smallint = { new Short(Short.MAX_VALUE), new Short(Short.MIN_VALUE), 5 };

		BigDecimal[] numeric = { new BigDecimal("999999999999999999999999.9999"),
				new BigDecimal("-999999999999999999999999.9999"), new BigDecimal("1234.1234") };

		String[] ntext = { "林ན་དགོས་ཏེ།ངག་ཕྱོαβγδεζη", "जंतुआपेड़पौधाकेमिलल太陽系の年齢もま", "放我放问역사적으" };

		String[] varbinaryMax = { "0x" + toString(generateBinaryTypes("max", false)),
				"0x" + toString(generateBinaryTypes("max", false)),
				"0x" + toString(generateBinaryTypes("max", false)) };

		String[] nchar512 = { generateNCharTypes("512", false), generateNCharTypes("0", false),
				generateNCharTypes("225", false) };

		DateTimeOffset maxDTS = calculateDateTimeOffsetMinMax("max", 7, "9999-12-31 23:59:59");
		DateTimeOffset minDTS = calculateDateTimeOffsetMinMax("min", 7, "0001-01-01 00:00:00");
		DateTimeOffset[] dto = { maxDTS, minDTS, minDTS };

		String[] xml = { "<test><set><id>1</id><names>EMP</names></set></test>",
				"<test><set><id>1</id><names>EMP</names></set></test>",
				"<test><set><id>1</id><names>EMP</names></set></test>" };

		max = Timestamp.valueOf("2079-06-06 23:59:00").getTime();
		min = Timestamp.valueOf("1900-01-01 00:00:00").getTime();
		Timestamp[] smallDatetime = { new Timestamp(max), new Timestamp(min), new Timestamp(min) };

		String[] varbinary512 = { "0x633234", "0x633234", "0x633234" };

		max = Timestamp.valueOf("9999-12-31 23:59:59.997").getTime();
		min = Timestamp.valueOf("1753-01-01 00:00:00.000").getTime();
		Timestamp[] Datetime = { new Timestamp(max), new Timestamp(min), new Timestamp(min) };

		max = Timestamp.valueOf("9999-12-31 23:59:59").getTime();
		min = Timestamp.valueOf("0001-01-01 00:00:00").getTime();
		Timestamp[] Datetime2 = { new Timestamp(max), new Timestamp(min), new Timestamp(min) };

		Float[] real = { Float.valueOf("3.4E38"), Float.valueOf("-3.4E38"), Float.valueOf("10.0") };

		Short[] tinyint = { Short.valueOf("255"), Short.valueOf("0"), Short.valueOf("125") };

		Double[] floatVal = { new Double(+1.79E308), new Double(-1.79E308), new Double(10.0) };

		String[] text = { "text", "text", "text" };

		String[] binary512 = { "0x" + toString(generateBinaryTypes("512", false)),
				"0x" + toString(generateBinaryTypes("512", false)),
				"0x" + toString(generateBinaryTypes("512", false)) };

		Integer[] intVal = { new Integer(Integer.MAX_VALUE), new Integer(Integer.MIN_VALUE), 10 };

		Long[] bigInt = { new Long(Long.MAX_VALUE), new Long(Long.MIN_VALUE), (long) 10 };

		String[] nvarchar512 = { generateNCharTypes("512", false), generateNCharTypes("0", false),
				generateNCharTypes("225", false) };

		BigDecimal[] smallmoney = { new BigDecimal("214748.3647"), new BigDecimal("-214748.3648"),
				new BigDecimal("10.0000") };

		String[] bit = { "true", "false", "true" };

		String[] varchar512 = { generateCharTypes("512", false), generateCharTypes("1", false),
				generateCharTypes("225", false) };

		tableValues = new ArrayList<ArrayList<Object>>();
		rowNumbers = 0;

		for (int i = 0; i < 3; i++) {
			valuesPerRow = new ArrayList<Object>();

			valuesPerRow.add(char512[i]);
			valuesPerRow.add(date[i]);
			valuesPerRow.add(decimal[i]);
			valuesPerRow.add(money[i]);
			valuesPerRow.add(time[i]);
			valuesPerRow.add(guid[i]);
			valuesPerRow.add(varcharMax[i]);
			valuesPerRow.add(nVarcharMax[i]);
			valuesPerRow.add(smallint[i]);
			valuesPerRow.add(numeric[i]);
			valuesPerRow.add(ntext[i]);
			valuesPerRow.add(varbinaryMax[i]);
			valuesPerRow.add(nchar512[i]);
			valuesPerRow.add(dto[i]);
			valuesPerRow.add(xml[i]);
			valuesPerRow.add(smallDatetime[i]);
			valuesPerRow.add(varbinary512[i]);
			valuesPerRow.add(bit[i]);
			valuesPerRow.add(varchar512[i]);
			valuesPerRow.add(real[i]);
			valuesPerRow.add(tinyint[i]);
			valuesPerRow.add(floatVal[i]);
			valuesPerRow.add(text[i]);
			valuesPerRow.add(binary512[i]);
			valuesPerRow.add(Datetime2[i]);
			valuesPerRow.add((i + 1));
			valuesPerRow.add(Datetime[i]);
			valuesPerRow.add(bigInt[i]);
			valuesPerRow.add(nvarchar512[i]);
			valuesPerRow.add(smallmoney[i]);
			valuesPerRow.add(intVal[i]);

			tableValues.add(valuesPerRow);
			rowNumbers++;
		}
	}

	public static ArrayList<Object> getTableValues(int index) {
		return tableValues.get(index);
	}

	private static String generateCharTypes(String columnLength, boolean nullable) {
		String charSet = normalCharSet;
		return buildCharOrNChar(columnLength, nullable, charSet, 8001);
	}

	private static String generateNCharTypes(String columnLength, boolean nullable) {
		String charSet = normalCharSet;

		return buildCharOrNChar(columnLength, nullable, charSet, 4001);
	}

	private static String buildCharOrNChar(String columnLength, boolean nullable, String charSet, int maxBound) {
		if (nullable) {
			return null;
		}

		int minimumLength = 0;
		int length;
		if (columnLength.toLowerCase().equals("max")) {
			// 50% chance of return value longer than 8000/4000
			if (r.nextBoolean()) {
				length = r.nextInt(100000) + maxBound;
				return buildRandomString(length, charSet);
			} else {
				length = r.nextInt(maxBound - minimumLength) + minimumLength;
				;
				return buildRandomString(length, charSet);
			}
		} else {
			int columnLengthInt = Integer.parseInt(columnLength);
			length = columnLengthInt;
			return buildRandomString(length, charSet);
		}
	}

	private static String buildRandomString(int length, String charSet) {
		StringBuffer sb = new StringBuffer();
		for (int i = 0; i < length; i++) {
			char c = pickRandomChar(charSet);
			sb.append(c);
		}
		return sb.toString();
	}

	private static char pickRandomChar(String charSet) {
		int charSetLength = charSet.length();
		int randomIndex = r.nextInt(charSetLength);
		return charSet.charAt(randomIndex);
	}

	public static byte[] generateBinaryTypes(String columnLength, boolean nullable) {
		int maxBound = 8001;

		if (nullable) {
			return null;
		}

		int minimumLength = 0;
		int length;
		if (columnLength.toLowerCase().equals("max")) {
			// 50% chance of return value longer than 8000/4000
			if (r.nextBoolean()) {
				length = r.nextInt(100000) + maxBound;
				byte[] bytes = new byte[length];
				r.nextBytes(bytes);
				return bytes;
			} else {
				length = r.nextInt(maxBound - minimumLength) + minimumLength;
				byte[] bytes = new byte[length];
				r.nextBytes(bytes);
				return bytes;
			}
		} else {
			int columnLengthInt = Integer.parseInt(columnLength);
			length = columnLengthInt;
			byte[] bytes = new byte[length];
			r.nextBytes(bytes);
			return bytes;
		}
	}

	public static String toString(byte[] bytes) {
		if (null == bytes)
			return null;

		StringBuffer buffer = new StringBuffer();

		// 0xFF => "FF" (it takes two characters to represent one byte)
		for (int i = 0; i < bytes.length; i++)
			toString(buffer, bytes[i]);

		return buffer.toString();
	}

	public static void toString(StringBuffer buffer, byte b) {
		// 0xFF => "FF" (it takes two characters to represent one byte)
		int i = b;
		if (i < 0)
			i = 256 + i; // Bytes are 'signed' in Java

		buffer.append(hexdigits[(i >> 4)]);
		buffer.append(hexdigits[(i & 0xF)]);
	}
	
	private static DateTimeOffset calculateDateTimeOffsetMinMax(String maxOrMin, Integer precision, String tsMinMax){
		int providedTimeZoneInMinutes;
		if(maxOrMin.toLowerCase().equals("max")){
			providedTimeZoneInMinutes = 840;
		}
		else{
			providedTimeZoneInMinutes = -840;
		}
		
		Timestamp tsMax = Timestamp.valueOf(tsMinMax);
		
		Calendar cal = Calendar.getInstance();
		long offset = cal.get(Calendar.ZONE_OFFSET); //in milliseconds
		
		//max Timestamp + difference of current time zone and GMT - provided time zone in milliseconds
		tsMax = new Timestamp(tsMax.getTime() + offset - (providedTimeZoneInMinutes * 60 * 1000));
		
		if(maxOrMin.toLowerCase().equals("max")){
			int precisionDigits = buildPrecision(precision, "9");
			tsMax.setNanos(precisionDigits); 
		}
		
		return microsoft.sql.DateTimeOffset.valueOf(tsMax, providedTimeZoneInMinutes);
	}
	
	private static int buildPrecision(int precision, String charSet){
		String stringValue = calculatePrecisionDigits(precision, charSet);
		return Integer.parseInt(stringValue);
	}
	
	//setNanos(999999900) gives 00:00:00.9999999
	//so, this value has to be 9 digits
	private static String calculatePrecisionDigits(int precision, String charSet){
		StringBuffer sb = new StringBuffer();
		for(int i = 0; i < precision; i++){
			char c = pickRandomChar(charSet);
			sb.append(c);
		}
		
		for(int i = sb.length(); i < 9; i++){
			sb.append("0");
		}

		return sb.toString();
	}

}
