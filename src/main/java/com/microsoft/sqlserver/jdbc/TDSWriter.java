package com.microsoft.sqlserver.jdbc;

import java.io.*;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.Charset;
import java.sql.Timestamp;
import java.text.MessageFormat;
import java.time.OffsetDateTime;
import java.time.OffsetTime;
import java.util.*;
import java.util.Map.Entry;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * TDSWriter implements the client to server TDS data pipe.
 */
public final class TDSWriter {
	private static Logger logger = Logger.getLogger("com.microsoft.sqlserver.jdbc.internals.TDS.Writer");
	private final String traceID;

	final public String toString() {
		return traceID;
	}

	private final TDSChannel tdsChannel;
	private final SQLServerConnection con;

	// Flag to indicate whether data written via writeXXX() calls
	// is loggable. Data is normally loggable. But sensitive
	// data, such as user credentials, should never be logged for
	// security reasons.
	private boolean dataIsLoggable = true;

	void setDataLoggable(boolean value) {
		dataIsLoggable = value;
	}

	private TDSCommand command = null;

	// TDS message type (Query, RPC, DTC, etc.) sent at the beginning
	// of every TDS message header. Value is set when starting a new
	// TDS message of the specified type.
	private byte tdsMessageType;

	private volatile int sendResetConnection = 0;

	// Size (in bytes) of the TDS packets to/from the server.
	// This size is normally fixed for the life of the connection,
	// but it can change once after the logon packet because packet
	// size negotiation happens at logon time.
	private int currentPacketSize = 0;

	// Size of the TDS packet header, which is:
	// byte type
	// byte status
	// short length
	// short SPID
	// byte packet
	// byte window
	private final static int TDS_PACKET_HEADER_SIZE = 8;
	private final static byte[] placeholderHeader = new byte[TDS_PACKET_HEADER_SIZE];

	// Intermediate array used to convert typically "small" values such as
	// fixed-length types
	// (byte, int, long, etc.) and Strings from their native form to bytes for
	// sending to
	// the channel buffers.
	private byte valueBytes[] = new byte[256];

	// Monotonically increasing packet number associated with the current message
	private int packetNum = 0;

	// Bytes for sending decimal/numeric data
	private final static int BYTES4 = 4;
	private final static int BYTES8 = 8;
	private final static int BYTES12 = 12;
	private final static int BYTES16 = 16;
	public final static int BIGDECIMAL_MAX_LENGTH = 0x11;

	// is set to true when EOM is sent for the current message.
	// Note that this variable will never be accessed from multiple threads
	// simultaneously and so it need not be volatile
	private boolean isEOMSent = false;

	boolean isEOMSent() {
		return isEOMSent;
	}

	// Packet data buffers
	private ByteBuffer stagingBuffer;
	private ByteBuffer socketBuffer;
	private ByteBuffer logBuffer;

	private CryptoMetadata cryptoMeta = null;

	TDSWriter(TDSChannel tdsChannel, SQLServerConnection con) {
		this.tdsChannel = tdsChannel;
		this.con = con;
		traceID = "TDSWriter@" + Integer.toHexString(hashCode()) + " (" + con.toString() + ")";
	}

	// TDS message start/end operations

	void preparePacket() throws SQLServerException {
		if (tdsChannel.isLoggingPackets()) {
			Arrays.fill(logBuffer.array(), (byte) 0xFE);
			((Buffer) logBuffer).clear();
		}

		// Write a placeholder packet header. This will be replaced
		// with the real packet header when the packet is flushed.
		writeBytes(placeholderHeader);
	}

	/**
	 * Start a new TDS message.
	 */
	void writeMessageHeader() throws SQLServerException {
		// TDS 7.2 & later:
		// Include ALL_Headers/MARS header in message's first packet
		// Note: The PKT_BULK message does not nees this ALL_HEADERS
		if ((TDS.PKT_QUERY == tdsMessageType || TDS.PKT_DTC == tdsMessageType || TDS.PKT_RPC == tdsMessageType)) {
			boolean includeTraceHeader = false;
			int totalHeaderLength = TDS.MESSAGE_HEADER_LENGTH;
			if (TDS.PKT_QUERY == tdsMessageType || TDS.PKT_RPC == tdsMessageType) {
				if (con.isDenaliOrLater() && !ActivityCorrelator.getCurrent().IsSentToServer()
						&& Util.IsActivityTraceOn()) {
					includeTraceHeader = true;
					totalHeaderLength += TDS.TRACE_HEADER_LENGTH;
				}
			}
			writeInt(totalHeaderLength); // allHeaders.TotalLength (DWORD)
			writeInt(TDS.MARS_HEADER_LENGTH); // MARS header length (DWORD)
			writeShort((short) 2); // allHeaders.HeaderType(MARS header) (USHORT)
			writeBytes(con.getTransactionDescriptor());
			writeInt(1); // marsHeader.OutstandingRequestCount
			if (includeTraceHeader) {
				writeInt(TDS.TRACE_HEADER_LENGTH); // trace header length (DWORD)
				writeTraceHeaderData();
				ActivityCorrelator.setCurrentActivityIdSentFlag(); // set the flag to indicate this ActivityId is sent
			}
		}
	}

	void writeTraceHeaderData() throws SQLServerException {
		ActivityId activityId = ActivityCorrelator.getCurrent();
		final byte[] actIdByteArray = Util.asGuidByteArray(activityId.getId());
		long seqNum = activityId.getSequence();
		writeShort(TDS.HEADERTYPE_TRACE); // trace header type
		writeBytes(actIdByteArray, 0, actIdByteArray.length); // guid part of ActivityId
		writeInt((int) seqNum); // sequence number of ActivityId

		if (logger.isLoggable(Level.FINER))
			logger.finer("Send Trace Header - ActivityID: " + activityId.toString());
	}

	/**
	 * Convenience method to prepare the TDS channel for writing and start a new TDS
	 * message.
	 *
	 * @param command        The TDS command
	 * @param tdsMessageType The TDS message type (PKT_QUERY, PKT_RPC, etc.)
	 */
	void startMessage(TDSCommand command, byte tdsMessageType) throws SQLServerException {
		this.command = command;
		this.tdsMessageType = tdsMessageType;
		this.packetNum = 0;
		this.isEOMSent = false;
		this.dataIsLoggable = true;

		// If the TDS packet size has changed since the last request
		// (which should really only happen after the login packet)
		// then allocate new buffers that are the correct size.
		int negotiatedPacketSize = con.getTDSPacketSize();
		if (currentPacketSize != negotiatedPacketSize) {
			socketBuffer = ByteBuffer.allocate(negotiatedPacketSize).order(ByteOrder.LITTLE_ENDIAN);
			stagingBuffer = ByteBuffer.allocate(negotiatedPacketSize).order(ByteOrder.LITTLE_ENDIAN);
			logBuffer = ByteBuffer.allocate(negotiatedPacketSize).order(ByteOrder.LITTLE_ENDIAN);
			currentPacketSize = negotiatedPacketSize;
		}

		((Buffer) socketBuffer).position(((Buffer) socketBuffer).limit());
		((Buffer) stagingBuffer).clear();

		preparePacket();
		writeMessageHeader();
	}

	final void endMessage() throws SQLServerException {
		if (logger.isLoggable(Level.FINEST))
			logger.finest(toString() + " Finishing TDS message");
		writePacket(TDS.STATUS_BIT_EOM);
	}

	// If a complete request has not been sent to the server,
	// the client MUST send the next packet with both ignore bit (0x02) and EOM bit
	// (0x01)
	// set in the status to cancel the request.
	final boolean ignoreMessage() throws SQLServerException {
		if (packetNum > 0 || TDS.PKT_BULK == this.tdsMessageType) {
			assert !isEOMSent;

			if (logger.isLoggable(Level.FINER))
				logger.finest(toString() + " Finishing TDS message by sending ignore bit and end of message");
			writePacket(TDS.STATUS_BIT_EOM | TDS.STATUS_BIT_ATTENTION);
			return true;
		}
		return false;
	}

	final void resetPooledConnection() {
		if (logger.isLoggable(Level.FINEST))
			logger.finest(toString() + " resetPooledConnection");
		sendResetConnection = TDS.STATUS_BIT_RESET_CONN;
	}

	// Primitive write operations

	void writeByte(byte value) throws SQLServerException {
		if (stagingBuffer.remaining() >= 1) {
			stagingBuffer.put(value);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.put(value);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + 1);
			}
		} else {
			valueBytes[0] = value;
			writeWrappedBytes(valueBytes, 1);
		}
	}

	/**
	 * writing sqlCollation information for sqlVariant type when sending character
	 * types.
	 * 
	 * @param variantType
	 * @throws SQLServerException
	 */
	void writeCollationForSqlVariant(SqlVariant variantType) throws SQLServerException {
		writeInt(variantType.getCollation().getCollationInfo());
		writeByte((byte) (variantType.getCollation().getCollationSortID() & 0xFF));
	}

	void writeChar(char value) throws SQLServerException {
		if (stagingBuffer.remaining() >= 2) {
			stagingBuffer.putChar(value);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.putChar(value);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + 2);
			}
		} else {
			Util.writeShort((short) value, valueBytes, 0);
			writeWrappedBytes(valueBytes, 2);
		}
	}

	void writeShort(short value) throws SQLServerException {
		if (stagingBuffer.remaining() >= 2) {
			stagingBuffer.putShort(value);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.putShort(value);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + 2);
			}
		} else {
			Util.writeShort(value, valueBytes, 0);
			writeWrappedBytes(valueBytes, 2);
		}
	}

	void writeInt(int value) throws SQLServerException {
		if (stagingBuffer.remaining() >= 4) {
			stagingBuffer.putInt(value);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.putInt(value);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + 4);
			}
		} else {
			Util.writeInt(value, valueBytes, 0);
			writeWrappedBytes(valueBytes, 4);
		}
	}

	/**
	 * Append a real value in the TDS stream.
	 * 
	 * @param value the data value
	 */
	void writeReal(Float value) throws SQLServerException {
		writeInt(Float.floatToRawIntBits(value));
	}

	/**
	 * Append a double value in the TDS stream.
	 * 
	 * @param value the data value
	 */
	void writeDouble(double value) throws SQLServerException {
		if (stagingBuffer.remaining() >= 8) {
			stagingBuffer.putDouble(value);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.putDouble(value);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + 8);
			}
		} else {
			long bits = Double.doubleToLongBits(value);
			long mask = 0xFF;
			int nShift = 0;
			for (int i = 0; i < 8; i++) {
				writeByte((byte) ((bits & mask) >> nShift));
				nShift += 8;
				mask = mask << 8;
			}
		}
	}

	/**
	 * Append a big decimal in the TDS stream.
	 * 
	 * @param bigDecimalVal the big decimal data value
	 * @param srcJdbcType   the source JDBCType
	 * @param precision     the precision of the data value
	 * @param scale         the scale of the column
	 * @throws SQLServerException
	 */
	void writeBigDecimal(BigDecimal bigDecimalVal, int srcJdbcType, int precision, int scale)
			throws SQLServerException {
		/*
		 * Length including sign byte One 1-byte unsigned integer that represents the
		 * sign of the decimal value (0 => Negative, 1 => positive) One 4-, 8-, 12-, or
		 * 16-byte signed integer that represents the decimal value multiplied by
		 * 10^scale.
		 */

		/*
		 * setScale of all BigDecimal value based on metadata as scale is not sent
		 * seperately for individual value. Use the rounding used in Server. Say, for
		 * BigDecimal("0.1"), if scale in metdadata is 0, then ArithmeticException would
		 * be thrown if RoundingMode is not set
		 */
		bigDecimalVal = bigDecimalVal.setScale(scale, RoundingMode.HALF_UP);

		// data length + 1 byte for sign
		int bLength = BYTES16 + 1;
		writeByte((byte) (bLength));

		// Byte array to hold all the data and padding bytes.
		byte[] bytes = new byte[bLength];

		byte[] valueBytes = DDC.convertBigDecimalToBytes(bigDecimalVal, scale);
		// removing the precision and scale information from the valueBytes array
		System.arraycopy(valueBytes, 2, bytes, 0, valueBytes.length - 2);
		writeBytes(bytes);
	}

	/**
	 * Append a big decimal inside sql_variant in the TDS stream.
	 * 
	 * @param bigDecimalVal the big decimal data value
	 * @param srcJdbcType   the source JDBCType
	 */
	void writeSqlVariantInternalBigDecimal(BigDecimal bigDecimalVal, int srcJdbcType) throws SQLServerException {
		/*
		 * Length including sign byte One 1-byte unsigned integer that represents the
		 * sign of the decimal value (0 => Negative, 1 => positive) One 16-byte signed
		 * integer that represents the decimal value multiplied by 10^scale. In
		 * sql_variant, we send the bigdecimal with precision 38, therefore we use 16
		 * bytes for the maximum size of this integer.
		 */

		boolean isNegative = (bigDecimalVal.signum() < 0);
		BigInteger bi = bigDecimalVal.unscaledValue();
		if (isNegative) {
			bi = bi.negate();
		}
		int bLength;
		bLength = BYTES16;

		writeByte((byte) (isNegative ? 0 : 1));

		// Get the bytes of the BigInteger value. It is in reverse order, with
		// most significant byte in 0-th element. We need to reverse it first before
		// sending over TDS.
		byte[] unscaledBytes = bi.toByteArray();

		if (unscaledBytes.length > bLength) {
			// If precession of input is greater than maximum allowed (p><= 38) throw
			// Exception
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
			Object[] msgArgs = { JDBCType.of(srcJdbcType) };
			throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH,
					DriverError.NOT_SET, null);
		}

		// Byte array to hold all the reversed and padding bytes.
		byte[] bytes = new byte[bLength];

		// We need to fill up the rest of the array with zeros, as unscaledBytes may
		// have less bytes
		// than the required size for TDS.
		int remaining = bLength - unscaledBytes.length;

		// Reverse the bytes.
		int i, j;
		for (i = 0, j = unscaledBytes.length - 1; i < unscaledBytes.length;)
			bytes[i++] = unscaledBytes[j--];

		// Fill the rest of the array with zeros.
		for (; i < remaining; i++) {
			bytes[i] = (byte) 0x00;
		}
		writeBytes(bytes);
	}

	void writeSmalldatetime(String value) throws SQLServerException {
		GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
		long utcMillis; // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00
						// GMT)
		java.sql.Timestamp timestampValue = java.sql.Timestamp.valueOf(value);
		utcMillis = timestampValue.getTime();

		// Load the calendar with the desired value
		calendar.setTimeInMillis(utcMillis);

		// Number of days since the SQL Server Base Date (January 1, 1900)
		int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(calendar.get(Calendar.YEAR),
				calendar.get(Calendar.DAY_OF_YEAR), TDS.BASE_YEAR_1900);

		// Next, figure out the number of milliseconds since midnight of the current
		// day.
		int millisSinceMidnight = 1000 * calendar.get(Calendar.SECOND) + // Seconds into the current minute
				60 * 1000 * calendar.get(Calendar.MINUTE) + // Minutes into the current hour
				60 * 60 * 1000 * calendar.get(Calendar.HOUR_OF_DAY); // Hours into the current day

		// The last millisecond of the current day is always rounded to the first
		// millisecond
		// of the next day because DATETIME is only accurate to 1/300th of a second.
		if (1000 * 60 * 60 * 24 - 1 <= millisSinceMidnight) {
			++daysSinceSQLBaseDate;
			millisSinceMidnight = 0;
		}

		// Number of days since the SQL Server Base Date (January 1, 1900)
		writeShort((short) daysSinceSQLBaseDate);

		int secondsSinceMidnight = (millisSinceMidnight / 1000);
		int minutesSinceMidnight = (secondsSinceMidnight / 60);

		// Values that are 29.998 seconds or less are rounded down to the nearest minute
		minutesSinceMidnight = ((secondsSinceMidnight % 60) > 29.998) ? minutesSinceMidnight + 1 : minutesSinceMidnight;

		// Minutes since midnight
		writeShort((short) minutesSinceMidnight);
	}

	void writeDatetime(String value) throws SQLServerException {
		GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
		long utcMillis; // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00
						// GMT)
		int subSecondNanos;
		java.sql.Timestamp timestampValue = java.sql.Timestamp.valueOf(value);
		utcMillis = timestampValue.getTime();
		subSecondNanos = timestampValue.getNanos();

		// Load the calendar with the desired value
		calendar.setTimeInMillis(utcMillis);

		// Number of days there have been since the SQL Base Date.
		// These are based on SQL Server algorithms
		int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(calendar.get(Calendar.YEAR),
				calendar.get(Calendar.DAY_OF_YEAR), TDS.BASE_YEAR_1900);

		// Number of milliseconds since midnight of the current day.
		int millisSinceMidnight = (subSecondNanos + Nanos.PER_MILLISECOND / 2) / Nanos.PER_MILLISECOND + // Millis into
																											// the
																											// current
																											// second
				1000 * calendar.get(Calendar.SECOND) + // Seconds into the current minute
				60 * 1000 * calendar.get(Calendar.MINUTE) + // Minutes into the current hour
				60 * 60 * 1000 * calendar.get(Calendar.HOUR_OF_DAY); // Hours into the current day

		// The last millisecond of the current day is always rounded to the first
		// millisecond
		// of the next day because DATETIME is only accurate to 1/300th of a second.
		if (1000 * 60 * 60 * 24 - 1 <= millisSinceMidnight) {
			++daysSinceSQLBaseDate;
			millisSinceMidnight = 0;
		}

		// Last-ditch verification that the value is in the valid range for the
		// DATETIMEN TDS data type (1/1/1753 to 12/31/9999). If it's not, then
		// throw an exception now so that statement execution is safely canceled.
		// Attempting to put an invalid value on the wire would result in a TDS
		// exception, which would close the connection.
		// These are based on SQL Server algorithms
		if (daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1753, 1, TDS.BASE_YEAR_1900)
				|| daysSinceSQLBaseDate >= DDC.daysSinceBaseDate(10000, 1, TDS.BASE_YEAR_1900)) {
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
			Object[] msgArgs = { SSType.DATETIME };
			throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
					DriverError.NOT_SET, null);
		}

		// Number of days since the SQL Server Base Date (January 1, 1900)
		writeInt(daysSinceSQLBaseDate);

		// Milliseconds since midnight (at a resolution of three hundredths of a second)
		writeInt((3 * millisSinceMidnight + 5) / 10);
	}

	void writeDate(String value) throws SQLServerException {
		GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
		long utcMillis;
		java.sql.Date dateValue = java.sql.Date.valueOf(value);
		utcMillis = dateValue.getTime();

		// Load the calendar with the desired value
		calendar.setTimeInMillis(utcMillis);

		writeScaledTemporal(calendar, 0, // subsecond nanos (none for a date value)
				0, // scale (dates are not scaled)
				SSType.DATE);
	}

	void writeTime(java.sql.Timestamp value, int scale) throws SQLServerException {
		GregorianCalendar calendar = initializeCalender(TimeZone.getDefault());
		long utcMillis; // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00
						// GMT)
		int subSecondNanos;
		utcMillis = value.getTime();
		subSecondNanos = value.getNanos();

		// Load the calendar with the desired value
		calendar.setTimeInMillis(utcMillis);

		writeScaledTemporal(calendar, subSecondNanos, scale, SSType.TIME);
	}

	void writeDateTimeOffset(Object value, int scale, SSType destSSType) throws SQLServerException {
		GregorianCalendar calendar;
		TimeZone timeZone; // Time zone to associate with the value in the Gregorian calendar
		long utcMillis; // Value to which the calendar is to be set (in milliseconds 1/1/1970 00:00:00
						// GMT)
		int subSecondNanos;
		int minutesOffset;

		microsoft.sql.DateTimeOffset dtoValue = (microsoft.sql.DateTimeOffset) value;
		utcMillis = dtoValue.getTimestamp().getTime();
		subSecondNanos = dtoValue.getTimestamp().getNanos();
		minutesOffset = dtoValue.getMinutesOffset();

		// If the target data type is DATETIMEOFFSET, then use UTC for the calendar that
		// will hold the value, since writeRPCDateTimeOffset expects a UTC calendar.
		// Otherwise, when converting from DATETIMEOFFSET to other temporal data types,
		// use a local time zone determined by the minutes offset of the value, since
		// the writers for those types expect local calendars.
		timeZone = (SSType.DATETIMEOFFSET == destSSType) ? UTC.timeZone
				: new SimpleTimeZone(minutesOffset * 60 * 1000, "");

		calendar = new GregorianCalendar(timeZone, Locale.US);
		calendar.setLenient(true);
		calendar.clear();
		calendar.setTimeInMillis(utcMillis);

		writeScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);

		writeShort((short) minutesOffset);
	}

	void writeOffsetDateTimeWithTimezone(OffsetDateTime offsetDateTimeValue, int scale) throws SQLServerException {
		GregorianCalendar calendar;
		TimeZone timeZone;
		long utcMillis;
		int subSecondNanos;
		int minutesOffset = 0;

		try {
			// offsetTimeValue.getOffset() returns a ZoneOffset object which has only hours
			// and minutes
			// components. So the result of the division will be an integer always. SQL
			// Server also supports
			// offsets in minutes precision.
			minutesOffset = offsetDateTimeValue.getOffset().getTotalSeconds() / 60;
		} catch (Exception e) {
			throw new SQLServerException(SQLServerException.getErrString("R_zoneOffsetError"), null, // SQLState is null
																										// as this error
																										// is
																										// generated in
																										// the driver
					0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
					e);
		}
		subSecondNanos = offsetDateTimeValue.getNano();

		// writeScaledTemporal() expects subSecondNanos in 9 digits precssion
		// but getNano() used in OffsetDateTime returns precession based on nanoseconds
		// read from csv
		// padding zeros to match the expectation of writeScaledTemporal()
		int padding = 9 - String.valueOf(subSecondNanos).length();
		while (padding > 0) {
			subSecondNanos = subSecondNanos * 10;
			padding--;
		}

		// For TIME_WITH_TIMEZONE, use UTC for the calendar that will hold the value
		timeZone = UTC.timeZone;

		// The behavior is similar to microsoft.sql.DateTimeOffset
		// In Timestamp format, only YEAR needs to have 4 digits. The leading zeros for
		// the rest of the fields can be
		// omitted.
		String offDateTimeStr = String.format("%04d", offsetDateTimeValue.getYear()) + '-'
				+ offsetDateTimeValue.getMonthValue() + '-' + offsetDateTimeValue.getDayOfMonth() + ' '
				+ offsetDateTimeValue.getHour() + ':' + offsetDateTimeValue.getMinute() + ':'
				+ offsetDateTimeValue.getSecond();
		utcMillis = Timestamp.valueOf(offDateTimeStr).getTime();
		calendar = initializeCalender(timeZone);
		calendar.setTimeInMillis(utcMillis);

		// Local timezone value in minutes
		int minuteAdjustment = ((TimeZone.getDefault().getRawOffset()) / (60 * 1000));
		// check if date is in day light savings and add daylight saving minutes
		if (TimeZone.getDefault().inDaylightTime(calendar.getTime()))
			minuteAdjustment += (TimeZone.getDefault().getDSTSavings()) / (60 * 1000);
		// If the local time is negative then positive minutesOffset must be subtracted
		// from calender
		minuteAdjustment += (minuteAdjustment < 0) ? (minutesOffset * (-1)) : minutesOffset;
		calendar.add(Calendar.MINUTE, minuteAdjustment);

		writeScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);
		writeShort((short) minutesOffset);
	}

	void writeOffsetTimeWithTimezone(OffsetTime offsetTimeValue, int scale) throws SQLServerException {
		GregorianCalendar calendar;
		TimeZone timeZone;
		long utcMillis;
		int subSecondNanos;
		int minutesOffset = 0;

		try {
			// offsetTimeValue.getOffset() returns a ZoneOffset object which has only hours
			// and minutes
			// components. So the result of the division will be an integer always. SQL
			// Server also supports
			// offsets in minutes precision.
			minutesOffset = offsetTimeValue.getOffset().getTotalSeconds() / 60;
		} catch (Exception e) {
			throw new SQLServerException(SQLServerException.getErrString("R_zoneOffsetError"), null, // SQLState is null
																										// as this error
																										// is
																										// generated in
																										// the driver
					0, // Use 0 instead of DriverError.NOT_SET to use the correct constructor
					e);
		}
		subSecondNanos = offsetTimeValue.getNano();

		// writeScaledTemporal() expects subSecondNanos in 9 digits precssion
		// but getNano() used in OffsetDateTime returns precession based on nanoseconds
		// read from csv
		// padding zeros to match the expectation of writeScaledTemporal()
		int padding = 9 - String.valueOf(subSecondNanos).length();
		while (padding > 0) {
			subSecondNanos = subSecondNanos * 10;
			padding--;
		}

		// For TIME_WITH_TIMEZONE, use UTC for the calendar that will hold the value
		timeZone = UTC.timeZone;

		// Using TDS.BASE_YEAR_1900, based on SQL server behavious
		// If date only contains a time part, the return value is 1900, the base year.
		// https://msdn.microsoft.com/en-us/library/ms186313.aspx
		// In Timestamp format, leading zeros for the fields can be omitted.
		String offsetTimeStr = TDS.BASE_YEAR_1900 + "-01-01" + ' ' + offsetTimeValue.getHour() + ':'
				+ offsetTimeValue.getMinute() + ':' + offsetTimeValue.getSecond();
		utcMillis = Timestamp.valueOf(offsetTimeStr).getTime();

		calendar = initializeCalender(timeZone);
		calendar.setTimeInMillis(utcMillis);

		int minuteAdjustment = (TimeZone.getDefault().getRawOffset()) / (60 * 1000);
		// check if date is in day light savings and add daylight saving minutes to
		// Local timezone(in minutes)
		if (TimeZone.getDefault().inDaylightTime(calendar.getTime()))
			minuteAdjustment += ((TimeZone.getDefault().getDSTSavings()) / (60 * 1000));
		// If the local time is negative then positive minutesOffset must be subtracted
		// from calender
		minuteAdjustment += (minuteAdjustment < 0) ? (minutesOffset * (-1)) : minutesOffset;
		calendar.add(Calendar.MINUTE, minuteAdjustment);

		writeScaledTemporal(calendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);
		writeShort((short) minutesOffset);
	}

	void writeLong(long value) throws SQLServerException {
		if (stagingBuffer.remaining() >= 8) {
			stagingBuffer.putLong(value);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.putLong(value);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + 8);
			}
		} else {
			Util.writeLong(value, valueBytes, 0);
			writeWrappedBytes(valueBytes, 8);
		}
	}

	void writeBytes(byte[] value) throws SQLServerException {
		writeBytes(value, 0, value.length);
	}

	void writeBytes(byte[] value, int offset, int length) throws SQLServerException {
		assert length <= value.length;

		int bytesWritten = 0;
		int bytesToWrite;

		if (logger.isLoggable(Level.FINEST))
			logger.finest(toString() + " Writing " + length + " bytes");

		while ((bytesToWrite = length - bytesWritten) > 0) {
			if (0 == stagingBuffer.remaining())
				writePacket(TDS.STATUS_NORMAL);

			if (bytesToWrite > stagingBuffer.remaining())
				bytesToWrite = stagingBuffer.remaining();

			stagingBuffer.put(value, offset + bytesWritten, bytesToWrite);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.put(value, offset + bytesWritten, bytesToWrite);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + bytesToWrite);
			}

			bytesWritten += bytesToWrite;
		}
	}

	void writeWrappedBytes(byte value[], int valueLength) throws SQLServerException {
		// This function should only be used to write a value that is longer than
		// what remains in the current staging buffer. However, the value must
		// be short enough to fit in an empty buffer.
		assert valueLength <= value.length;

		int remaining = stagingBuffer.remaining();
		assert remaining < valueLength;

		assert valueLength <= stagingBuffer.capacity();

		// Fill any remaining space in the staging buffer
		remaining = stagingBuffer.remaining();
		if (remaining > 0) {
			stagingBuffer.put(value, 0, remaining);
			if (tdsChannel.isLoggingPackets()) {
				if (dataIsLoggable)
					logBuffer.put(value, 0, remaining);
				else
					((Buffer) logBuffer).position(((Buffer) logBuffer).position() + remaining);
			}
		}

		writePacket(TDS.STATUS_NORMAL);

		// After swapping, the staging buffer should once again be empty, so the
		// remainder of the value can be written to it.
		stagingBuffer.put(value, remaining, valueLength - remaining);
		if (tdsChannel.isLoggingPackets()) {
			if (dataIsLoggable)
				logBuffer.put(value, remaining, valueLength - remaining);
			else
				((Buffer) logBuffer).position(((Buffer) logBuffer).position() + remaining);
		}
	}

	void writeString(String value) throws SQLServerException {
		int charsCopied = 0;
		int length = value.length();
		while (charsCopied < length) {
			int bytesToCopy = 2 * (length - charsCopied);

			if (bytesToCopy > valueBytes.length)
				bytesToCopy = valueBytes.length;

			int bytesCopied = 0;
			while (bytesCopied < bytesToCopy) {
				char ch = value.charAt(charsCopied++);
				valueBytes[bytesCopied++] = (byte) ((ch >> 0) & 0xFF);
				valueBytes[bytesCopied++] = (byte) ((ch >> 8) & 0xFF);
			}

			writeBytes(valueBytes, 0, bytesCopied);
		}
	}

	void writeStream(InputStream inputStream, long advertisedLength, boolean writeChunkSizes)
			throws SQLServerException {
		assert DataTypes.UNKNOWN_STREAM_LENGTH == advertisedLength || advertisedLength >= 0;

		long actualLength = 0;
		final byte[] streamByteBuffer = new byte[4 * currentPacketSize];
		int bytesRead = 0;
		int bytesToWrite;
		do {
			// Read in next chunk
			for (bytesToWrite = 0; -1 != bytesRead
					&& bytesToWrite < streamByteBuffer.length; bytesToWrite += bytesRead) {
				try {
					bytesRead = inputStream.read(streamByteBuffer, bytesToWrite,
							streamByteBuffer.length - bytesToWrite);
				} catch (IOException e) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
					Object[] msgArgs = { e.toString() };
					error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
				}

				if (-1 == bytesRead)
					break;

				// Check for invalid bytesRead returned from InputStream.read
				if (bytesRead < 0 || bytesRead > streamByteBuffer.length - bytesToWrite) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
					Object[] msgArgs = { SQLServerException.getErrString("R_streamReadReturnedInvalidValue") };
					error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
				}
			}

			// Write it out
			if (writeChunkSizes)
				writeInt(bytesToWrite);

			writeBytes(streamByteBuffer, 0, bytesToWrite);
			actualLength += bytesToWrite;
		} while (-1 != bytesRead || bytesToWrite > 0);

		// If we were given an input stream length that we had to match and
		// the actual stream length did not match then cancel the request.
		if (DataTypes.UNKNOWN_STREAM_LENGTH != advertisedLength && actualLength != advertisedLength) {
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
			Object[] msgArgs = { advertisedLength, actualLength };
			error(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET);
		}
	}

	/*
	 * Adding another function for writing non-unicode reader instead of
	 * re-factoring the writeReader() for performance efficiency. As this method
	 * will only be used in bulk copy, it needs to be efficient. Note: Any changes
	 * in algorithm/logic should propagate to both writeReader() and
	 * writeNonUnicodeReader().
	 */

	void writeNonUnicodeReader(Reader reader, long advertisedLength, boolean isDestBinary, Charset charSet)
			throws SQLServerException {
		assert DataTypes.UNKNOWN_STREAM_LENGTH == advertisedLength || advertisedLength >= 0;

		long actualLength = 0;
		char[] streamCharBuffer = new char[currentPacketSize];
		// The unicode version, writeReader() allocates a byte buffer that is 4 times
		// the currentPacketSize, not sure
		// why.
		byte[] streamByteBuffer = new byte[currentPacketSize];
		int charsRead = 0;
		int charsToWrite;
		int bytesToWrite;
		String streamString;

		do {
			// Read in next chunk
			for (charsToWrite = 0; -1 != charsRead
					&& charsToWrite < streamCharBuffer.length; charsToWrite += charsRead) {
				try {
					charsRead = reader.read(streamCharBuffer, charsToWrite, streamCharBuffer.length - charsToWrite);
				} catch (IOException e) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
					Object[] msgArgs = { e.toString() };
					error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
				}

				if (-1 == charsRead)
					break;

				// Check for invalid bytesRead returned from Reader.read
				if (charsRead < 0 || charsRead > streamCharBuffer.length - charsToWrite) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
					Object[] msgArgs = { SQLServerException.getErrString("R_streamReadReturnedInvalidValue") };
					error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
				}
			}

			if (!isDestBinary) {
				// Write it out
				// This also writes the PLP_TERMINATOR token after all the data in the the
				// stream are sent.
				// The Do-While loop goes on one more time as charsToWrite is greater than 0 for
				// the last chunk, and
				// in this last round the only thing that is written is an int value of 0, which
				// is the PLP Terminator
				// token(0x00000000).
				writeInt(charsToWrite);

				for (int charsCopied = 0; charsCopied < charsToWrite; ++charsCopied) {
					if (null == charSet) {
						streamByteBuffer[charsCopied] = (byte) (streamCharBuffer[charsCopied] & 0xFF);
					} else {
						// encoding as per collation
						streamByteBuffer[charsCopied] = new String(streamCharBuffer[charsCopied] + "")
								.getBytes(charSet)[0];
					}
				}
				writeBytes(streamByteBuffer, 0, charsToWrite);
			} else {
				bytesToWrite = charsToWrite;
				if (0 != charsToWrite)
					bytesToWrite = charsToWrite / 2;

				streamString = new String(streamCharBuffer);
				byte[] bytes = ParameterUtils.HexToBin(streamString.trim());
				writeInt(bytesToWrite);
				writeBytes(bytes, 0, bytesToWrite);
			}
			actualLength += charsToWrite;
		} while (-1 != charsRead || charsToWrite > 0);

		// If we were given an input stream length that we had to match and
		// the actual stream length did not match then cancel the request.
		if (DataTypes.UNKNOWN_STREAM_LENGTH != advertisedLength && actualLength != advertisedLength) {
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
			Object[] msgArgs = { advertisedLength, actualLength };
			error(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET);
		}
	}

	/*
	 * Note: There is another method with same code logic for non unicode reader,
	 * writeNonUnicodeReader(), implemented for performance efficiency. Any changes
	 * in algorithm/logic should propagate to both writeReader() and
	 * writeNonUnicodeReader().
	 */
	void writeReader(Reader reader, long advertisedLength, boolean writeChunkSizes) throws SQLServerException {
		assert DataTypes.UNKNOWN_STREAM_LENGTH == advertisedLength || advertisedLength >= 0;

		long actualLength = 0;
		char[] streamCharBuffer = new char[2 * currentPacketSize];
		byte[] streamByteBuffer = new byte[4 * currentPacketSize];
		int charsRead = 0;
		int charsToWrite;
		do {
			// Read in next chunk
			for (charsToWrite = 0; -1 != charsRead
					&& charsToWrite < streamCharBuffer.length; charsToWrite += charsRead) {
				try {
					charsRead = reader.read(streamCharBuffer, charsToWrite, streamCharBuffer.length - charsToWrite);
				} catch (IOException e) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
					Object[] msgArgs = { e.toString() };
					error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
				}

				if (-1 == charsRead)
					break;

				// Check for invalid bytesRead returned from Reader.read
				if (charsRead < 0 || charsRead > streamCharBuffer.length - charsToWrite) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_errorReadingStream"));
					Object[] msgArgs = { SQLServerException.getErrString("R_streamReadReturnedInvalidValue") };
					error(form.format(msgArgs), SQLState.DATA_EXCEPTION_NOT_SPECIFIC, DriverError.NOT_SET);
				}
			}

			// Write it out
			if (writeChunkSizes)
				writeInt(2 * charsToWrite);

			// Convert from Unicode characters to bytes
			//
			// Note: The following inlined code is much faster than the equivalent
			// call to (new String(streamCharBuffer)).getBytes("UTF-16LE") because it
			// saves a conversion to String and use of Charset in that conversion.
			for (int charsCopied = 0; charsCopied < charsToWrite; ++charsCopied) {
				streamByteBuffer[2 * charsCopied] = (byte) ((streamCharBuffer[charsCopied] >> 0) & 0xFF);
				streamByteBuffer[2 * charsCopied + 1] = (byte) ((streamCharBuffer[charsCopied] >> 8) & 0xFF);
			}

			writeBytes(streamByteBuffer, 0, 2 * charsToWrite);
			actualLength += charsToWrite;
		} while (-1 != charsRead || charsToWrite > 0);

		// If we were given an input stream length that we had to match and
		// the actual stream length did not match then cancel the request.
		if (DataTypes.UNKNOWN_STREAM_LENGTH != advertisedLength && actualLength != advertisedLength) {
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_mismatchedStreamLength"));
			Object[] msgArgs = { advertisedLength, actualLength };
			error(form.format(msgArgs), SQLState.DATA_EXCEPTION_LENGTH_MISMATCH, DriverError.NOT_SET);
		}
	}

	GregorianCalendar initializeCalender(TimeZone timeZone) {
		GregorianCalendar calendar;

		// Create the calendar that will hold the value. For DateTimeOffset values, the
		// calendar's
		// time zone is UTC. For other values, the calendar's time zone is a local time
		// zone.
		calendar = new GregorianCalendar(timeZone, Locale.US);

		// Set the calendar lenient to allow setting the DAY_OF_YEAR and MILLISECOND
		// fields
		// to roll other fields to their correct values.
		calendar.setLenient(true);

		// Clear the calendar of any existing state. The state of a new Calendar object
		// always
		// reflects the current date, time, DST offset, etc.
		calendar.clear();

		return calendar;
	}

	final void error(String reason, SQLState sqlState, DriverError driverError) throws SQLServerException {
		assert null != command;
		command.interrupt(reason);
		throw new SQLServerException(reason, sqlState, driverError, null);
	}

	/**
	 * Sends an attention signal to the server, if necessary, to tell it to stop
	 * processing the current command on this connection.
	 *
	 * If no packets of the command's request have yet been sent to the server, then
	 * no attention signal needs to be sent. The interrupt will be handled entirely
	 * by the driver.
	 *
	 * This method does not need synchronization as it does not manipulate interrupt
	 * state and writing is guaranteed to occur only from one thread at a time.
	 */
	final boolean sendAttention() throws SQLServerException {
		// If any request packets were already written to the server then send an
		// attention signal to the server to tell it to ignore the request or
		// cancel its execution.
		if (packetNum > 0) {
			// Ideally, we would want to add the following assert here.
			// But to add that the variable isEOMSent would have to be made
			// volatile as this piece of code would be reached from multiple
			// threads. So, not doing it to avoid perf hit. Note that
			// isEOMSent would be updated in writePacket everytime an EOM is sent
			// assert isEOMSent;

			if (logger.isLoggable(Level.FINE))
				logger.fine(this + ": sending attention...");

			++tdsChannel.numMsgsSent;

			startMessage(command, TDS.PKT_CANCEL_REQ);
			endMessage();

			return true;
		}

		return false;
	}

	private void writePacket(int tdsMessageStatus) throws SQLServerException {
		final boolean atEOM = (TDS.STATUS_BIT_EOM == (TDS.STATUS_BIT_EOM & tdsMessageStatus));
		final boolean isCancelled = ((TDS.PKT_CANCEL_REQ == tdsMessageType)
				|| ((tdsMessageStatus & TDS.STATUS_BIT_ATTENTION) == TDS.STATUS_BIT_ATTENTION));
		// Before writing each packet to the channel, check if an interrupt has
		// occurred.
		if (null != command && (!isCancelled))
			command.checkForInterrupt();

		writePacketHeader(tdsMessageStatus | sendResetConnection);
		sendResetConnection = 0;

		flush(atEOM);

		// If this is the last packet then flush the remainder of the request
		// through the socket. The first flush() call ensured that data currently
		// waiting in the socket buffer was sent, flipped the buffers, and started
		// sending data from the staging buffer (flipped to be the new socket buffer).
		// This flush() call ensures that all remaining data in the socket buffer is
		// sent.
		if (atEOM) {
			flush(atEOM);
			isEOMSent = true;
			++tdsChannel.numMsgsSent;
		}

		// If we just sent the first login request packet and SSL encryption was enabled
		// for login only, then disable SSL now.
		if (TDS.PKT_LOGON70 == tdsMessageType && 1 == packetNum
				&& TDS.ENCRYPT_OFF == con.getNegotiatedEncryptionLevel()) {
			tdsChannel.disableSSL();
		}

		// Notify the currently associated command (if any) that we have written the
		// last
		// of the response packets to the channel.
		if (null != command && (!isCancelled) && atEOM)
			command.onRequestComplete();
	}

	private void writePacketHeader(int tdsMessageStatus) {
		int tdsMessageLength = ((Buffer) stagingBuffer).position();
		++packetNum;

		// Write the TDS packet header back at the start of the staging buffer
		stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_TYPE, tdsMessageType);
		stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_STATUS, (byte) tdsMessageStatus);
		stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH, (byte) ((tdsMessageLength >> 8) & 0xFF)); // Note: message
																										// length is 16
																										// bits,
		stagingBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH + 1, (byte) ((tdsMessageLength >> 0) & 0xFF)); // written BIG
																											// ENDIAN
		stagingBuffer.put(TDS.PACKET_HEADER_SPID, (byte) ((tdsChannel.getSPID() >> 8) & 0xFF)); // Note: SPID is 16
																								// bits,
		stagingBuffer.put(TDS.PACKET_HEADER_SPID + 1, (byte) ((tdsChannel.getSPID() >> 0) & 0xFF)); // written BIG
																									// ENDIAN
		stagingBuffer.put(TDS.PACKET_HEADER_SEQUENCE_NUM, (byte) (packetNum % 256));
		stagingBuffer.put(TDS.PACKET_HEADER_WINDOW, (byte) 0); // Window (Reserved/Not used)

		// Write the header to the log buffer too if logging.
		if (tdsChannel.isLoggingPackets()) {
			logBuffer.put(TDS.PACKET_HEADER_MESSAGE_TYPE, tdsMessageType);
			logBuffer.put(TDS.PACKET_HEADER_MESSAGE_STATUS, (byte) tdsMessageStatus);
			logBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH, (byte) ((tdsMessageLength >> 8) & 0xFF)); // Note: message
																										// length is 16
																										// bits,
			logBuffer.put(TDS.PACKET_HEADER_MESSAGE_LENGTH + 1, (byte) ((tdsMessageLength >> 0) & 0xFF)); // written BIG
																											// ENDIAN
			logBuffer.put(TDS.PACKET_HEADER_SPID, (byte) ((tdsChannel.getSPID() >> 8) & 0xFF)); // Note: SPID is 16
																								// bits,
			logBuffer.put(TDS.PACKET_HEADER_SPID + 1, (byte) ((tdsChannel.getSPID() >> 0) & 0xFF)); // written BIG
																									// ENDIAN
			logBuffer.put(TDS.PACKET_HEADER_SEQUENCE_NUM, (byte) (packetNum % 256));
			logBuffer.put(TDS.PACKET_HEADER_WINDOW, (byte) 0); // Window (Reserved/Not used);
		}
	}

	void flush(boolean atEOM) throws SQLServerException {
		// First, flush any data left in the socket buffer.
		tdsChannel.write(socketBuffer.array(), ((Buffer) socketBuffer).position(), socketBuffer.remaining());
		((Buffer) socketBuffer).position(((Buffer) socketBuffer).limit());

		// If there is data in the staging buffer that needs to be written
		// to the socket, the socket buffer is now empty, so swap buffers
		// and start writing data from the staging buffer.
		if (((Buffer) stagingBuffer).position() >= TDS_PACKET_HEADER_SIZE) {
			// Swap the packet buffers ...
			ByteBuffer swapBuffer = stagingBuffer;
			stagingBuffer = socketBuffer;
			socketBuffer = swapBuffer;

			// ... and prepare to send data from the from the new socket
			// buffer (the old staging buffer).
			//
			// We need to use flip() rather than rewind() here so that
			// the socket buffer's limit is properly set for the last
			// packet, which may be shorter than the other packets.
			((Buffer) socketBuffer).flip();
			((Buffer) stagingBuffer).clear();

			// If we are logging TDS packets then log the packet we're about
			// to send over the wire now.
			if (tdsChannel.isLoggingPackets()) {
				tdsChannel.logPacket(logBuffer.array(), 0, ((Buffer) socketBuffer).limit(),
						this.toString() + " sending packet (" + ((Buffer) socketBuffer).limit() + " bytes)");
			}

			// Prepare for the next packet
			if (!atEOM)
				preparePacket();

			// Finally, start sending data from the new socket buffer.
			tdsChannel.write(socketBuffer.array(), ((Buffer) socketBuffer).position(), socketBuffer.remaining());
			((Buffer) socketBuffer).position(((Buffer) socketBuffer).limit());
		}
	}

	// Composite write operations

	/**
	 * Write out elements common to all RPC values.
	 * 
	 * @param sName   the optional parameter name
	 * @param bOut    boolean true if the value that follows is being registered as
	 *                an ouput parameter
	 * @param tdsType TDS type of the value that follows
	 */
	void writeRPCNameValType(String sName, boolean bOut, TDSType tdsType) throws SQLServerException {
		int nNameLen = 0;

		if (null != sName)
			nNameLen = sName.length() + 1; // The @ prefix is required for the param

		writeByte((byte) nNameLen); // param name len
		if (nNameLen > 0) {
			writeChar('@');
			writeString(sName);
		}

		if (null != cryptoMeta)
			writeByte((byte) (bOut ? 1 | TDS.AE_METADATA : 0 | TDS.AE_METADATA)); // status
		else
			writeByte((byte) (bOut ? 1 : 0)); // status
		writeByte(tdsType.byteValue()); // type
	}

	/**
	 * Append a boolean value in RPC transmission format.
	 * 
	 * @param sName        the optional parameter name
	 * @param booleanValue the data value
	 * @param bOut         boolean true if the data value is being registered as an
	 *                     ouput parameter
	 */
	void writeRPCBit(String sName, Boolean booleanValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.BITN);
		writeByte((byte) 1); // max length of datatype
		if (null == booleanValue) {
			writeByte((byte) 0); // len of data bytes
		} else {
			writeByte((byte) 1); // length of datatype
			writeByte((byte) (booleanValue ? 1 : 0));
		}
	}

	/**
	 * Append a short value in RPC transmission format.
	 * 
	 * @param sName      the optional parameter name
	 * @param shortValue the data value
	 * @param bOut       boolean true if the data value is being registered as an
	 *                   ouput parameter
	 */
	void writeRPCByte(String sName, Byte byteValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.INTN);
		writeByte((byte) 1); // max length of datatype
		if (null == byteValue) {
			writeByte((byte) 0); // len of data bytes
		} else {
			writeByte((byte) 1); // length of datatype
			writeByte(byteValue);
		}
	}

	/**
	 * Append a short value in RPC transmission format.
	 * 
	 * @param sName      the optional parameter name
	 * @param shortValue the data value
	 * @param bOut       boolean true if the data value is being registered as an
	 *                   ouput parameter
	 */
	void writeRPCShort(String sName, Short shortValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.INTN);
		writeByte((byte) 2); // max length of datatype
		if (null == shortValue) {
			writeByte((byte) 0); // len of data bytes
		} else {
			writeByte((byte) 2); // length of datatype
			writeShort(shortValue);
		}
	}

	/**
	 * Append an int value in RPC transmission format.
	 * 
	 * @param sName    the optional parameter name
	 * @param intValue the data value
	 * @param bOut     boolean true if the data value is being registered as an
	 *                 ouput parameter
	 */
	void writeRPCInt(String sName, Integer intValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.INTN);
		writeByte((byte) 4); // max length of datatype
		if (null == intValue) {
			writeByte((byte) 0); // len of data bytes
		} else {
			writeByte((byte) 4); // length of datatype
			writeInt(intValue);
		}
	}

	/**
	 * Append a long value in RPC transmission format.
	 * 
	 * @param sName     the optional parameter name
	 * @param longValue the data value
	 * @param bOut      boolean true if the data value is being registered as an
	 *                  ouput parameter
	 */
	void writeRPCLong(String sName, Long longValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.INTN);
		writeByte((byte) 8); // max length of datatype
		if (null == longValue) {
			writeByte((byte) 0); // len of data bytes
		} else {
			writeByte((byte) 8); // length of datatype
			writeLong(longValue);
		}
	}

	/**
	 * Append a real value in RPC transmission format.
	 * 
	 * @param sName      the optional parameter name
	 * @param floatValue the data value
	 * @param bOut       boolean true if the data value is being registered as an
	 *                   ouput parameter
	 */
	void writeRPCReal(String sName, Float floatValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.FLOATN);

		// Data and length
		if (null == floatValue) {
			writeByte((byte) 4); // max length
			writeByte((byte) 0); // actual length (0 == null)
		} else {
			writeByte((byte) 4); // max length
			writeByte((byte) 4); // actual length
			writeInt(Float.floatToRawIntBits(floatValue));
		}
	}

	void writeRPCSqlVariant(String sName, SqlVariant sqlVariantValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.SQL_VARIANT);

		// Data and length
		if (null == sqlVariantValue) {
			writeInt(0); // max length
			writeInt(0); // actual length
		}
	}

	/**
	 * Append a double value in RPC transmission format.
	 * 
	 * @param sName       the optional parameter name
	 * @param doubleValue the data value
	 * @param bOut        boolean true if the data value is being registered as an
	 *                    ouput parameter
	 */
	void writeRPCDouble(String sName, Double doubleValue, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.FLOATN);

		int l = 8;
		writeByte((byte) l); // max length of datatype

		// Data and length
		if (null == doubleValue) {
			writeByte((byte) 0); // len of data bytes
		} else {
			writeByte((byte) l); // len of data bytes
			long bits = Double.doubleToLongBits(doubleValue);
			long mask = 0xFF;
			int nShift = 0;
			for (int i = 0; i < 8; i++) {
				writeByte((byte) ((bits & mask) >> nShift));
				nShift += 8;
				mask = mask << 8;
			}
		}
	}

	/**
	 * Append a big decimal in RPC transmission format.
	 * 
	 * @param sName   the optional parameter name
	 * @param bdValue the data value
	 * @param nScale  the desired scale
	 * @param bOut    boolean true if the data value is being registered as an ouput
	 *                parameter
	 */
	void writeRPCBigDecimal(String sName, BigDecimal bdValue, int nScale, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.DECIMALN);
		writeByte((byte) 0x11); // maximum length
		writeByte((byte) SQLServerConnection.maxDecimalPrecision); // precision

		byte[] valueBytes = DDC.convertBigDecimalToBytes(bdValue, nScale);
		writeBytes(valueBytes, 0, valueBytes.length);
	}

	/**
	 * Appends a standard v*max header for RPC parameter transmission.
	 * 
	 * @param headerLength the total length of the PLP data block.
	 * @param isNull       true if the value is NULL.
	 * @param collation    The SQL collation associated with the value that follows
	 *                     the v*max header. Null for non-textual types.
	 */
	void writeVMaxHeader(long headerLength, boolean isNull, SQLCollation collation) throws SQLServerException {
		// Send v*max length indicator 0xFFFF.
		writeShort((short) 0xFFFF);

		// Send collation if requested.
		if (null != collation)
			collation.writeCollation(this);

		// Handle null here and return, we're done here if it's null.
		if (isNull) {
			// Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
			writeLong(0xFFFFFFFFFFFFFFFFL);
		} else if (DataTypes.UNKNOWN_STREAM_LENGTH == headerLength) {
			// Append v*max length.
			// UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
			writeLong(0xFFFFFFFFFFFFFFFEL);

			// NOTE: Don't send the first chunk length, this will be calculated by caller.
		} else {
			// For v*max types with known length, length is <totallength8><chunklength4>
			// We're sending same total length as chunk length (as we're sending 1 chunk).
			writeLong(headerLength);
		}
	}

	/**
	 * Utility for internal writeRPCString calls
	 */
	void writeRPCStringUnicode(String sValue) throws SQLServerException {
		writeRPCStringUnicode(null, sValue, false, null);
	}

	/**
	 * Writes a string value as Unicode for RPC
	 * 
	 * @param sName     the optional parameter name
	 * @param sValue    the data value
	 * @param bOut      boolean true if the data value is being registered as an
	 *                  ouput parameter
	 * @param collation the collation of the data value
	 */
	void writeRPCStringUnicode(String sName, String sValue, boolean bOut, SQLCollation collation)
			throws SQLServerException {
		boolean bValueNull = (sValue == null);
		int nValueLen = bValueNull ? 0 : (2 * sValue.length());
		boolean isShortValue = nValueLen <= DataTypes.SHORT_VARTYPE_MAX_BYTES;

		// Textual RPC requires a collation. If none is provided, as is the case when
		// the SSType is non-textual, then use the database collation by default.
		if (null == collation)
			collation = con.getDatabaseCollation();

		// Use PLP encoding on Yukon and later with long values and OUT parameters
		boolean usePLP = (!isShortValue || bOut);
		if (usePLP) {
			writeRPCNameValType(sName, bOut, TDSType.NVARCHAR);

			// Handle Yukon v*max type header here.
			writeVMaxHeader(nValueLen, // Length
					bValueNull, // Is null?
					collation);

			// Send the data.
			if (!bValueNull) {
				if (nValueLen > 0) {
					writeInt(nValueLen);
					writeString(sValue);
				}

				// Send the terminator PLP chunk.
				writeInt(0);
			}
		} else // non-PLP type
		{
			// Write maximum length of data
			if (isShortValue) {
				writeRPCNameValType(sName, bOut, TDSType.NVARCHAR);
				writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
			} else {
				writeRPCNameValType(sName, bOut, TDSType.NTEXT);
				writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
			}

			collation.writeCollation(this);

			// Data and length
			if (bValueNull) {
				writeShort((short) -1); // actual len
			} else {
				// Write actual length of data
				if (isShortValue)
					writeShort((short) nValueLen);
				else
					writeInt(nValueLen);

				// If length is zero, we're done.
				if (0 != nValueLen)
					writeString(sValue); // data
			}
		}
	}

	void writeTVP(TVP value) throws SQLServerException {
		if (!value.isNull()) {
			writeByte((byte) 0); // status
		} else {
			// Default TVP
			writeByte((byte) TDS.TVP_STATUS_DEFAULT); // default TVP
		}

		writeByte((byte) TDS.TDS_TVP);

		/*
		 * TVP_TYPENAME = DbName OwningSchema TypeName
		 */
		// Database where TVP type resides
		if (null != value.getDbNameTVP()) {
			writeByte((byte) value.getDbNameTVP().length());
			writeString(value.getDbNameTVP());
		} else
			writeByte((byte) 0x00); // empty DB name

		// Schema where TVP type resides
		if (null != value.getOwningSchemaNameTVP()) {
			writeByte((byte) value.getOwningSchemaNameTVP().length());
			writeString(value.getOwningSchemaNameTVP());
		} else
			writeByte((byte) 0x00); // empty Schema name

		// TVP type name
		if (null != value.getTVPName()) {
			writeByte((byte) value.getTVPName().length());
			writeString(value.getTVPName());
		} else
			writeByte((byte) 0x00); // empty TVP name

		if (!value.isNull()) {
			writeTVPColumnMetaData(value);

			// optional OrderUnique metadata
			writeTvpOrderUnique(value);
		} else {
			writeShort((short) TDS.TVP_NULL_TOKEN);
		}

		// TVP_END_TOKEN
		writeByte((byte) 0x00);

		try {
			writeTVPRows(value);
		} catch (NumberFormatException e) {
			throw new SQLServerException(SQLServerException.getErrString("R_TVPInvalidColumnValue"), e);
		} catch (ClassCastException e) {
			throw new SQLServerException(SQLServerException.getErrString("R_TVPInvalidColumnValue"), e);
		}
	}

	void writeTVPRows(TVP value) throws SQLServerException {
		boolean tdsWritterCached = false;
		ByteBuffer cachedTVPHeaders = null;
		TDSCommand cachedCommand = null;

		boolean cachedRequestComplete = false;
		boolean cachedInterruptsEnabled = false;
		boolean cachedProcessedResponse = false;

		if (!value.isNull()) {

			// If the preparedStatement and the ResultSet are created by the same
			// connection, and TVP is set with
			// ResultSet and Server Cursor
			// is used, the tdsWriter of the calling preparedStatement is overwritten by the
			// SQLServerResultSet#next()
			// method when fetching new rows.
			// Therefore, we need to send TVP data row by row before fetching new row.
			if (TVPType.ResultSet == value.tvpType) {
				if ((null != value.sourceResultSet) && (value.sourceResultSet instanceof SQLServerResultSet)) {
					SQLServerResultSet sourceResultSet = (SQLServerResultSet) value.sourceResultSet;
					SQLServerStatement src_stmt = (SQLServerStatement) sourceResultSet.getStatement();
					int resultSetServerCursorId = sourceResultSet.getServerCursorId();

					if (con.equals(src_stmt.getConnection()) && 0 != resultSetServerCursorId) {
						cachedTVPHeaders = ByteBuffer.allocate(stagingBuffer.capacity()).order(stagingBuffer.order());
						cachedTVPHeaders.put(stagingBuffer.array(), 0, ((Buffer) stagingBuffer).position());

						cachedCommand = this.command;

						cachedRequestComplete = command.getRequestComplete();
						cachedInterruptsEnabled = command.getInterruptsEnabled();
						cachedProcessedResponse = command.getProcessedResponse();

						tdsWritterCached = true;

						if (sourceResultSet.isForwardOnly()) {
							sourceResultSet.setFetchSize(1);
						}
					}
				}
			}

			Map<Integer, SQLServerMetaData> columnMetadata = value.getColumnMetadata();
			Iterator<Entry<Integer, SQLServerMetaData>> columnsIterator;

			while (value.next()) {

				// restore command and TDS header, which have been overwritten by value.next()
				if (tdsWritterCached) {
					command = cachedCommand;

					((Buffer) stagingBuffer).clear();
					((Buffer) logBuffer).clear();
					writeBytes(cachedTVPHeaders.array(), 0, ((Buffer) cachedTVPHeaders).position());
				}

				Object[] rowData = value.getRowData();

				// ROW
				writeByte((byte) TDS.TVP_ROW);
				columnsIterator = columnMetadata.entrySet().iterator();
				int currentColumn = 0;
				while (columnsIterator.hasNext()) {
					Map.Entry<Integer, SQLServerMetaData> columnPair = columnsIterator.next();

					// If useServerDefault is set, client MUST NOT emit TvpColumnData for the
					// associated column
					if (columnPair.getValue().useServerDefault) {
						currentColumn++;
						continue;
					}

					JDBCType jdbcType = JDBCType.of(columnPair.getValue().javaSqlType);
					String currentColumnStringValue = null;

					Object currentObject = null;
					if (null != rowData) {
						// if rowData has value for the current column, retrieve it. If not, current
						// column will stay
						// null.
						if (rowData.length > currentColumn) {
							currentObject = rowData[currentColumn];
							if (null != currentObject) {
								currentColumnStringValue = String.valueOf(currentObject);
							}
						}
					}
					writeInternalTVPRowValues(jdbcType, currentColumnStringValue, currentObject, columnPair, false);
					currentColumn++;
				}

				// send this row, read its response (throw exception in case of errors) and
				// reset command status
				if (tdsWritterCached) {
					// TVP_END_TOKEN
					writeByte((byte) 0x00);

					writePacket(TDS.STATUS_BIT_EOM);

					TDSReader tdsReader = tdsChannel.getReader(command);
					int tokenType = tdsReader.peekTokenType();

					if (TDS.TDS_ERR == tokenType) {
						StreamError databaseError = new StreamError();
						databaseError.setFromTDS(tdsReader);

						SQLServerException.makeFromDatabaseError(con, null, databaseError.getMessage(), databaseError,
								false);
					}

					command.setInterruptsEnabled(true);
					command.setRequestComplete(false);
				}
			}
		}

		// reset command status which have been overwritten
		if (tdsWritterCached) {
			command.setRequestComplete(cachedRequestComplete);
			command.setInterruptsEnabled(cachedInterruptsEnabled);
			command.setProcessedResponse(cachedProcessedResponse);
		} else {
			// TVP_END_TOKEN
			writeByte((byte) 0x00);
		}
	}

	private void writeInternalTVPRowValues(JDBCType jdbcType, String currentColumnStringValue, Object currentObject,
			Map.Entry<Integer, SQLServerMetaData> columnPair, boolean isSqlVariant) throws SQLServerException {
		boolean isShortValue, isNull;
		int dataLength;
		switch (jdbcType) {
		case BIGINT:
			if (null == currentColumnStringValue)
				writeByte((byte) 0);
			else {
				if (isSqlVariant) {
					writeTVPSqlVariantHeader(10, TDSType.INT8.byteValue(), (byte) 0);
				} else {
					writeByte((byte) 8);
				}
				writeLong(Long.valueOf(currentColumnStringValue).longValue());
			}
			break;

		case BIT:
			if (null == currentColumnStringValue)
				writeByte((byte) 0);
			else {
				if (isSqlVariant)
					writeTVPSqlVariantHeader(3, TDSType.BIT1.byteValue(), (byte) 0);
				else
					writeByte((byte) 1);
				writeByte((byte) (Boolean.valueOf(currentColumnStringValue).booleanValue() ? 1 : 0));
			}
			break;

		case INTEGER:
			if (null == currentColumnStringValue)
				writeByte((byte) 0);
			else {
				if (!isSqlVariant)
					writeByte((byte) 4);
				else
					writeTVPSqlVariantHeader(6, TDSType.INT4.byteValue(), (byte) 0);
				writeInt(Integer.valueOf(currentColumnStringValue).intValue());
			}
			break;

		case SMALLINT:
		case TINYINT:
			if (null == currentColumnStringValue)
				writeByte((byte) 0);
			else {
				if (isSqlVariant) {
					writeTVPSqlVariantHeader(6, TDSType.INT4.byteValue(), (byte) 0);
					writeInt(Integer.valueOf(currentColumnStringValue));
				} else {
					writeByte((byte) 2); // length of datatype
					writeShort(Short.valueOf(currentColumnStringValue).shortValue());
				}
			}
			break;

		case DECIMAL:
		case NUMERIC:
			if (null == currentColumnStringValue)
				writeByte((byte) 0);
			else {
				if (isSqlVariant) {
					writeTVPSqlVariantHeader(21, TDSType.DECIMALN.byteValue(), (byte) 2);
					writeByte((byte) 38); // scale (byte)variantType.getScale()
					writeByte((byte) 4); // scale (byte)variantType.getScale()
				} else {
					writeByte((byte) TDSWriter.BIGDECIMAL_MAX_LENGTH); // maximum length
				}
				BigDecimal bdValue = new BigDecimal(currentColumnStringValue);

				/*
				 * setScale of all BigDecimal value based on metadata as scale is not sent
				 * seperately for individual value. Use the rounding used in Server. Say, for
				 * BigDecimal("0.1"), if scale in metdadata is 0, then ArithmeticException would
				 * be thrown if RoundingMode is not set
				 */
				bdValue = bdValue.setScale(columnPair.getValue().scale, RoundingMode.HALF_UP);

				byte[] valueBytes = DDC.convertBigDecimalToBytes(bdValue, bdValue.scale());

				// 1-byte for sign and 16-byte for integer
				byte[] byteValue = new byte[17];

				// removing the precision and scale information from the valueBytes array
				System.arraycopy(valueBytes, 2, byteValue, 0, valueBytes.length - 2);
				writeBytes(byteValue);
			}
			break;

		case DOUBLE:
			if (null == currentColumnStringValue)
				writeByte((byte) 0); // len of data bytes
			else {
				if (isSqlVariant) {
					writeTVPSqlVariantHeader(10, TDSType.FLOAT8.byteValue(), (byte) 0);
					writeDouble(Double.valueOf(currentColumnStringValue));
					break;
				}
				writeByte((byte) 8); // len of data bytes
				long bits = Double.doubleToLongBits(Double.valueOf(currentColumnStringValue).doubleValue());
				long mask = 0xFF;
				int nShift = 0;
				for (int i = 0; i < 8; i++) {
					writeByte((byte) ((bits & mask) >> nShift));
					nShift += 8;
					mask = mask << 8;
				}
			}
			break;

		case FLOAT:
		case REAL:
			if (null == currentColumnStringValue)
				writeByte((byte) 0);
			else {
				if (isSqlVariant) {
					writeTVPSqlVariantHeader(6, TDSType.FLOAT4.byteValue(), (byte) 0);
					writeInt(Float.floatToRawIntBits(Float.valueOf(currentColumnStringValue).floatValue()));
				} else {
					writeByte((byte) 4);
					writeInt(Float.floatToRawIntBits(Float.valueOf(currentColumnStringValue).floatValue()));
				}
			}
			break;

		case DATE:
		case TIME:
		case TIMESTAMP:
		case DATETIMEOFFSET:
		case DATETIME:
		case SMALLDATETIME:
		case TIMESTAMP_WITH_TIMEZONE:
		case TIME_WITH_TIMEZONE:
		case CHAR:
		case VARCHAR:
		case NCHAR:
		case NVARCHAR:
		case LONGVARCHAR:
		case LONGNVARCHAR:
		case SQLXML:
			isShortValue = (2L * columnPair.getValue().precision) <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
			isNull = (null == currentColumnStringValue);
			dataLength = isNull ? 0 : currentColumnStringValue.length() * 2;
			if (!isShortValue) {
				// check null
				if (isNull) {
					// Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
					writeLong(0xFFFFFFFFFFFFFFFFL);
				} else if (isSqlVariant) {
					// for now we send as bigger type, but is sendStringParameterAsUnicoe is set to
					// false we can't
					// send nvarchar
					// since we are writing as nvarchar we need to write as tdstype.bigvarchar value
					// because if we
					// want to supprot varchar(8000) it becomes as nvarchar, 8000*2 therefore we
					// should send as
					// longvarchar,
					// but we cannot send more than 8000 cause sql_variant datatype in sql server
					// does not support
					// it.
					// then throw exception if user is sending more than that
					if (dataLength > 2 * DataTypes.SHORT_VARTYPE_MAX_BYTES) {
						MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidStringValue"));
						throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
					}
					int length = currentColumnStringValue.length();
					writeTVPSqlVariantHeader(9 + length, TDSType.BIGVARCHAR.byteValue(), (byte) 0x07);
					SQLCollation col = con.getDatabaseCollation();
					// write collation for sql variant
					writeInt(col.getCollationInfo());
					writeByte((byte) col.getCollationSortID());
					writeShort((short) (length));
					writeBytes(currentColumnStringValue.getBytes());
					break;
				}

				else if (DataTypes.UNKNOWN_STREAM_LENGTH == dataLength)
					// Append v*max length.
					// UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
					writeLong(0xFFFFFFFFFFFFFFFEL);
				else
					// For v*max types with known length, length is <totallength8><chunklength4>
					writeLong(dataLength);
				if (!isNull) {
					if (dataLength > 0) {
						writeInt(dataLength);
						writeString(currentColumnStringValue);
					}
					// Send the terminator PLP chunk.
					writeInt(0);
				}
			} else {
				if (isNull)
					writeShort((short) -1); // actual len
				else {
					if (isSqlVariant) {
						// for now we send as bigger type, but is sendStringParameterAsUnicoe is set to
						// false we
						// can't send nvarchar
						// check for this
						int length = currentColumnStringValue.length() * 2;
						writeTVPSqlVariantHeader(9 + length, TDSType.NVARCHAR.byteValue(), (byte) 7);
						SQLCollation col = con.getDatabaseCollation();
						// write collation for sql variant
						writeInt(col.getCollationInfo());
						writeByte((byte) col.getCollationSortID());
						int stringLength = currentColumnStringValue.length();
						byte[] typevarlen = new byte[2];
						typevarlen[0] = (byte) (2 * stringLength & 0xFF);
						typevarlen[1] = (byte) ((2 * stringLength >> 8) & 0xFF);
						writeBytes(typevarlen);
						writeString(currentColumnStringValue);
						break;
					} else {
						writeShort((short) dataLength);
						writeString(currentColumnStringValue);
					}
				}
			}
			break;

		case BINARY:
		case VARBINARY:
		case LONGVARBINARY:
			// Handle conversions as done in other types.
			isShortValue = columnPair.getValue().precision <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
			isNull = (null == currentObject);
			if (currentObject instanceof String)
				dataLength = isNull ? 0 : (ParameterUtils.HexToBin(currentObject.toString())).length;
			else
				dataLength = isNull ? 0 : ((byte[]) currentObject).length;
			if (!isShortValue) {
				// check null
				if (isNull)
					// Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
					writeLong(0xFFFFFFFFFFFFFFFFL);
				else if (DataTypes.UNKNOWN_STREAM_LENGTH == dataLength)
					// Append v*max length.
					// UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
					writeLong(0xFFFFFFFFFFFFFFFEL);
				else
					// For v*max types with known length, length is <totallength8><chunklength4>
					writeLong(dataLength);
				if (!isNull) {
					if (dataLength > 0) {
						writeInt(dataLength);
						if (currentObject instanceof String)
							writeBytes(ParameterUtils.HexToBin(currentObject.toString()));
						else
							writeBytes((byte[]) currentObject);
					}
					// Send the terminator PLP chunk.
					writeInt(0);
				}
			} else {
				if (isNull)
					writeShort((short) -1); // actual len
				else {
					writeShort((short) dataLength);
					if (currentObject instanceof String)
						writeBytes(ParameterUtils.HexToBin(currentObject.toString()));
					else
						writeBytes((byte[]) currentObject);
				}
			}
			break;
		case SQL_VARIANT:
			boolean isShiloh = (8 >= con.getServerMajorVersion());
			if (isShiloh) {
				MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_SQLVariantSupport"));
				throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
			}
			JDBCType internalJDBCType;
			JavaType javaType = JavaType.of(currentObject);
			internalJDBCType = javaType.getJDBCType(SSType.UNKNOWN, jdbcType);
			writeInternalTVPRowValues(internalJDBCType, currentColumnStringValue, currentObject, columnPair, true);
			break;
		default:
			assert false : "Unexpected JDBC type " + jdbcType.toString();
		}
	}

	/**
	 * writes Header for sql_variant for TVP
	 * 
	 * @param length
	 * @param tdsType
	 * @param probBytes
	 * @throws SQLServerException
	 */
	private void writeTVPSqlVariantHeader(int length, byte tdsType, byte probBytes) throws SQLServerException {
		writeInt(length);
		writeByte(tdsType);
		writeByte(probBytes);
	}

	void writeTVPColumnMetaData(TVP value) throws SQLServerException {
		boolean isShortValue;

		// TVP_COLMETADATA
		writeShort((short) value.getTVPColumnCount());

		Map<Integer, SQLServerMetaData> columnMetadata = value.getColumnMetadata();
		/*
		 * TypeColumnMetaData = UserType Flags TYPE_INFO ColName ;
		 */

		for (Entry<Integer, SQLServerMetaData> pair : columnMetadata.entrySet()) {
			JDBCType jdbcType = JDBCType.of(pair.getValue().javaSqlType);
			boolean useServerDefault = pair.getValue().useServerDefault;
			// ULONG ; UserType of column
			// The value will be 0x0000 with the exceptions of TIMESTAMP (0x0050) and alias
			// types (greater than 0x00FF).
			writeInt(0);
			/*
			 * Flags = fNullable ; Column is nullable - %x01 fCaseSen -- Ignored ;
			 * usUpdateable -- Ignored ; fIdentity ; Column is identity column - %x10
			 * fComputed ; Column is computed - %x20 usReservedODBC -- Ignored ;
			 * fFixedLenCLRType-- Ignored ; fDefault ; Column is default value - %x200
			 * usReserved -- Ignored ;
			 */

			short flags = TDS.FLAG_NULLABLE;
			if (useServerDefault) {
				flags |= TDS.FLAG_TVP_DEFAULT_COLUMN;
			}
			writeShort(flags);

			// Type info
			switch (jdbcType) {
			case BIGINT:
				writeByte(TDSType.INTN.byteValue());
				writeByte((byte) 8); // max length of datatype
				break;
			case BIT:
				writeByte(TDSType.BITN.byteValue());
				writeByte((byte) 1); // max length of datatype
				break;
			case INTEGER:
				writeByte(TDSType.INTN.byteValue());
				writeByte((byte) 4); // max length of datatype
				break;
			case SMALLINT:
			case TINYINT:
				writeByte(TDSType.INTN.byteValue());
				writeByte((byte) 2); // max length of datatype
				break;

			case DECIMAL:
			case NUMERIC:
				writeByte(TDSType.NUMERICN.byteValue());
				writeByte((byte) 0x11); // maximum length
				writeByte((byte) pair.getValue().precision);
				writeByte((byte) pair.getValue().scale);
				break;

			case DOUBLE:
				writeByte(TDSType.FLOATN.byteValue());
				writeByte((byte) 8); // max length of datatype
				break;

			case FLOAT:
			case REAL:
				writeByte(TDSType.FLOATN.byteValue());
				writeByte((byte) 4); // max length of datatype
				break;

			case DATE:
			case TIME:
			case TIMESTAMP:
			case DATETIMEOFFSET:
			case DATETIME:
			case SMALLDATETIME:
			case TIMESTAMP_WITH_TIMEZONE:
			case TIME_WITH_TIMEZONE:
			case CHAR:
			case VARCHAR:
			case NCHAR:
			case NVARCHAR:
			case LONGVARCHAR:
			case LONGNVARCHAR:
			case SQLXML:
				writeByte(TDSType.NVARCHAR.byteValue());
				isShortValue = (2L * pair.getValue().precision) <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
				// Use PLP encoding on Yukon and later with long values
				if (!isShortValue) // PLP
				{
					// Handle Yukon v*max type header here.
					writeShort((short) 0xFFFF);
					con.getDatabaseCollation().writeCollation(this);
				} else // non PLP
				{
					writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
					con.getDatabaseCollation().writeCollation(this);
				}

				break;

			case BINARY:
			case VARBINARY:
			case LONGVARBINARY:
				writeByte(TDSType.BIGVARBINARY.byteValue());
				isShortValue = pair.getValue().precision <= DataTypes.SHORT_VARTYPE_MAX_BYTES;
				// Use PLP encoding on Yukon and later with long values
				if (!isShortValue) // PLP
					// Handle Yukon v*max type header here.
					writeShort((short) 0xFFFF);
				else // non PLP
					writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
				break;
			case SQL_VARIANT:
				writeByte(TDSType.SQL_VARIANT.byteValue());
				writeInt(TDS.SQL_VARIANT_LENGTH);// write length of sql variant 8009

				break;

			default:
				assert false : "Unexpected JDBC type " + jdbcType.toString();
			}
			// Column name - must be null (from TDS - TVP_COLMETADATA)
			writeByte((byte) 0x00);

			// [TVP_ORDER_UNIQUE]
			// [TVP_COLUMN_ORDERING]
		}
	}

	void writeTvpOrderUnique(TVP value) throws SQLServerException {
		/*
		 * TVP_ORDER_UNIQUE = TVP_ORDER_UNIQUE_TOKEN (Count <Count>(ColNum
		 * OrderUniqueFlags))
		 */

		Map<Integer, SQLServerMetaData> columnMetadata = value.getColumnMetadata();
		Iterator<Entry<Integer, SQLServerMetaData>> columnsIterator = columnMetadata.entrySet().iterator();
		LinkedList<TdsOrderUnique> columnList = new LinkedList<>();

		while (columnsIterator.hasNext()) {
			byte flags = 0;
			Map.Entry<Integer, SQLServerMetaData> pair = columnsIterator.next();
			SQLServerMetaData metaData = pair.getValue();

			if (SQLServerSortOrder.Ascending == metaData.sortOrder)
				flags = TDS.TVP_ORDERASC_FLAG;
			else if (SQLServerSortOrder.Descending == metaData.sortOrder)
				flags = TDS.TVP_ORDERDESC_FLAG;
			if (metaData.isUniqueKey)
				flags |= TDS.TVP_UNIQUE_FLAG;

			// Remember this column if any flags were set
			if (0 != flags)
				columnList.add(new TdsOrderUnique(pair.getKey(), flags));
		}

		// Write flagged columns
		if (!columnList.isEmpty()) {
			writeByte((byte) TDS.TVP_ORDER_UNIQUE_TOKEN);
			writeShort((short) columnList.size());
			for (TdsOrderUnique column : columnList) {
				writeShort((short) (column.columnOrdinal + 1));
				writeByte(column.flags);
			}
		}
	}

	private class TdsOrderUnique {
		int columnOrdinal;
		byte flags;

		TdsOrderUnique(int ordinal, byte flags) {
			this.columnOrdinal = ordinal;
			this.flags = flags;
		}
	}

	void setCryptoMetaData(CryptoMetadata cryptoMetaForBulk) {
		this.cryptoMeta = cryptoMetaForBulk;
	}

	CryptoMetadata getCryptoMetaData() {
		return cryptoMeta;
	}

	void writeEncryptedRPCByteArray(byte bValue[]) throws SQLServerException {
		boolean bValueNull = (bValue == null);
		long nValueLen = bValueNull ? 0 : bValue.length;
		boolean isShortValue = (nValueLen <= DataTypes.SHORT_VARTYPE_MAX_BYTES);

		boolean isPLP = (!isShortValue) && (nValueLen <= DataTypes.MAX_VARTYPE_MAX_BYTES);

		// Handle Shiloh types here.
		if (isShortValue) {
			writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
		} else if (isPLP) {
			writeShort((short) DataTypes.SQL_USHORTVARMAXLEN);
		} else {
			writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
		}

		// Data and length
		if (bValueNull) {
			writeShort((short) -1); // actual len
		} else {
			if (isShortValue) {
				writeShort((short) nValueLen); // actual len
			} else if (isPLP) {
				writeLong(nValueLen); // actual length
			} else {
				writeInt((int) nValueLen); // actual len
			}

			// If length is zero, we're done.
			if (0 != nValueLen) {
				if (isPLP) {
					writeInt((int) nValueLen);
				}
				writeBytes(bValue);
			}

			if (isPLP) {
				writeInt(0); // PLP_TERMINATOR, 0x00000000
			}
		}
	}

	void writeEncryptedRPCPLP() throws SQLServerException {
		writeShort((short) DataTypes.SQL_USHORTVARMAXLEN);
		writeLong((long) 0); // actual length
		writeInt(0); // PLP_TERMINATOR, 0x00000000
	}

	void writeCryptoMetaData() throws SQLServerException {
		writeByte(cryptoMeta.cipherAlgorithmId);
		writeByte(cryptoMeta.encryptionType.getValue());
		writeInt(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).databaseId);
		writeInt(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).cekId);
		writeInt(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).cekVersion);
		writeBytes(cryptoMeta.cekTableEntry.getColumnEncryptionKeyValues().get(0).cekMdVersion);
		writeByte(cryptoMeta.normalizationRuleVersion);
	}

	void writeRPCByteArray(String sName, byte bValue[], boolean bOut, JDBCType jdbcType, SQLCollation collation)
			throws SQLServerException {
		boolean bValueNull = (bValue == null);
		int nValueLen = bValueNull ? 0 : bValue.length;
		boolean isShortValue = (nValueLen <= DataTypes.SHORT_VARTYPE_MAX_BYTES);

		// Use PLP encoding on Yukon and later with long values and OUT parameters
		boolean usePLP = (!isShortValue || bOut);

		TDSType tdsType;

		if (null != cryptoMeta) {
			// send encrypted data as BIGVARBINARY
			tdsType = (isShortValue || usePLP) ? TDSType.BIGVARBINARY : TDSType.IMAGE;
			collation = null;
		} else
			switch (jdbcType) {
			case BINARY:
			case VARBINARY:
			case LONGVARBINARY:
			case BLOB:
			default:
				tdsType = (isShortValue || usePLP) ? TDSType.BIGVARBINARY : TDSType.IMAGE;
				collation = null;
				break;

			case CHAR:
			case VARCHAR:
			case LONGVARCHAR:
			case CLOB:
				tdsType = (isShortValue || usePLP) ? TDSType.BIGVARCHAR : TDSType.TEXT;
				if (null == collation)
					collation = con.getDatabaseCollation();
				break;

			case NCHAR:
			case NVARCHAR:
			case LONGNVARCHAR:
			case NCLOB:
				tdsType = (isShortValue || usePLP) ? TDSType.NVARCHAR : TDSType.NTEXT;
				if (null == collation)
					collation = con.getDatabaseCollation();
				break;
			}

		writeRPCNameValType(sName, bOut, tdsType);

		if (usePLP) {
			// Handle Yukon v*max type header here.
			writeVMaxHeader(nValueLen, bValueNull, collation);

			// Send the data.
			if (!bValueNull) {
				if (nValueLen > 0) {
					writeInt(nValueLen);
					writeBytes(bValue);
				}

				// Send the terminator PLP chunk.
				writeInt(0);
			}
		} else // non-PLP type
		{
			// Handle Shiloh types here.
			if (isShortValue) {
				writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
			} else {
				writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
			}

			if (null != collation)
				collation.writeCollation(this);

			// Data and length
			if (bValueNull) {
				writeShort((short) -1); // actual len
			} else {
				if (isShortValue)
					writeShort((short) nValueLen); // actual len
				else
					writeInt(nValueLen); // actual len

				// If length is zero, we're done.
				if (0 != nValueLen)
					writeBytes(bValue);
			}
		}
	}

	/**
	 * Append a timestamp in RPC transmission format as a SQL Server DATETIME data
	 * type
	 * 
	 * @param sName          the optional parameter name
	 * @param cal            Pure Gregorian calendar containing the timestamp,
	 *                       including its associated time zone
	 * @param subSecondNanos the sub-second nanoseconds (0 - 999,999,999)
	 * @param bOut           boolean true if the data value is being registered as
	 *                       an ouput parameter
	 *
	 */
	void writeRPCDateTime(String sName, GregorianCalendar cal, int subSecondNanos, boolean bOut)
			throws SQLServerException {
		assert (subSecondNanos >= 0) && (subSecondNanos < Nanos.PER_SECOND) : "Invalid subNanoSeconds value: "
				+ subSecondNanos;
		assert (cal != null) || (subSecondNanos == 0) : "Invalid subNanoSeconds value when calendar is null: "
				+ subSecondNanos;

		writeRPCNameValType(sName, bOut, TDSType.DATETIMEN);
		writeByte((byte) 8); // max length of datatype

		if (null == cal) {
			writeByte((byte) 0); // len of data bytes
			return;
		}

		writeByte((byte) 8); // len of data bytes

		// We need to extract the Calendar's current date & time in terms
		// of the number of days since the SQL Base Date (1/1/1900) plus
		// the number of milliseconds since midnight in the current day.
		//
		// We cannot rely on any pre-calculated value for the number of
		// milliseconds in a day or the number of milliseconds since the
		// base date to do this because days with DST changes are shorter
		// or longer than "normal" days.
		//
		// ASSUMPTION: We assume we are dealing with a GregorianCalendar here.
		// If not, we have no basis in which to compare dates. E.g. if we
		// are dealing with a Chinese Calendar implementation which does not
		// use the same value for Calendar.YEAR as the GregorianCalendar,
		// we cannot meaningfully compute a value relative to 1/1/1900.

		// First, figure out how many days there have been since the SQL Base Date.
		// These are based on SQL Server algorithms
		int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR),
				TDS.BASE_YEAR_1900);

		// Next, figure out the number of milliseconds since midnight of the current
		// day.
		int millisSinceMidnight = (subSecondNanos + Nanos.PER_MILLISECOND / 2) / Nanos.PER_MILLISECOND + // Millis into
																											// the
																											// current
																											// second
				1000 * cal.get(Calendar.SECOND) + // Seconds into the current minute
				60 * 1000 * cal.get(Calendar.MINUTE) + // Minutes into the current hour
				60 * 60 * 1000 * cal.get(Calendar.HOUR_OF_DAY); // Hours into the current day

		// The last millisecond of the current day is always rounded to the first
		// millisecond
		// of the next day because DATETIME is only accurate to 1/300th of a second.
		if (millisSinceMidnight >= 1000 * 60 * 60 * 24 - 1) {
			++daysSinceSQLBaseDate;
			millisSinceMidnight = 0;
		}

		// Last-ditch verification that the value is in the valid range for the
		// DATETIMEN TDS data type (1/1/1753 to 12/31/9999). If it's not, then
		// throw an exception now so that statement execution is safely canceled.
		// Attempting to put an invalid value on the wire would result in a TDS
		// exception, which would close the connection.
		// These are based on SQL Server algorithms
		if (daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1753, 1, TDS.BASE_YEAR_1900)
				|| daysSinceSQLBaseDate >= DDC.daysSinceBaseDate(10000, 1, TDS.BASE_YEAR_1900)) {
			MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
			Object[] msgArgs = { SSType.DATETIME };
			throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
					DriverError.NOT_SET, null);
		}

		// And put it all on the wire...

		// Number of days since the SQL Server Base Date (January 1, 1900)
		writeInt(daysSinceSQLBaseDate);

		// Milliseconds since midnight (at a resolution of three hundredths of a second)
		writeInt((3 * millisSinceMidnight + 5) / 10);
	}

	void writeRPCTime(String sName, GregorianCalendar localCalendar, int subSecondNanos, int scale, boolean bOut)
			throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.TIMEN);
		writeByte((byte) scale);

		if (null == localCalendar) {
			writeByte((byte) 0);
			return;
		}

		writeByte((byte) TDS.timeValueLength(scale));
		writeScaledTemporal(localCalendar, subSecondNanos, scale, SSType.TIME);
	}

	void writeRPCDate(String sName, GregorianCalendar localCalendar, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.DATEN);
		if (null == localCalendar) {
			writeByte((byte) 0);
			return;
		}

		writeByte((byte) TDS.DAYS_INTO_CE_LENGTH);
		writeScaledTemporal(localCalendar, 0, // subsecond nanos (none for a date value)
				0, // scale (dates are not scaled)
				SSType.DATE);
	}

	void writeEncryptedRPCTime(String sName, GregorianCalendar localCalendar, int subSecondNanos, int scale,
			boolean bOut) throws SQLServerException {
		if (con.getSendTimeAsDatetime()) {
			throw new SQLServerException(SQLServerException.getErrString("R_sendTimeAsDateTimeForAE"), null);
		}
		writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

		if (null == localCalendar)
			writeEncryptedRPCByteArray(null);
		else
			writeEncryptedRPCByteArray(
					writeEncryptedScaledTemporal(localCalendar, subSecondNanos, scale, SSType.TIME, (short) 0));

		writeByte(TDSType.TIMEN.byteValue());
		writeByte((byte) scale);
		writeCryptoMetaData();
	}

	void writeEncryptedRPCDate(String sName, GregorianCalendar localCalendar, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

		if (null == localCalendar)
			writeEncryptedRPCByteArray(null);
		else
			writeEncryptedRPCByteArray(writeEncryptedScaledTemporal(localCalendar, 0, // subsecond nanos (none for a
																						// date value)
					0, // scale (dates are not scaled)
					SSType.DATE, (short) 0));

		writeByte(TDSType.DATEN.byteValue());
		writeCryptoMetaData();
	}

	void writeEncryptedRPCDateTime(String sName, GregorianCalendar cal, int subSecondNanos, boolean bOut,
			JDBCType jdbcType) throws SQLServerException {
		assert (subSecondNanos >= 0) && (subSecondNanos < Nanos.PER_SECOND) : "Invalid subNanoSeconds value: "
				+ subSecondNanos;
		assert (cal != null) || (subSecondNanos == 0) : "Invalid subNanoSeconds value when calendar is null: "
				+ subSecondNanos;

		writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

		if (null == cal)
			writeEncryptedRPCByteArray(null);
		else
			writeEncryptedRPCByteArray(getEncryptedDateTimeAsBytes(cal, subSecondNanos, jdbcType));

		if (JDBCType.SMALLDATETIME == jdbcType) {
			writeByte(TDSType.DATETIMEN.byteValue());
			writeByte((byte) 4);
		} else {
			writeByte(TDSType.DATETIMEN.byteValue());
			writeByte((byte) 8);
		}
		writeCryptoMetaData();
	}

	// getEncryptedDateTimeAsBytes is called if jdbcType/ssType is SMALLDATETIME or
	// DATETIME
	byte[] getEncryptedDateTimeAsBytes(GregorianCalendar cal, int subSecondNanos, JDBCType jdbcType)
			throws SQLServerException {
		int daysSinceSQLBaseDate = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR),
				TDS.BASE_YEAR_1900);

		// Next, figure out the number of milliseconds since midnight of the current
		// day.
		int millisSinceMidnight = (subSecondNanos + Nanos.PER_MILLISECOND / 2) / Nanos.PER_MILLISECOND + // Millis into
																											// the
																											// current
																											// second
				1000 * cal.get(Calendar.SECOND) + // Seconds into the current minute
				60 * 1000 * cal.get(Calendar.MINUTE) + // Minutes into the current hour
				60 * 60 * 1000 * cal.get(Calendar.HOUR_OF_DAY); // Hours into the current day

		// The last millisecond of the current day is always rounded to the first
		// millisecond
		// of the next day because DATETIME is only accurate to 1/300th of a second.
		if (millisSinceMidnight >= 1000 * 60 * 60 * 24 - 1) {
			++daysSinceSQLBaseDate;
			millisSinceMidnight = 0;
		}

		if (JDBCType.SMALLDATETIME == jdbcType) {

			int secondsSinceMidnight = (millisSinceMidnight / 1000);
			int minutesSinceMidnight = (secondsSinceMidnight / 60);

			// Values that are 29.998 seconds or less are rounded down to the nearest minute
			minutesSinceMidnight = ((secondsSinceMidnight % 60) > 29.998) ? minutesSinceMidnight + 1
					: minutesSinceMidnight;

			// minutesSinceMidnight for (23:59:30)
			int maxMinutesSinceMidnight_SmallDateTime = 1440;
			// Verification for smalldatetime to be within valid range of (1900.01.01) to
			// (2079.06.06)
			// smalldatetime for unencrypted does not allow insertion of 2079.06.06 23:59:59
			// and it is rounded up
			// to 2079.06.07 00:00:00, therefore, we are checking minutesSinceMidnight for
			// that condition. If it's not
			// within valid range, then
			// throw an exception now so that statement execution is safely canceled.
			// 157 is the calculated day of year from 06-06 , 1440 is minutesince midnight
			// for (23:59:30)
			if ((daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1900, 1, TDS.BASE_YEAR_1900)
					|| daysSinceSQLBaseDate > DDC.daysSinceBaseDate(2079, 157, TDS.BASE_YEAR_1900))
					|| (daysSinceSQLBaseDate == DDC.daysSinceBaseDate(2079, 157, TDS.BASE_YEAR_1900)
							&& minutesSinceMidnight >= maxMinutesSinceMidnight_SmallDateTime)) {
				MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
				Object[] msgArgs = { SSType.SMALLDATETIME };
				throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
						DriverError.NOT_SET, null);
			}

			ByteBuffer days = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
			days.putShort((short) daysSinceSQLBaseDate);
			ByteBuffer seconds = ByteBuffer.allocate(2).order(ByteOrder.LITTLE_ENDIAN);
			seconds.putShort((short) minutesSinceMidnight);

			byte[] value = new byte[4];
			System.arraycopy(days.array(), 0, value, 0, 2);
			System.arraycopy(seconds.array(), 0, value, 2, 2);
			return SQLServerSecurityUtility.encryptWithKey(value, cryptoMeta, con);
		} else if (JDBCType.DATETIME == jdbcType) {
			// Last-ditch verification that the value is in the valid range for the
			// DATETIMEN TDS data type (1/1/1753 to 12/31/9999). If it's not, then
			// throw an exception now so that statement execution is safely canceled.
			// Attempting to put an invalid value on the wire would result in a TDS
			// exception, which would close the connection.
			// These are based on SQL Server algorithms
			// And put it all on the wire...
			if (daysSinceSQLBaseDate < DDC.daysSinceBaseDate(1753, 1, TDS.BASE_YEAR_1900)
					|| daysSinceSQLBaseDate >= DDC.daysSinceBaseDate(10000, 1, TDS.BASE_YEAR_1900)) {
				MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
				Object[] msgArgs = { SSType.DATETIME };
				throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
						DriverError.NOT_SET, null);
			}

			// Number of days since the SQL Server Base Date (January 1, 1900)
			ByteBuffer days = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
			days.putInt(daysSinceSQLBaseDate);
			ByteBuffer seconds = ByteBuffer.allocate(4).order(ByteOrder.LITTLE_ENDIAN);
			seconds.putInt((3 * millisSinceMidnight + 5) / 10);

			byte[] value = new byte[8];
			System.arraycopy(days.array(), 0, value, 0, 4);
			System.arraycopy(seconds.array(), 0, value, 4, 4);
			return SQLServerSecurityUtility.encryptWithKey(value, cryptoMeta, con);
		}

		assert false : "Unexpected JDBCType type " + jdbcType;
		return null;
	}

	void writeEncryptedRPCDateTime2(String sName, GregorianCalendar localCalendar, int subSecondNanos, int scale,
			boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

		if (null == localCalendar)
			writeEncryptedRPCByteArray(null);
		else
			writeEncryptedRPCByteArray(
					writeEncryptedScaledTemporal(localCalendar, subSecondNanos, scale, SSType.DATETIME2, (short) 0));

		writeByte(TDSType.DATETIME2N.byteValue());
		writeByte((byte) (scale));
		writeCryptoMetaData();
	}

	void writeEncryptedRPCDateTimeOffset(String sName, GregorianCalendar utcCalendar, int minutesOffset,
			int subSecondNanos, int scale, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.BIGVARBINARY);

		if (null == utcCalendar)
			writeEncryptedRPCByteArray(null);
		else {
			assert 0 == utcCalendar.get(Calendar.ZONE_OFFSET);
			writeEncryptedRPCByteArray(writeEncryptedScaledTemporal(utcCalendar, subSecondNanos, scale,
					SSType.DATETIMEOFFSET, (short) minutesOffset));
		}

		writeByte(TDSType.DATETIMEOFFSETN.byteValue());
		writeByte((byte) (scale));
		writeCryptoMetaData();

	}

	void writeRPCDateTime2(String sName, GregorianCalendar localCalendar, int subSecondNanos, int scale, boolean bOut)
			throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.DATETIME2N);
		writeByte((byte) scale);

		if (null == localCalendar) {
			writeByte((byte) 0);
			return;
		}

		writeByte((byte) TDS.datetime2ValueLength(scale));
		writeScaledTemporal(localCalendar, subSecondNanos, scale, SSType.DATETIME2);
	}

	void writeRPCDateTimeOffset(String sName, GregorianCalendar utcCalendar, int minutesOffset, int subSecondNanos,
			int scale, boolean bOut) throws SQLServerException {
		writeRPCNameValType(sName, bOut, TDSType.DATETIMEOFFSETN);
		writeByte((byte) scale);

		if (null == utcCalendar) {
			writeByte((byte) 0);
			return;
		}

		assert 0 == utcCalendar.get(Calendar.ZONE_OFFSET);

		writeByte((byte) TDS.datetimeoffsetValueLength(scale));
		writeScaledTemporal(utcCalendar, subSecondNanos, scale, SSType.DATETIMEOFFSET);

		writeShort((short) minutesOffset);
	}

	/**
	 * Returns subSecondNanos rounded to the maximum precision supported. The
	 * maximum fractional scale is MAX_FRACTIONAL_SECONDS_SCALE(7). Eg1: if you pass
	 * 456,790,123 the function would return 456,790,100 Eg2: if you pass
	 * 456,790,150 the function would return 456,790,200 Eg3: if you pass
	 * 999,999,951 the function would return 1,000,000,000 This is done to ensure
	 * that we have consistent rounding behaviour in setters and getters. Bug
	 * #507919
	 */
	private int getRoundedSubSecondNanos(int subSecondNanos) {
		int roundedNanos = ((subSecondNanos + (Nanos.PER_MAX_SCALE_INTERVAL / 2)) / Nanos.PER_MAX_SCALE_INTERVAL)
				* Nanos.PER_MAX_SCALE_INTERVAL;
		return roundedNanos;
	}

	/**
	 * Writes to the TDS channel a temporal value as an instance instance of one of
	 * the scaled temporal SQL types: DATE, TIME, DATETIME2, or DATETIMEOFFSET.
	 *
	 * @param cal            Calendar representing the value to write, except for
	 *                       any sub-second nanoseconds
	 * @param subSecondNanos the sub-second nanoseconds (0 - 999,999,999)
	 * @param scale          the scale (in digits: 0 - 7) to use for the sub-second
	 *                       nanos component
	 * @param ssType         the SQL Server data type (DATE, TIME, DATETIME2, or
	 *                       DATETIMEOFFSET)
	 *
	 * @throws SQLServerException if an I/O error occurs or if the value is not in
	 *                            the valid range
	 */
	private void writeScaledTemporal(GregorianCalendar cal, int subSecondNanos, int scale, SSType ssType)
			throws SQLServerException {

		assert con.isKatmaiOrLater();

		assert SSType.DATE == ssType || SSType.TIME == ssType || SSType.DATETIME2 == ssType
				|| SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: " + ssType;

		// First, for types with a time component, write the scaled nanos since midnight
		if (SSType.TIME == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
			assert subSecondNanos >= 0;
			assert subSecondNanos < Nanos.PER_SECOND;
			assert scale >= 0;
			assert scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

			int secondsSinceMidnight = cal.get(Calendar.SECOND) + 60 * cal.get(Calendar.MINUTE)
					+ 60 * 60 * cal.get(Calendar.HOUR_OF_DAY);

			// Scale nanos since midnight to the desired scale, rounding the value as
			// necessary
			long divisor = Nanos.PER_MAX_SCALE_INTERVAL * (long) Math.pow(10, TDS.MAX_FRACTIONAL_SECONDS_SCALE - scale);

			// The scaledNanos variable represents the fractional seconds of the value at
			// the scale
			// indicated by the scale variable. So, for example, scaledNanos = 3 means 300
			// nanoseconds
			// at scale TDS.MAX_FRACTIONAL_SECONDS_SCALE, but 3000 nanoseconds at
			// TDS.MAX_FRACTIONAL_SECONDS_SCALE - 1
			long scaledNanos = ((long) Nanos.PER_SECOND * secondsSinceMidnight
					+ getRoundedSubSecondNanos(subSecondNanos) + divisor / 2) / divisor;

			// SQL Server rounding behavior indicates that it always rounds up unless
			// we are at the max value of the type(NOT every day), in which case it
			// truncates.
			// Side effect on Calendar date:
			// If rounding nanos to the specified scale rolls the value to the next day ...
			if (Nanos.PER_DAY / divisor == scaledNanos) {

				// If the type is time, always truncate
				if (SSType.TIME == ssType) {
					--scaledNanos;
				}
				// If the type is datetime2 or datetimeoffset, truncate only if its the max
				// value supported
				else {
					assert SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: "
							+ ssType;

					// ... then bump the date, provided that the resulting date is still within
					// the valid date range.
					//
					// Extreme edge case (literally, the VERY edge...):
					// If nanos overflow rolls the date value out of range (that is, we have a value
					// a few nanoseconds later than 9999-12-31 23:59:59) then truncate the nanos
					// instead of rolling.
					//
					// This case is very likely never hit by "real world" applications, but exists
					// here as a security measure to ensure that such values don't result in a
					// connection-closing TDS exception.
					cal.add(Calendar.SECOND, 1);

					if (cal.get(Calendar.YEAR) <= 9999) {
						scaledNanos = 0;
					} else {
						cal.add(Calendar.SECOND, -1);
						--scaledNanos;
					}
				}
			}

			// Encode the scaled nanos to TDS
			int encodedLength = TDS.nanosSinceMidnightLength(scale);
			byte[] encodedBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

			writeBytes(encodedBytes);
		}

		// Second, for types with a date component, write the days into the Common Era
		if (SSType.DATE == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
			// Computation of the number of days into the Common Era assumes that
			// the DAY_OF_YEAR field reflects a pure Gregorian calendar - one that
			// uses Gregorian leap year rules across the entire range of dates.
			//
			// For the DAY_OF_YEAR field to accurately reflect pure Gregorian behavior,
			// we need to use a pure Gregorian calendar for dates that are Julian dates
			// under a standard Gregorian calendar and for (Gregorian) dates later than
			// the cutover date in the cutover year.
			if (cal.getTimeInMillis() < GregorianChange.STANDARD_CHANGE_DATE.getTime()
					|| cal.getActualMaximum(Calendar.DAY_OF_YEAR) < TDS.DAYS_PER_YEAR) {
				int year = cal.get(Calendar.YEAR);
				int month = cal.get(Calendar.MONTH);
				int date = cal.get(Calendar.DATE);

				// Set the cutover as early as possible (pure Gregorian behavior)
				cal.setGregorianChange(GregorianChange.PURE_CHANGE_DATE);

				// Initialize the date field by field (preserving the "wall calendar" value)
				cal.set(year, month, date);
			}

			int daysIntoCE = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR), 1);

			// Last-ditch verification that the value is in the valid range for the
			// DATE/DATETIME2/DATETIMEOFFSET TDS data type (1/1/0001 to 12/31/9999).
			// If it's not, then throw an exception now so that statement execution
			// is safely canceled. Attempting to put an invalid value on the wire
			// would result in a TDS exception, which would close the connection.
			if (daysIntoCE < 0 || daysIntoCE >= DDC.daysSinceBaseDate(10000, 1, 1)) {
				MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
				Object[] msgArgs = { ssType };
				throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
						DriverError.NOT_SET, null);
			}

			byte encodedBytes[] = new byte[3];
			encodedBytes[0] = (byte) ((daysIntoCE >> 0) & 0xFF);
			encodedBytes[1] = (byte) ((daysIntoCE >> 8) & 0xFF);
			encodedBytes[2] = (byte) ((daysIntoCE >> 16) & 0xFF);
			writeBytes(encodedBytes);
		}
	}

	/**
	 * Writes to the TDS channel a temporal value as an instance instance of one of
	 * the scaled temporal SQL types: DATE, TIME, DATETIME2, or DATETIMEOFFSET.
	 *
	 * @param cal            Calendar representing the value to write, except for
	 *                       any sub-second nanoseconds
	 * @param subSecondNanos the sub-second nanoseconds (0 - 999,999,999)
	 * @param scale          the scale (in digits: 0 - 7) to use for the sub-second
	 *                       nanos component
	 * @param ssType         the SQL Server data type (DATE, TIME, DATETIME2, or
	 *                       DATETIMEOFFSET)
	 * @param minutesOffset  the offset value for DATETIMEOFFSET
	 * @throws SQLServerException if an I/O error occurs or if the value is not in
	 *                            the valid range
	 */
	byte[] writeEncryptedScaledTemporal(GregorianCalendar cal, int subSecondNanos, int scale, SSType ssType,
			short minutesOffset) throws SQLServerException {
		assert con.isKatmaiOrLater();

		assert SSType.DATE == ssType || SSType.TIME == ssType || SSType.DATETIME2 == ssType
				|| SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: " + ssType;

		// store the time and minutesOffset portion of DATETIME2 and DATETIMEOFFSET to
		// be used with date portion
		byte encodedBytesForEncryption[] = null;

		int secondsSinceMidnight = 0;
		long divisor = 0;
		long scaledNanos = 0;

		// First, for types with a time component, write the scaled nanos since midnight
		if (SSType.TIME == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
			assert subSecondNanos >= 0;
			assert subSecondNanos < Nanos.PER_SECOND;
			assert scale >= 0;
			assert scale <= TDS.MAX_FRACTIONAL_SECONDS_SCALE;

			secondsSinceMidnight = cal.get(Calendar.SECOND) + 60 * cal.get(Calendar.MINUTE)
					+ 60 * 60 * cal.get(Calendar.HOUR_OF_DAY);

			// Scale nanos since midnight to the desired scale, rounding the value as
			// necessary
			divisor = Nanos.PER_MAX_SCALE_INTERVAL * (long) Math.pow(10, TDS.MAX_FRACTIONAL_SECONDS_SCALE - scale);

			// The scaledNanos variable represents the fractional seconds of the value at
			// the scale
			// indicated by the scale variable. So, for example, scaledNanos = 3 means 300
			// nanoseconds
			// at scale TDS.MAX_FRACTIONAL_SECONDS_SCALE, but 3000 nanoseconds at
			// TDS.MAX_FRACTIONAL_SECONDS_SCALE - 1
			scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight + getRoundedSubSecondNanos(subSecondNanos)
					+ divisor / 2) / divisor) * divisor / 100;

			// for encrypted time value, SQL server cannot do rounding or casting,
			// So, driver needs to cast it before encryption.
			if (SSType.TIME == ssType && 864000000000L <= scaledNanos) {
				scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight
						+ getRoundedSubSecondNanos(subSecondNanos)) / divisor) * divisor / 100;
			}

			// SQL Server rounding behavior indicates that it always rounds up unless
			// we are at the max value of the type(NOT every day), in which case it
			// truncates.
			// Side effect on Calendar date:
			// If rounding nanos to the specified scale rolls the value to the next day ...
			if (Nanos.PER_DAY / divisor == scaledNanos) {

				// If the type is time, always truncate
				if (SSType.TIME == ssType) {
					--scaledNanos;
				}
				// If the type is datetime2 or datetimeoffset, truncate only if its the max
				// value supported
				else {
					assert SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType : "Unexpected SSType: "
							+ ssType;

					// ... then bump the date, provided that the resulting date is still within
					// the valid date range.
					//
					// Extreme edge case (literally, the VERY edge...):
					// If nanos overflow rolls the date value out of range (that is, we have a value
					// a few nanoseconds later than 9999-12-31 23:59:59) then truncate the nanos
					// instead of rolling.
					//
					// This case is very likely never hit by "real world" applications, but exists
					// here as a security measure to ensure that such values don't result in a
					// connection-closing TDS exception.
					cal.add(Calendar.SECOND, 1);

					if (cal.get(Calendar.YEAR) <= 9999) {
						scaledNanos = 0;
					} else {
						cal.add(Calendar.SECOND, -1);
						--scaledNanos;
					}
				}
			}

			// Encode the scaled nanos to TDS
			int encodedLength = TDS.nanosSinceMidnightLength(TDS.MAX_FRACTIONAL_SECONDS_SCALE);
			byte[] encodedBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

			if (SSType.TIME == ssType) {
				byte[] cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytes, cryptoMeta, con);
				return cipherText;
			} else if (SSType.DATETIME2 == ssType) {
				// for DATETIME2 sends both date and time part together for encryption
				encodedBytesForEncryption = new byte[encodedLength + 3];
				System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, 0, encodedBytes.length);
			} else if (SSType.DATETIMEOFFSET == ssType) {
				// for DATETIMEOFFSET sends date, time and offset part together for encryption
				encodedBytesForEncryption = new byte[encodedLength + 5];
				System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, 0, encodedBytes.length);
			}
		}

		// Second, for types with a date component, write the days into the Common Era
		if (SSType.DATE == ssType || SSType.DATETIME2 == ssType || SSType.DATETIMEOFFSET == ssType) {
			// Computation of the number of days into the Common Era assumes that
			// the DAY_OF_YEAR field reflects a pure Gregorian calendar - one that
			// uses Gregorian leap year rules across the entire range of dates.
			//
			// For the DAY_OF_YEAR field to accurately reflect pure Gregorian behavior,
			// we need to use a pure Gregorian calendar for dates that are Julian dates
			// under a standard Gregorian calendar and for (Gregorian) dates later than
			// the cutover date in the cutover year.
			if (cal.getTimeInMillis() < GregorianChange.STANDARD_CHANGE_DATE.getTime()
					|| cal.getActualMaximum(Calendar.DAY_OF_YEAR) < TDS.DAYS_PER_YEAR) {
				int year = cal.get(Calendar.YEAR);
				int month = cal.get(Calendar.MONTH);
				int date = cal.get(Calendar.DATE);

				// Set the cutover as early as possible (pure Gregorian behavior)
				cal.setGregorianChange(GregorianChange.PURE_CHANGE_DATE);

				// Initialize the date field by field (preserving the "wall calendar" value)
				cal.set(year, month, date);
			}

			int daysIntoCE = DDC.daysSinceBaseDate(cal.get(Calendar.YEAR), cal.get(Calendar.DAY_OF_YEAR), 1);

			// Last-ditch verification that the value is in the valid range for the
			// DATE/DATETIME2/DATETIMEOFFSET TDS data type (1/1/0001 to 12/31/9999).
			// If it's not, then throw an exception now so that statement execution
			// is safely canceled. Attempting to put an invalid value on the wire
			// would result in a TDS exception, which would close the connection.
			if (daysIntoCE < 0 || daysIntoCE >= DDC.daysSinceBaseDate(10000, 1, 1)) {
				MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_valueOutOfRange"));
				Object[] msgArgs = { ssType };
				throw new SQLServerException(form.format(msgArgs), SQLState.DATA_EXCEPTION_DATETIME_FIELD_OVERFLOW,
						DriverError.NOT_SET, null);
			}

			byte encodedBytes[] = new byte[3];
			encodedBytes[0] = (byte) ((daysIntoCE >> 0) & 0xFF);
			encodedBytes[1] = (byte) ((daysIntoCE >> 8) & 0xFF);
			encodedBytes[2] = (byte) ((daysIntoCE >> 16) & 0xFF);

			byte[] cipherText;
			if (SSType.DATE == ssType) {
				cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytes, cryptoMeta, con);
			} else if (SSType.DATETIME2 == ssType) {
				// for Max value, does not round up, do casting instead.
				if (3652058 == daysIntoCE) { // 9999-12-31
					if (864000000000L == scaledNanos) { // 24:00:00 in nanoseconds
						// does not round up
						scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight
								+ getRoundedSubSecondNanos(subSecondNanos)) / divisor) * divisor / 100;

						int encodedLength = TDS.nanosSinceMidnightLength(TDS.MAX_FRACTIONAL_SECONDS_SCALE);
						byte[] encodedNanoBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

						// for DATETIME2 sends both date and time part together for encryption
						encodedBytesForEncryption = new byte[encodedLength + 3];
						System.arraycopy(encodedNanoBytes, 0, encodedBytesForEncryption, 0, encodedNanoBytes.length);
					}
				}
				// Copy the 3 byte date value
				System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, (encodedBytesForEncryption.length - 3), 3);

				cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytesForEncryption, cryptoMeta, con);
			} else {
				// for Max value, does not round up, do casting instead.
				if (3652058 == daysIntoCE) { // 9999-12-31
					if (864000000000L == scaledNanos) { // 24:00:00 in nanoseconds
						// does not round up
						scaledNanos = (((long) Nanos.PER_SECOND * secondsSinceMidnight
								+ getRoundedSubSecondNanos(subSecondNanos)) / divisor) * divisor / 100;

						int encodedLength = TDS.nanosSinceMidnightLength(TDS.MAX_FRACTIONAL_SECONDS_SCALE);
						byte[] encodedNanoBytes = scaledNanosToEncodedBytes(scaledNanos, encodedLength);

						// for DATETIMEOFFSET sends date, time and offset part together for encryption
						encodedBytesForEncryption = new byte[encodedLength + 5];
						System.arraycopy(encodedNanoBytes, 0, encodedBytesForEncryption, 0, encodedNanoBytes.length);
					}
				}

				// Copy the 3 byte date value
				System.arraycopy(encodedBytes, 0, encodedBytesForEncryption, (encodedBytesForEncryption.length - 5), 3);
				// Copy the 2 byte minutesOffset value
				System.arraycopy(
						ByteBuffer.allocate(Short.SIZE / Byte.SIZE).order(ByteOrder.LITTLE_ENDIAN)
								.putShort(minutesOffset).array(),
						0, encodedBytesForEncryption, (encodedBytesForEncryption.length - 2), 2);

				cipherText = SQLServerSecurityUtility.encryptWithKey(encodedBytesForEncryption, cryptoMeta, con);
			}
			return cipherText;
		}

		// Invalid type ssType. This condition should never happen.
		MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_unknownSSType"));
		Object[] msgArgs = { ssType };
		SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), null, true);

		return null;
	}

	private byte[] scaledNanosToEncodedBytes(long scaledNanos, int encodedLength) {
		byte encodedBytes[] = new byte[encodedLength];
		for (int i = 0; i < encodedLength; i++)
			encodedBytes[i] = (byte) ((scaledNanos >> (8 * i)) & 0xFF);
		return encodedBytes;
	}

	/**
	 * Append the data in a stream in RPC transmission format.
	 * 
	 * @param sName        the optional parameter name
	 * @param stream       is the stream
	 * @param streamLength length of the stream (may be unknown)
	 * @param bOut         boolean true if the data value is being registered as an
	 *                     ouput parameter
	 * @param jdbcType     The JDBC type used to determine whether the value is
	 *                     textual or non-textual.
	 * @param collation    The SQL collation associated with the value. Null for
	 *                     non-textual SQL Server types.
	 * @throws SQLServerException
	 */
	void writeRPCInputStream(String sName, InputStream stream, long streamLength, boolean bOut, JDBCType jdbcType,
			SQLCollation collation) throws SQLServerException {
		assert null != stream;
		assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength >= 0;

		// Send long values and values with unknown length
		// using PLP chunking on Yukon and later.
		boolean usePLP = (DataTypes.UNKNOWN_STREAM_LENGTH == streamLength
				|| streamLength > DataTypes.SHORT_VARTYPE_MAX_BYTES);
		if (usePLP) {
			assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength <= DataTypes.MAX_VARTYPE_MAX_BYTES;

			writeRPCNameValType(sName, bOut, jdbcType.isTextual() ? TDSType.BIGVARCHAR : TDSType.BIGVARBINARY);

			// Handle Yukon v*max type header here.
			writeVMaxHeader(streamLength, false, jdbcType.isTextual() ? collation : null);
		}

		// Send non-PLP in all other cases
		else {
			// If the length of the InputStream is unknown then we need to buffer the entire
			// stream
			// in memory so that we can determine its length and send that length to the
			// server
			// before the stream data itself.
			if (DataTypes.UNKNOWN_STREAM_LENGTH == streamLength) {
				// Create ByteArrayOutputStream with initial buffer size of 8K to handle typical
				// binary field sizes more efficiently. Note we can grow beyond 8000 bytes.
				ByteArrayOutputStream baos = new ByteArrayOutputStream(8000);
				streamLength = 0L;

				// Since Shiloh is limited to 64K TDS packets, that's a good upper bound on the
				// maximum
				// length of InputStream we should try to handle before throwing an exception.
				long maxStreamLength = 65535L * con.getTDSPacketSize();

				try {
					byte buff[] = new byte[8000];
					int bytesRead;

					while (streamLength < maxStreamLength && -1 != (bytesRead = stream.read(buff, 0, buff.length))) {
						baos.write(buff);
						streamLength += bytesRead;
					}
				} catch (IOException e) {
					throw new SQLServerException(e.getMessage(), SQLState.DATA_EXCEPTION_NOT_SPECIFIC,
							DriverError.NOT_SET, e);
				}

				if (streamLength >= maxStreamLength) {
					MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidLength"));
					Object[] msgArgs = { streamLength };
					SQLServerException.makeFromDriverError(null, null, form.format(msgArgs), "", true);
				}

				assert streamLength <= Integer.MAX_VALUE;
				stream = new ByteArrayInputStream(baos.toByteArray(), 0, (int) streamLength);
			}

			assert 0 <= streamLength && streamLength <= DataTypes.IMAGE_TEXT_MAX_BYTES;

			boolean useVarType = streamLength <= DataTypes.SHORT_VARTYPE_MAX_BYTES;

			writeRPCNameValType(sName, bOut, jdbcType.isTextual() ? (useVarType ? TDSType.BIGVARCHAR : TDSType.TEXT)
					: (useVarType ? TDSType.BIGVARBINARY : TDSType.IMAGE));

			// Write maximum length, optional collation, and actual length
			if (useVarType) {
				writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
				if (jdbcType.isTextual())
					collation.writeCollation(this);
				writeShort((short) streamLength);
			} else {
				writeInt(DataTypes.IMAGE_TEXT_MAX_BYTES);
				if (jdbcType.isTextual())
					collation.writeCollation(this);
				writeInt((int) streamLength);
			}
		}

		// Write the data
		writeStream(stream, streamLength, usePLP);
	}

	/**
	 * Append the XML data in a stream in RPC transmission format.
	 * 
	 * @param sName        the optional parameter name
	 * @param stream       is the stream
	 * @param streamLength length of the stream (may be unknown)
	 * @param bOut         boolean true if the data value is being registered as an
	 *                     ouput parameter
	 * @throws SQLServerException
	 */
	void writeRPCXML(String sName, InputStream stream, long streamLength, boolean bOut) throws SQLServerException {
		assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength >= 0;
		assert DataTypes.UNKNOWN_STREAM_LENGTH == streamLength || streamLength <= DataTypes.MAX_VARTYPE_MAX_BYTES;

		writeRPCNameValType(sName, bOut, TDSType.XML);
		writeByte((byte) 0); // No schema
		// Handle null here and return, we're done here if it's null.
		if (null == stream) {
			// Null header for v*max types is 0xFFFFFFFFFFFFFFFF.
			writeLong(0xFFFFFFFFFFFFFFFFL);
		} else if (DataTypes.UNKNOWN_STREAM_LENGTH == streamLength) {
			// Append v*max length.
			// UNKNOWN_PLP_LEN is 0xFFFFFFFFFFFFFFFE
			writeLong(0xFFFFFFFFFFFFFFFEL);

			// NOTE: Don't send the first chunk length, this will be calculated by caller.
		} else {
			// For v*max types with known length, length is <totallength8><chunklength4>
			// We're sending same total length as chunk length (as we're sending 1 chunk).
			writeLong(streamLength);
		}
		if (null != stream)
			// Write the data
			writeStream(stream, streamLength, true);
	}

	/**
	 * Append the data in a character reader in RPC transmission format.
	 * 
	 * @param sName     the optional parameter name
	 * @param re        the reader
	 * @param reLength  the reader data length (in characters)
	 * @param bOut      boolean true if the data value is being registered as an
	 *                  ouput parameter
	 * @param collation The SQL collation associated with the value. Null for
	 *                  non-textual SQL Server types.
	 * @throws SQLServerException
	 */
	void writeRPCReaderUnicode(String sName, Reader re, long reLength, boolean bOut, SQLCollation collation)
			throws SQLServerException {
		assert null != re;
		assert DataTypes.UNKNOWN_STREAM_LENGTH == reLength || reLength >= 0;

		// Textual RPC requires a collation. If none is provided, as is the case when
		// the SSType is non-textual, then use the database collation by default.
		if (null == collation)
			collation = con.getDatabaseCollation();

		// Send long values and values with unknown length
		// using PLP chunking on Yukon and later.
		boolean usePLP = (DataTypes.UNKNOWN_STREAM_LENGTH == reLength || reLength > DataTypes.SHORT_VARTYPE_MAX_CHARS);
		if (usePLP) {
			assert DataTypes.UNKNOWN_STREAM_LENGTH == reLength || reLength <= DataTypes.MAX_VARTYPE_MAX_CHARS;

			writeRPCNameValType(sName, bOut, TDSType.NVARCHAR);

			// Handle Yukon v*max type header here.
			writeVMaxHeader(
					(DataTypes.UNKNOWN_STREAM_LENGTH == reLength) ? DataTypes.UNKNOWN_STREAM_LENGTH : 2 * reLength, // Length
																													// (in
																													// bytes)
					false, collation);
		}

		// Send non-PLP in all other cases
		else {
			// Length must be known if we're not sending PLP-chunked data. Yukon is handled
			// above.
			// For Shiloh, this is enforced in DTV by converting the Reader to some other
			// length-
			// prefixed value in the setter.
			assert 0 <= reLength && reLength <= DataTypes.NTEXT_MAX_CHARS;

			// For non-PLP types, use the long TEXT type rather than the short VARCHAR
			// type if the stream is too long to fit in the latter or if we don't know the
			// length up
			// front so we have to assume that it might be too long.
			boolean useVarType = reLength <= DataTypes.SHORT_VARTYPE_MAX_CHARS;

			writeRPCNameValType(sName, bOut, useVarType ? TDSType.NVARCHAR : TDSType.NTEXT);

			// Write maximum length, collation, and actual length of the data
			if (useVarType) {
				writeShort((short) DataTypes.SHORT_VARTYPE_MAX_BYTES);
				collation.writeCollation(this);
				writeShort((short) (2 * reLength));
			} else {
				writeInt(DataTypes.NTEXT_MAX_CHARS);
				collation.writeCollation(this);
				writeInt((int) (2 * reLength));
			}
		}

		// Write the data
		writeReader(re, reLength, usePLP);
	}
}