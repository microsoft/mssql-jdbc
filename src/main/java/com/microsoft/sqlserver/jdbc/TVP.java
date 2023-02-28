/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package com.microsoft.sqlserver.jdbc;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.MessageFormat;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;


enum TVPType {
    RESULTSET,
    ISQLSERVERDATARECORD,
    SQLSERVERDATATABLE,
    NULL
}


/**
 * 
 * Implementation of Table-valued parameters which provide an easy way to marshal multiple rows of data from a client
 * application to SQL Server without requiring multiple round trips or special server-side logic for processing the
 * data.
 * <p>
 * You can use table-valued parameters to encapsulate rows of data in a client application and send the data to the
 * server in a single parameterized command. The incoming data rows are stored in a table variable that can then be
 * operated on by using Transact-SQL.
 * <p>
 * Column values in table-valued parameters can be accessed using standard Transact-SQL SELECT statements. Table-valued
 * parameters are strongly typed and their structure is automatically validated. The size of table-valued parameters is
 * limited only by server memory.
 * <p>
 * You cannot return data in a table-valued parameter. Table-valued parameters are input-only; the OUTPUT keyword is not
 * supported.
 */
class TVP {

    String tvpName;
    String tvpOwningSchema;
    String tvpDbName;
    ResultSet sourceResultSet = null;
    SQLServerDataTable sourceDataTable = null;
    Map<Integer, SQLServerMetaData> columnMetadata = null;
    Iterator<Entry<Integer, Object[]>> sourceDataTableRowIterator = null;
    ISQLServerDataRecord sourceRecord = null;
    TVPType tvpType = null;
    Set<String> columnNames = null;

    // MultiPartIdentifierState
    enum MPIState {
        MPI_VALUE,
        MPI_PARSE_NONQUOTE,
        MPI_LOOK_FOR_SEPARATOR,
        MPI_LOOK_FOR_NEXT_CHAR_OR_SEPARATOR,
        MPI_PARSE_QUOTE,
        MPI_RIGHT_QUOTE,
    }

    void initTVP(TVPType type, String tvpPartName) throws SQLServerException {
        tvpType = type;
        columnMetadata = new LinkedHashMap<>();
        parseTypeName(tvpPartName);
    }

    TVP(String tvpPartName) throws SQLServerException {
        initTVP(TVPType.NULL, tvpPartName);
    }

    // Name used in CREATE TYPE
    TVP(String tvpPartName, SQLServerDataTable tvpDataTable) throws SQLServerException {
        if (tvpPartName == null) {
            tvpPartName = tvpDataTable.getTvpName();
        }
        initTVP(TVPType.SQLSERVERDATATABLE, tvpPartName);
        sourceDataTable = tvpDataTable;
        sourceDataTableRowIterator = sourceDataTable.getIterator();
        populateMetadataFromDataTable();
    }

    TVP(String tvpPartName, ResultSet tvpResultSet) throws SQLServerException {
        initTVP(TVPType.RESULTSET, tvpPartName);
        sourceResultSet = tvpResultSet;
        // Populate TVP metadata from ResultSetMetadata.
        populateMetadataFromResultSet();
    }

    TVP(String tvpPartName, ISQLServerDataRecord tvpRecord) throws SQLServerException {
        initTVP(TVPType.ISQLSERVERDATARECORD, tvpPartName);
        sourceRecord = tvpRecord;
        columnNames = new HashSet<>();

        // Populate TVP metadata from ISQLServerDataRecord.
        populateMetadataFromDataRecord();

        // validate sortOrdinal and throw all relevant exceptions before proceeding
        validateOrderProperty();
    }

    boolean isNull() {
        return (TVPType.NULL == tvpType);
    }

    Object[] getRowData() throws SQLServerException {
        if (TVPType.RESULTSET == tvpType) {
            int colCount = columnMetadata.size();
            Object[] rowData = new Object[colCount];
            for (int i = 0; i < colCount; i++) {
                try {
                    /*
                     * for Time types, getting TimeStamp instead of Time, because this value will be converted to String
                     * later on. If the value is a time object, the millisecond would be removed.
                     */
                    if (java.sql.Types.TIME == sourceResultSet.getMetaData().getColumnType(i + 1)) {
                        rowData[i] = sourceResultSet.getTimestamp(i + 1);
                    } else {
                        rowData[i] = sourceResultSet.getObject(i + 1);
                    }
                } catch (SQLException e) {
                    throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
                }
            }
            return rowData;
        } else if (TVPType.SQLSERVERDATATABLE == tvpType) {
            Map.Entry<Integer, Object[]> rowPair = sourceDataTableRowIterator.next();
            return rowPair.getValue();
        } else
            return sourceRecord.getRowData();
    }

    boolean next() throws SQLServerException {
        if (TVPType.RESULTSET == tvpType) {
            try {
                return sourceResultSet.next();
            } catch (SQLException e) {
                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveSourceData"), e);
            }
        } else if (TVPType.SQLSERVERDATATABLE == tvpType) {
            return sourceDataTableRowIterator.hasNext();
        } else if (null != sourceRecord) {
            return sourceRecord.next();
        }
        return false;
    }

    void populateMetadataFromDataTable() throws SQLServerException {
        if (null != sourceDataTable) {
            Map<Integer, SQLServerDataColumn> dataTableMetaData = sourceDataTable.getColumnMetadata();
            if (null == dataTableMetaData || dataTableMetaData.isEmpty()) {
                throw new SQLServerException(SQLServerException.getErrString("R_TVPEmptyMetadata"), null);
            }
            dataTableMetaData.entrySet()
                    .forEach(e -> columnMetadata.put(e.getKey(), new SQLServerMetaData(e.getValue().columnName,
                            e.getValue().javaSqlType, e.getValue().precision, e.getValue().scale)));
        }
    }

    void populateMetadataFromResultSet() throws SQLServerException {
        if (null != sourceResultSet) {
            try {
                ResultSetMetaData rsmd = sourceResultSet.getMetaData();
                for (int i = 0; i < rsmd.getColumnCount(); i++) {
                    SQLServerMetaData columnMetaData = new SQLServerMetaData(rsmd.getColumnName(i + 1),
                            rsmd.getColumnType(i + 1), rsmd.getPrecision(i + 1), rsmd.getScale(i + 1));
                    columnMetadata.put(i, columnMetaData);
                }
            } catch (SQLException e) {
                throw new SQLServerException(SQLServerException.getErrString("R_unableRetrieveColMeta"), e);
            }
        }
    }

    void populateMetadataFromDataRecord() throws SQLServerException {
        if (null != sourceRecord) {
            if (0 >= sourceRecord.getColumnCount()) {
                throw new SQLServerException(SQLServerException.getErrString("R_TVPEmptyMetadata"), null);
            }
            for (int i = 0; i < sourceRecord.getColumnCount(); i++) {
                Util.checkDuplicateColumnName(sourceRecord.getColumnMetaData(i + 1).columnName, columnNames);

                // Make a copy here as we do not want to change user's metadata.
                SQLServerMetaData metaData = new SQLServerMetaData(sourceRecord.getColumnMetaData(i + 1));
                columnMetadata.put(i, metaData);
            }
        }
    }

    void validateOrderProperty() throws SQLServerException {
        int columnCount = columnMetadata.size();
        boolean[] sortOrdinalSpecified = new boolean[columnCount];

        int maxSortOrdinal = -1;
        int sortCount = 0;
        for (Entry<Integer, SQLServerMetaData> columnPair : columnMetadata.entrySet()) {
            SQLServerSortOrder columnSortOrder = columnPair.getValue().sortOrder;
            int columnSortOrdinal = columnPair.getValue().sortOrdinal;

            if (SQLServerSortOrder.UNSPECIFIED != columnSortOrder) {
                // check if there's no way sort order could be monotonically increasing
                if (columnCount <= columnSortOrdinal) {
                    MessageFormat form = new MessageFormat(
                            SQLServerException.getErrString("R_TVPSortOrdinalGreaterThanFieldCount"));
                    throw new SQLServerException(form.format(new Object[] {columnSortOrdinal, columnPair.getKey()}),
                            null, 0, null);
                }

                // Check to make sure we haven't seen this ordinal before
                if (sortOrdinalSpecified[columnSortOrdinal]) {
                    MessageFormat form = new MessageFormat(
                            SQLServerException.getErrString("R_TVPDuplicateSortOrdinal"));
                    throw new SQLServerException(form.format(new Object[] {columnSortOrdinal}), null, 0, null);
                }

                sortOrdinalSpecified[columnSortOrdinal] = true;
                if (columnSortOrdinal > maxSortOrdinal)
                    maxSortOrdinal = columnSortOrdinal;

                sortCount++;
            }
        }

        if (0 < sortCount) {
            // validate monotonically increasing sort order. watch for values outside of the sortCount range.
            if (maxSortOrdinal >= sortCount) {
                // there is at least one hole, find the first one
                int i;
                for (i = 0; i < sortCount; i++) {
                    if (!sortOrdinalSpecified[i])
                        break;
                }
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_TVPMissingSortOrdinal"));
                throw new SQLServerException(form.format(new Object[] {i}), null, 0, null);
            }
        }
    }

    void parseTypeName(String name) throws SQLServerException {
        String leftQuote = "[\"";
        String rightQuote = "]\"";
        char separator = '.';
        int limit = 3; // DbName, SchemaName, table type name
        String[] parsedNames = new String[limit];
        int stringCount = 0; // index of current string in the buffer

        if ((null == name) || (0 == name.length())) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidTVPName"));
            Object[] msgArgs = {};
            throw new SQLServerException(null, form.format(msgArgs), null, 0, false);
        }

        StringBuilder sb = new StringBuilder(name.length());

        // String buffer to hold white space used when parsing non-quoted strings 'a b . c d' = 'a b' and 'c d'
        StringBuilder whitespaceSB = null;

        // Right quote character to use given the left quote character found.
        char rightQuoteChar = ' ';
        MPIState state = MPIState.MPI_VALUE;

        for (int index = 0; index < name.length(); ++index) {
            char testchar = name.charAt(index);
            switch (state) {
                case MPI_VALUE:
                    int quoteIndex;
                    if (Character.isWhitespace(testchar)) // skip the whitespace
                        continue;
                    else if (testchar == separator) {
                        // If separator was found, but no string was found, initialize the string we are parsing to
                        // Empty.
                        parsedNames[stringCount] = "";
                        stringCount++;
                    } else if (-1 != (quoteIndex = leftQuote.indexOf(testchar))) {
                        // If we are at left quote, record the corresponding right quote for the left quote
                        rightQuoteChar = rightQuote.charAt(quoteIndex);
                        sb.setLength(0);
                        state = MPIState.MPI_PARSE_QUOTE;
                    } else if (-1 != rightQuote.indexOf(testchar)) {
                        // If we shouldn't see a right quote
                        MessageFormat form = new MessageFormat(
                                SQLServerException.getErrString("R_invalidThreePartName"));
                        throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                    } else {
                        sb.setLength(0);
                        sb.append(testchar);
                        state = MPIState.MPI_PARSE_NONQUOTE;
                    }
                    break;

                case MPI_PARSE_NONQUOTE:
                    if (testchar == separator) {
                        parsedNames[stringCount] = sb.toString(); // set the currently parsed string
                        stringCount = incrementStringCount(parsedNames, stringCount);
                        state = MPIState.MPI_VALUE;
                    }
                    // Quotes are not valid inside a non-quoted name
                    else if ((-1 != rightQuote.indexOf(testchar)) || (-1 != leftQuote.indexOf(testchar))) {
                        MessageFormat form = new MessageFormat(
                                SQLServerException.getErrString("R_invalidThreePartName"));
                        throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                    } else if (Character.isWhitespace(testchar)) {
                        // If it is Whitespace
                        parsedNames[stringCount] = sb.toString(); // Set the currently parsed string
                        if (null == whitespaceSB)
                            whitespaceSB = new StringBuilder();
                        whitespaceSB.setLength(0);
                        // start to record the white space, if we are parsing a name like "foo bar" we should return
                        // "foo bar"
                        whitespaceSB.append(testchar);
                        state = MPIState.MPI_LOOK_FOR_NEXT_CHAR_OR_SEPARATOR;
                    } else
                        sb.append(testchar);

                    break;

                case MPI_LOOK_FOR_NEXT_CHAR_OR_SEPARATOR:
                    if (!Character.isWhitespace(testchar)) {
                        // If it is not whitespace
                        if (testchar == separator) {
                            stringCount = incrementStringCount(parsedNames, stringCount);
                            state = MPIState.MPI_VALUE;
                        } else {
                            // If its not a separator and not whitespace
                            sb.append(whitespaceSB);
                            sb.append(testchar);
                            // Need to set the name here in case the string ends here.
                            parsedNames[stringCount] = sb.toString();
                            state = MPIState.MPI_PARSE_NONQUOTE;
                        }
                    } else {
                        if (null == whitespaceSB) {
                            whitespaceSB = new StringBuilder();
                        }
                        whitespaceSB.append(testchar);
                    }
                    break;

                case MPI_PARSE_QUOTE:
                    // if are on a right quote see if we are escaping the right quote or ending the quoted string
                    if (testchar == rightQuoteChar)
                        state = MPIState.MPI_RIGHT_QUOTE;
                    else
                        sb.append(testchar); // Append what we are currently parsing
                    break;

                case MPI_RIGHT_QUOTE:
                    if (testchar == rightQuoteChar) {
                        // If the next char is a another right quote then we were escaping the right quote
                        sb.append(testchar);
                        state = MPIState.MPI_PARSE_QUOTE;
                    } else if (testchar == separator) {
                        // If its a separator then record what we've parsed
                        parsedNames[stringCount] = sb.toString();
                        stringCount = incrementStringCount(parsedNames, stringCount);
                        state = MPIState.MPI_VALUE;
                    } else if (!Character.isWhitespace(testchar)) {
                        // If it is not white space we got problems
                        MessageFormat form = new MessageFormat(
                                SQLServerException.getErrString("R_invalidThreePartName"));
                        throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                    } else {
                        // It is a whitespace character
                        // the following char should be whitespace, separator, or end of string anything else is bad
                        parsedNames[stringCount] = sb.toString();
                        state = MPIState.MPI_LOOK_FOR_SEPARATOR;
                    }
                    break;

                case MPI_LOOK_FOR_SEPARATOR:
                    if (!Character.isWhitespace(testchar)) {
                        // If it is not whitespace
                        if (testchar == separator) {
                            // If it is a separator
                            stringCount = incrementStringCount(parsedNames, stringCount);
                            state = MPIState.MPI_VALUE;
                        } else {
                            // not a separator
                            MessageFormat form = new MessageFormat(
                                    SQLServerException.getErrString("R_invalidThreePartName"));
                            throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
                        }
                    }
                    break;
            }
        }

        if (stringCount > limit - 1) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidThreePartName"));
            throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
        }

        // Resolve final states after parsing the string
        switch (state) {
            case MPI_VALUE: // These states require no extra action
            case MPI_LOOK_FOR_SEPARATOR:
            case MPI_LOOK_FOR_NEXT_CHAR_OR_SEPARATOR:
                break;

            case MPI_PARSE_NONQUOTE: // Dump what ever was parsed
            case MPI_RIGHT_QUOTE:
                parsedNames[stringCount] = sb.toString();
                break;

            case MPI_PARSE_QUOTE: // Invalid Ending States
            default:
                MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidThreePartName"));
                throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
        }

        if (parsedNames[0] == null) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidThreePartName"));
            throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
        } else {
            // Shuffle the parsed name, from left justification to right justification, i.e. [a][b][null][null] goes to
            // [null][null][a][b]
            int offset = limit - stringCount - 1;
            if (offset > 0) {
                for (int x = limit - 1; x >= offset; --x) {
                    parsedNames[x] = parsedNames[x - offset];
                    parsedNames[x - offset] = null;
                }
            }
        }
        this.tvpName = parsedNames[2];
        this.tvpOwningSchema = parsedNames[1];
        this.tvpDbName = parsedNames[0];
    }

    /*
     * Parsing the multi-part identifier string. parameters: name - string to parse left-quote: set of characters which
     * are valid quoting characters to initiate a quote right-quote: set of characters which are valid to stop a quote,
     * array index's correspond to the the left-quote array. separator: separator to use limit: number of names to parse
     * out removequote:to remove the quotes on the returned string
     */
    private int incrementStringCount(String[] ary, int position) throws SQLServerException {
        ++position;
        int limit = ary.length;
        if (position >= limit) {
            MessageFormat form = new MessageFormat(SQLServerException.getErrString("R_invalidThreePartName"));
            throw new SQLServerException(null, form.format(new Object[] {}), null, 0, false);
        }
        ary[position] = new String();
        return position;
    }

    String getTVPName() {
        return tvpName;
    }

    String getDbNameTVP() {
        return tvpDbName;
    }

    String getOwningSchemaNameTVP() {
        return tvpOwningSchema;
    }

    int getTVPColumnCount() {
        return columnMetadata.size();
    }

    Map<Integer, SQLServerMetaData> getColumnMetadata() {
        return columnMetadata;
    }
}
