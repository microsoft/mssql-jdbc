package com.microsoft.sqlserver.jdbc;

public class SQLServerIntegrityConstraintViolationException extends RuntimeException { //java.sql.SQLException {

    // Facility for driver-specific error codes
    static final int DRIVER_ERROR_NONE = 0;
    static final int DRIVER_ERROR_FROM_DATABASE = 2;
    static final int DRIVER_ERROR_IO_FAILED = 3;
    static final int DRIVER_ERROR_INVALID_TDS = 4;
    static final int DRIVER_ERROR_SSL_FAILED = 5;
    static final int DRIVER_ERROR_UNSUPPORTED_CONFIG = 6;
    static final int DRIVER_ERROR_INTERMITTENT_TLS_FAILED = 7;
    static final int ERROR_SOCKET_TIMEOUT = 8;
    static final int ERROR_QUERY_TIMEOUT = 9;
    static final int DATA_CLASSIFICATION_INVALID_VERSION = 10;
    static final int DATA_CLASSIFICATION_NOT_EXPECTED = 11;
    static final int DATA_CLASSIFICATION_INVALID_LABEL_INDEX = 12;
    static final int DATA_CLASSIFICATION_INVALID_INFORMATION_TYPE_INDEX = 13;

    private int driverErrorCode = DRIVER_ERROR_NONE;
    private SQLServerError sqlServerError;
    private String sqlState;

    final int getDriverErrorCode() {
        return driverErrorCode;
    }

    final void setDriverErrorCode(int value) {
        driverErrorCode = value;
    }

    public SQLServerIntegrityConstraintViolationException(Throwable cause, int driverErrorCode, SQLServerError sqlServerError,
                                                          String sqlState) {
        super(cause);
        this.driverErrorCode = driverErrorCode;
        this.sqlServerError = sqlServerError;
        this.sqlState = sqlState;
    }
}
