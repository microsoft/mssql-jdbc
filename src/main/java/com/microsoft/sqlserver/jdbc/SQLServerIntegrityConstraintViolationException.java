package com.microsoft.sqlserver.jdbc;

public class SQLServerIntegrityConstraintViolationException extends SQLServerException { //java.sql.SQLException {

    public SQLServerIntegrityConstraintViolationException(String errText, SQLState sqlState, DriverError driverError, Throwable cause) {
        super(errText, sqlState, driverError, cause);
    }

    public SQLServerIntegrityConstraintViolationException(String errText, String errState, int errNum, Throwable cause) {
        super(errText, errState, errNum, cause);
    }

    public SQLServerIntegrityConstraintViolationException(String errText, Throwable cause) {
        super(errText, cause);
    }

    public SQLServerIntegrityConstraintViolationException(Object obj, String errText, String errState, int errNum, boolean bStack) {
        super(obj, errText, errState, errNum, bStack);
    }

    public SQLServerIntegrityConstraintViolationException(Object obj, String errText, String errState, SQLServerError sqlServerError, boolean bStack) {
        super(obj, errText, errState, sqlServerError, bStack);
    }
}
