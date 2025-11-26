package com.microsoft.sqlserver.testframework;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import org.junit.jupiter.api.Tag;

/*
 * Custom annotation to run vector and json tests on supported SQL Server versions
 * Currently, vector and json feature is supported to run on SQL Server 2025 and Azure SQL DB
 */
@Target({ElementType.METHOD, ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag(Constants.xSQLv11)
@Tag(Constants.xSQLv12)
@Tag(Constants.xSQLv14)
@Tag(Constants.xSQLv15)
@Tag(Constants.xSQLv16)
@Tag(Constants.xAzureSQLDW)
@Tag(Constants.xAzureSQLMI)
public @interface vectorJsonTest {}
