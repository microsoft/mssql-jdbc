package com.microsoft.sqlserver.jdbc.unit.statement;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

import com.microsoft.sqlserver.jdbc.RandomUtil;
import com.microsoft.sqlserver.testframework.AbstractSQLGenerator;
import com.microsoft.sqlserver.testframework.AbstractTest;

public class BigDecimalPrecisionTest extends AbstractTest {

    String procName1 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("test_bigdecimal_3"));
    String procName2 = AbstractSQLGenerator.escapeIdentifier(RandomUtil.getIdentifier("test_bigdecimal_5"));

    @BeforeEach
    public void init() throws SQLException {
        try (Connection connection = getConnection()) {
            String dropProcedureSQL = "DROP PROCEDURE IF EXISTS " + procName1 + ", " + procName2;
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(dropProcedureSQL);
            }
            
            String createProcedureSQL1 = "CREATE PROCEDURE " + procName1 + "\n" +
                    "    @big_decimal_type      decimal(15, 3),\n" +
                    "    @big_decimal_type_o    decimal(15, 3) OUTPUT\n" +
                    "AS\n" +
                    "BEGIN\n" +
                    "    SET @big_decimal_type_o = @big_decimal_type;\n" +
                    "END;";
            String createProcedureSQL2 = "CREATE PROCEDURE " + procName2 + "\n" +
                    "    @big_decimal_type      decimal(15, 5),\n" +
                    "    @big_decimal_type_o    decimal(15, 5) OUTPUT\n" +
                    "AS\n" +
                    "BEGIN\n" +
                    "    SET @big_decimal_type_o = @big_decimal_type;\n" +
                    "END;";
            try (Statement stmt = connection.createStatement()) {
                stmt.execute(createProcedureSQL1);
                stmt.execute(createProcedureSQL2);
            }
        }
    }

    @AfterEach
    public void terminate() throws SQLException {
        try (Connection connection = getConnection()) {
            try (Statement stmt = connection.createStatement()) {
                String dropProcedureSQL = "DROP PROCEDURE IF EXISTS " + procName1 + ", " + procName2;
                stmt.execute(dropProcedureSQL);
            }
        }
    }

    @Test
    @Tag("BigDecimal")
    public void testBigDecimalPrecision() throws SQLException {
        try (Connection connection = getConnection()) {
            // Test for DECIMAL(15, 3)
            String callSQL1 = "{call " + procName1 + "(100.241, ?)}";
            try (CallableStatement call = connection.prepareCall(callSQL1)) {
                call.registerOutParameter(1, Types.DECIMAL);
                call.execute();
                BigDecimal actual1 = call.getBigDecimal(1);
                assertEquals(new BigDecimal("100.241"), actual1);
            }

            // Test for DECIMAL(15, 5)
            String callSQL2 = "{call " + procName2 + "(100.24112, ?)}";
            try (CallableStatement call = connection.prepareCall(callSQL2)) {
                call.registerOutParameter(1, Types.DECIMAL);
                call.execute();
                BigDecimal actual2 = call.getBigDecimal(1);
                assertEquals(new BigDecimal("100.24112"), actual2);
            }
        }
    }
    
    @BeforeAll
    public static void setupTests() throws Exception {
        setConnection();
    }
}
