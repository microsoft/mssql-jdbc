package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;

public class Test {

    public static void main(String args[]) {
        BigDecimal bigDecimal = new BigDecimal("12356123456123456");
        String[] plainValueArray = bigDecimal.abs().toPlainString().split("\\.");

        Integer.parseInt(plainValueArray[0]);
    }
}
