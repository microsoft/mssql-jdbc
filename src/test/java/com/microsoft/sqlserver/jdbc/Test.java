package com.microsoft.sqlserver.jdbc;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;

class Test {

    public static BigDecimal roundIfNeeded(BigDecimal toRound) {
        BigDecimal rounded = toRound;

        if (rounded != null) {
            if (rounded.precision() >= 38) {
                rounded = rounded.round(new MathContext(38, RoundingMode.HALF_UP));
            }

            if (rounded.scale() >= 38) {
                rounded = rounded.setScale(38, RoundingMode.HALF_UP);
            }
        }

        return rounded;
    }


    public static void main(String[] args) {

        BigDecimal number = new BigDecimal("0.984323197631384351864123168423168743216874321684132168451946546357451684134654654561465");
        System.out.println(number.toPlainString() + "| scale:" + number.scale() + "| prec:" + number.precision());
        number = roundIfNeeded(number);
        System.out.println(number.toPlainString() + "| scale:" + number.scale() + "| prec:" + number.precision());

        String[] plainValueArray = number.abs().toPlainString().split("\\.");
        System.out.println(number.abs().toPlainString());

        int calculatedPrecision;
        if (plainValueArray.length == 2) {
            if (Integer.parseInt(plainValueArray[0]) == 0) {
                calculatedPrecision = plainValueArray[1].length();
            } else {
                calculatedPrecision = plainValueArray[0].length() + plainValueArray[1].length();
            }
        } else {
            calculatedPrecision = plainValueArray[0].length();
        }

        String sqlTypeDef = SSType.DECIMAL + "(" +
                //Precision +
                number.precision()
                + ", " +
                //Scale
                number.scale() + ")";
        System.out.println(sqlTypeDef);

        System.out.println("++++++++++++++++++++++++++++");


        number = new BigDecimal("1.984323197631384351864123168423168743216874321684132168451946546357451684134654654561465");
        System.out.println(number.toPlainString() + "| scale:" + number.scale() + "| prec:" + number.precision());
        number = roundIfNeeded(number);
        System.out.println(number.toPlainString() + "| scale:" + number.scale() + "| prec:" + number.precision());

        plainValueArray = number.abs().toPlainString().split("\\.");
        System.out.println(number.abs().toPlainString());
        sqlTypeDef = SSType.DECIMAL + "(" +
                //Precision
                (plainValueArray.length == 2 ? plainValueArray[0].length()
                        + plainValueArray[1].length() : plainValueArray[0].length())
                + ", " +
                //Scale
                (plainValueArray.length == 2 ? plainValueArray[1].length() : 0) + ")";

        System.out.println(sqlTypeDef);

    }
}
