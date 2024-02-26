/*
 * Microsoft JDBC Driver for SQL Server Copyright(c) Microsoft Corporation All rights reserved. This program is made
 * available under the terms of the MIT License. See the LICENSE file in the project root for more information.
 */

package microsoft.sql;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class DateTimeOffsetTest {

    @Test
    @DisplayName("DateTimeOffset.valueOf(offsetDateTime) instantiates correct DateTimeOffset instances.")
    void valueOfOffsetDateTime() {
        int nanos = 123456789;
        int hundredNanos = 1234568; // DateTimeOffset has a precision of 100 nanos of second
        OffsetDateTime offsetDateTime = OffsetDateTime.of(
                2024,
                2,
                25,
                23,
                55,
                6,
                nanos,
                ZoneOffset.ofHoursMinutes(1, 30)
        );
        Assertions
                .assertEquals(
                        offsetDateTime.withNano(hundredNanos * 100),
                        DateTimeOffset.valueOf(offsetDateTime).getOffsetDateTime()
                );
    }

    @Test
    @DisplayName("DateTimeOffset.valueOf(offsetDateTime) correctly rounds up values within 50 nanoseconds of the next second.")
    void valueOfOffsetDateTimeRounding() {
        OffsetDateTime offsetDateTime = OffsetDateTime.now().withNano(999999950);
        Assertions
                .assertEquals(
                        offsetDateTime
                                .withSecond(offsetDateTime.getSecond() + 1)
                                .withNano(0),
                        DateTimeOffset.valueOf(offsetDateTime).getOffsetDateTime()
                );
    }
}
