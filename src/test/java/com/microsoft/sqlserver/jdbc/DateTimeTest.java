package com.microsoft.sqlserver.jdbc;

import microsoft.sql.DateTimeOffset;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.time.temporal.ChronoUnit;

import static org.junit.jupiter.api.Assertions.*;

public class DateTimeTest {

    @Test
    public void testDateTime2() {
        final OffsetDateTime expectedValue = OffsetDateTime.of(2022, 1, 31, 12, 12, 34, 56789, ZoneOffset.MAX);

//        final DateTime2Entity dateTime2 = new DateTime2Entity();
//        dateTime2.setValue(expectedValue);
//
//        final TransactionTemplate transactionTemplate1 = new TransactionTemplate(this.transactionManager);
//        final DateTime2Entity expectedDateTime2Entity = transactionTemplate1.execute(status -> dateTime2EntityRepository.save(dateTime2));
//        assertNotNull(expectedDateTime2Entity);
//
//        final TransactionTemplate transactionTemplate2 = new TransactionTemplate(this.transactionManager);
//        final OffsetDateTime actualDateTime2Value = transactionTemplate2.execute(status -> dateTime2EntityRepository.getById(expectedDateTime2Entity.getId()).getValue());
//        assertNotNull(actualDateTime2Value);
//
//        this.assertEquals(expectedValue, actualDateTime2Value);
    }

    @Test
    public void testDateTimeOffset() {
        Date date = RandomData.generateDate(false);
        Timestamp datetime2 = RandomData.generateDatetime2(7, false);
        DateTimeOffset datetimeoffset = RandomData.generateDatetimeoffset(7, false);
        Time time = RandomData.generateTime(7, false);
        Timestamp datetime = RandomData.generateDatetime(false);
        Timestamp smalldatetime = RandomData.generateSmalldatetime(false);
    }

    private void assertEquals(final OffsetDateTime expected, final OffsetDateTime actual) {
        System.out.println(expected);
        System.out.println(actual);
        long diff = expected.until(actual, ChronoUnit.MINUTES);
        System.out.println("Diff: " + diff + " minutes");
        Assertions.assertEquals(0L, diff);
    }
}