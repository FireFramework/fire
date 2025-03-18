//
// Source code recreated from a .class file by IntelliJ IDEA
// (powered by FernFlower decompiler)
//

package org.apache.calcite.avatica.util;

import java.math.BigDecimal;

public enum TimeUnit {
    YEAR(true, ' ', 12L, (BigDecimal)null),
    MONTH(true, '-', 1L, BigDecimal.valueOf(12L)),
    DAY(false, '-', 86400000L, (BigDecimal)null),
    HOUR(false, ' ', 3600000L, BigDecimal.valueOf(24L)),
    MINUTE(false, ':', 60000L, BigDecimal.valueOf(60L)),
    MILLISECOND(false, '.', 0L, BigDecimal.valueOf(1000L)),
    MICROSECOND(false, '.', BigDecimal.ONE.scaleByPowerOfTen(-3).longValue(), BigDecimal.valueOf(1000000L)),
    NANOSECOND(false, '.', BigDecimal.ONE.scaleByPowerOfTen(-6).longValue(), BigDecimal.valueOf(1000000000L)),
    DOW(false, '-', null, (BigDecimal)null),
    ISODOW(false, '-', null, (BigDecimal)null),
    DOY(false, '-', null, (BigDecimal)null),
    ISOYEAR(true, ' ', 12L, (BigDecimal)null),
    WEEK(false, '*', 604800000L, BigDecimal.valueOf(53L)),
    QUARTER(true, '*', 3L, BigDecimal.valueOf(4L)),
    EPOCH(false, '*', null, (BigDecimal)null),
    DECADE(true, '*', 120L, (BigDecimal)null),
    CENTURY(true, '*', 1200L, (BigDecimal)null),
    MILLENNIUM(true, '*', 12000L, (BigDecimal)null),
    SECOND(false, ':', 1000L, BigDecimal.valueOf(60L));

    public final boolean yearMonth;
    public final char separator;
    public final Long multiplier;
    private final BigDecimal limit;
    private static final TimeUnit[] CACHED_VALUES = values();

    private TimeUnit(boolean yearMonth, char separator, Long multiplier, BigDecimal limit) {
        this.yearMonth = yearMonth;
        this.separator = separator;
        this.multiplier = multiplier;
        this.limit = limit;
    }

    public static TimeUnit getValue(int ordinal) {
        return ordinal >= 0 && ordinal < CACHED_VALUES.length ? CACHED_VALUES[ordinal] : null;
    }

    public boolean isValidValue(BigDecimal field) {
        return field.compareTo(BigDecimal.ZERO) >= 0 && (this.limit == null || field.compareTo(this.limit) < 0);
    }
}
