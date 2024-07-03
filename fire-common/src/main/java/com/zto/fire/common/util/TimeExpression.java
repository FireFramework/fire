package com.zto.fire.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

/**
 * 用于判断当前时间是否在指定时间段内
 */
public class TimeExpression {
    private static final Logger LOG = LoggerFactory.getLogger(TimeExpression.class);

    private final String str;  
    private static final long H24 = 24L * 60 * 60 * 1000;  
  
    public TimeExpression(String str) {  
        this.str = str;  
    }  
  
    private void validate() throws IllegalArgumentException {  
        if (str == null || str.isEmpty() || !str.contains(":")) {  
            throw new IllegalArgumentException("时间表达式不合法，格式如下：12:00:00~13:05, 23:00~00:05");  
        }  
    }  
  
    private SimpleDateFormat getTimeFormat(String pattern) {
        SimpleDateFormat timeFormat = new SimpleDateFormat(pattern, Locale.getDefault());
        timeFormat.setTimeZone(TimeZone.getTimeZone("Asia/Shanghai"));
        return timeFormat;  
    }  
  
    private String getCurrentDay() {  
        return getTimeFormat("yyyy-MM-dd").format(new Date());  
    }  
  
    private long parseToTimestamp(String date) throws ParseException {  
        return getTimeFormat("yyyy-MM-dd HH:mm:ss").parse(date).getTime();  
    }  
  
    private List<long[]> parse() throws ParseException {
        validate();  
        String currentDay = getCurrentDay();  
  
        List<String> timeDurations = Arrays.asList(str.split(","));
        List<long[]> result = new ArrayList<>();
  
        for (String timeDuration : timeDurations) {  
            String trimmed = timeDuration.trim();  
            if (!trimmed.isEmpty() && trimmed.contains(":") && trimmed.contains("~")) {  
                String[] timePair = trimmed.split("~");  
                for (int i = 0; i < timePair.length; i++) {  
                    timePair[i] = timePair[i].trim();  
                    if (timePair[i].length() == 5) {  
                        timePair[i] = currentDay + " " + timePair[i] + ":00";  
                    } else {  
                        timePair[i] = currentDay + " " + timePair[i];  
                    }  
                }  
  
                long start = parseToTimestamp(timePair[0]);  
                long end = parseToTimestamp(timePair[1]);  
                if (end > start) {  
                    result.add(new long[]{start, end});  
                } else {  
                    result.add(new long[]{start - H24, end});  
                }  
            }  
        }  
  
        return result;  
    }  
  
    public boolean isBetween(long timestamp) {  
        try {  
            List<long[]> timeRanges = parse();  
            for (long[] range : timeRanges) {  
                if (timestamp >= range[0] && timestamp <= range[1]) {  
                    return true;  
                }  
            }  
        } catch (ParseException e) {  
            LOG.error("时间区间判断失败！", e);
        }  
        return false;  
    }
}
