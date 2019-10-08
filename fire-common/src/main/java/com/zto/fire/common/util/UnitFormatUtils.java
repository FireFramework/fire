package com.zto.fire.common.util;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.LinkedList;
import java.util.List;

/**
 * 通用的计量单位转换工具
 *
 * @author ChengLong 2019年9月29日 18:05:56
 */
public class UnitFormatUtils {

    /**
     * 磁盘数据单位体系中的单位的枚举
     */
    public enum DateUnitEnum {
        // 数据类型的内容
        BYTE, KB, MB, GB, TB, PB, EB;
        // 对数据类型进行排序
        private static List<DateUnitEnum> orderList = Arrays.asList(BYTE, KB, MB, GB, TB, PB, EB);
        // 定义计量单位换算关系
        private static List<BigDecimal> metric = init(1024, 1024, 1024, 1024, 1024, 1024, 1);
    }

    /**
     * 金额单位体系中的单位的枚举
     */
    public enum MoneyUnitEnum {
        // 数据类型的内容
        分, 角, 元, 万元, 十万, 百万, 千万, 亿元, 万亿;
        // 对数据类型进行排序
        private static List<MoneyUnitEnum> orderList = Arrays.asList(分, 角, 元, 万元, 十万, 百万, 千万, 亿元, 万亿);
        // 定义计量单位换算关系
        private static List<BigDecimal> metric = init(10, 10, 10000, 10, 10, 10, 10, 10000, 1);
    }

    /**
     * 数字单位体系中的单位的枚举
     */
    public enum NumberUnitEnum {
        // 数据类型的内容
        个, 十, 百, 千, 万, 十万, 百万, 千万, 亿, 十亿, 百亿, 千亿, 万亿, 十万亿, 百万亿, 千万亿;
        // 对数据类型进行排序
        private static List<NumberUnitEnum> orderList = Arrays.asList(个, 十, 百, 千, 万, 十万, 百万, 千万, 亿, 十亿, 百亿, 千亿, 万亿, 十万亿, 百万亿, 千万亿);
        // 定义计量单位换算关系
        private static List<BigDecimal> metric = init(10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 10, 1);
    }

    /**
     * 时间单位体系中的单位的枚举
     */
    public enum TimeUnitEnum {
        // 数据类型的内容
        us, ms, s, min, h, d;
        // 对数据类型进行排序
        private static List<TimeUnitEnum> orderList = Arrays.asList(us, ms, s, min, h, d);
        // 定义计量单位换算关系
        private static List<BigDecimal> metric = init(1000, 1000, 60, 60, 24, 1);
    }

    /**
     * 重量单位体系中的单位的枚举
     */
    public enum WeightUnitEnum {
        microgram, milligram, g, Kg, T;
        // 对数据类型进行排序
        private static List<WeightUnitEnum> orderList = Arrays.asList(microgram, milligram, g, Kg, T);
        // 定义计量单位换算关系
        private static List<BigDecimal> metric = init(1000, 1000, 1000, 1000, 1);
    }

    /**
     * 获取当前单位在list中的索引值
     *
     * @param unit 要查询的单位
     * @return 索引值
     */
    private static <T> int getIndex(List<T> orderList, T unit) {
        for (int i = 0; i < orderList.size(); i++) {
            if (orderList.get(i) == unit) {
                return i;
            }
        }
        return 0;
    }

    /**
     * 初始化计量单位列表
     */
    private static List<BigDecimal> init(int ... metrics) {
        List<BigDecimal> list = new LinkedList<>();
        for (int metric : metrics) {
            list.add(new BigDecimal(metric));
        }
        return list;
    }

    /**
     * 将传入磁盘数据的大数/小数等等转换为易读的形式
     * 易读的标准是可以展示为某一单位区间内的大于1的数，自动取两位小数
     *
     * @param data 传入的初始数值
     * @param unit 传入数值的单位
     * @return 转换过后的易读字符串，带单位
     */
    public static String readable(Number data, DateUnitEnum unit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(DateUnitEnum.orderList, unit);
        // 判定传入参数在当前单位下，是否超出其数值区间
        if (data.longValue() < DateUnitEnum.metric.get(DateUnitEnum.orderList.indexOf(unit)).longValue() || unit == DateUnitEnum.orderList.get(DateUnitEnum.orderList.size() - 1)) {
            // 判定传入的数值是否小于1，如果小于1，则进入
            if (data.longValue() < 1 && unit != DateUnitEnum.orderList.get(0)) {
                // 对小于1的参数进行放大，向上进一位：数值放大相应进制，进制下调一位
                return readable(data1.multiply(DateUnitEnum.metric.get(index - 1)), DateUnitEnum.orderList.get(index - 1));
            }
            // 如果是本单位区间的大于1的值，进行返回处理
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + unit.toString();
        }
        // 超出了当前单位的取值范围
        else {
            // 对数值升位：数值除以相应的进制，单位上调一位
            return readable(data1.divide(DateUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), DateUnitEnum.orderList.get(index + 1));
        }
    }

    /**
     * 将磁盘数据大小从一种单位转换为传入的单位
     *
     * @param data     输入的初始参数
     * @param fromUnit 输入的初始参数的单位
     * @param toUnit   要转换的目标单位
     */
    public static String format(Number data, DateUnitEnum fromUnit, DateUnitEnum toUnit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(DateUnitEnum.orderList, fromUnit);
        // 判别初始参数索引是否高于目标参数索引
        if (DateUnitEnum.orderList.indexOf(fromUnit) > DateUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数放大相应进制倍数，将单位下调一位
            return format(data1.multiply(DateUnitEnum.metric.get(index - 1)), DateUnitEnum.orderList.get(index - 1), toUnit);
            // 判别初始参数索引是否低于目标参数索引
        } else if (DateUnitEnum.orderList.indexOf(fromUnit) < DateUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数缩小相应进制倍数，将单位上调一位
            return format(data1.divide(DateUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), DateUnitEnum.orderList.get(index + 1), toUnit);
            // 取得fromUnit与toUnit的索引值相同的情况
        } else {
            // 进行数据处理，返回相应结果
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + fromUnit.toString();
        }
    }

    /**
     * 将传入金额的大数/小数等等转换为易读的形式
     * 易读的标准是可以展示为某一单位区间内的大于1的数，自动取两位小数
     *
     * @param data 传入的初始数值
     * @param unit 传入数值的单位
     * @return 转换过后的易读字符串，带单位
     */
    public static String readable(Number data, MoneyUnitEnum unit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(MoneyUnitEnum.orderList, unit);
        // 判定传入参数在当前单位下，是否超出其数值区间
        if (data.longValue() < MoneyUnitEnum.metric.get(MoneyUnitEnum.orderList.indexOf(unit)).longValue() || unit == MoneyUnitEnum.orderList.get(MoneyUnitEnum.orderList.size() - 1)) {
            // 判定传入的数值是否小于1，如果小于1，则进入
            if (data.longValue() < 1 && unit != MoneyUnitEnum.orderList.get(0)) {
                // 对小于1的参数进行放大，向上进一位：数值放大相应进制，进制下调一位
                return readable(data1.multiply(MoneyUnitEnum.metric.get(index - 1)), MoneyUnitEnum.orderList.get(index - 1));
            }
            // 如果是本单位区间的大于1的值，进行返回处理
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + unit.toString();
        }
        // 超出了当前单位的取值范围
        else {
            // 对数值升位：数值除以相应的进制，单位上调一位
            return readable(data1.divide(MoneyUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), MoneyUnitEnum.orderList.get(index + 1));
        }
    }

    /**
     * 将金额从一种单位转换为传入的单位
     *
     * @param data     输入的初始参数
     * @param fromUnit 输入的初始参数的单位
     * @param toUnit   要转换的目标单位
     */
    public static String format(Number data, MoneyUnitEnum fromUnit, MoneyUnitEnum toUnit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(MoneyUnitEnum.orderList, fromUnit);
        // 判别初始参数索引是否高于目标参数索引
        if (MoneyUnitEnum.orderList.indexOf(fromUnit) > MoneyUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数放大相应进制倍数，将单位下调一位
            return format(data1.multiply(MoneyUnitEnum.metric.get(index - 1)), MoneyUnitEnum.orderList.get(index - 1), toUnit);
            // 判别初始参数索引是否低于目标参数索引
        } else if (MoneyUnitEnum.orderList.indexOf(fromUnit) < MoneyUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数缩小相应进制倍数，将单位上调一位
            return format(data1.divide(MoneyUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), MoneyUnitEnum.orderList.get(index + 1), toUnit);
            // 取得fromUnit与toUnit的索引值相同的情况
        } else {
            // 进行数据处理，返回相应结果
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + fromUnit.toString();
        }
    }

    /**
     * 将传入数字的大数/小数等等转换为易读的形式
     * 易读的标准是可以展示为某一单位区间内的大于1的数，自动取两位小数
     *
     * @param data 传入的初始数值
     * @param unit 传入数值的单位
     * @return 转换过后的易读字符串，带单位
     */
    public static String readable(Number data, NumberUnitEnum unit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(NumberUnitEnum.orderList, unit);
        // 判定传入参数在当前单位下，是否超出其数值区间
        if (data.longValue() < NumberUnitEnum.metric.get(NumberUnitEnum.orderList.indexOf(unit)).longValue() || unit == NumberUnitEnum.orderList.get(NumberUnitEnum.orderList.size() - 1)) {
            // 判定传入的数值是否小于1，如果小于1，则进入
            if (data.longValue() < 1 && unit != NumberUnitEnum.orderList.get(0)) {
                // 对小于1的参数进行放大，向上进一位：数值放大相应进制，进制下调一位
                return readable(data1.multiply(NumberUnitEnum.metric.get(index - 1)), NumberUnitEnum.orderList.get(index - 1));
            }
            // 如果是本单位区间的大于1的值，进行返回处理
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + unit.toString();
        }
        // 超出了当前单位的取值范围
        else {
            // 对数值升位：数值除以相应的进制，单位上调一位
            return readable(data1.divide(NumberUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), NumberUnitEnum.orderList.get(index + 1));
        }
    }

    /**
     * 将数字从一种单位转换为传入的单位
     *
     * @param data     输入的初始参数
     * @param fromUnit 输入的初始参数的单位
     * @param toUnit   要转换的目标单位
     */
    public static String format(Number data, NumberUnitEnum fromUnit, NumberUnitEnum toUnit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(NumberUnitEnum.orderList, fromUnit);
        // 判别初始参数索引是否高于目标参数索引
        if (NumberUnitEnum.orderList.indexOf(fromUnit) > NumberUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数放大相应进制倍数，将单位下调一位
            return format(data1.multiply(NumberUnitEnum.metric.get(index - 1)), NumberUnitEnum.orderList.get(index - 1), toUnit);
            // 判别初始参数索引是否低于目标参数索引
        } else if (NumberUnitEnum.orderList.indexOf(fromUnit) < NumberUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数缩小相应进制倍数，将单位上调一位
            return format(data1.divide(NumberUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), NumberUnitEnum.orderList.get(index + 1), toUnit);
            // 取得fromUnit与toUnit的索引值相同的情况
        } else {
            // 进行数据处理，返回相应结果
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + fromUnit.toString();
        }
    }

    /**
     * 将传入时间的大数/小数等等转换为易读的形式
     * 易读的标准是可以展示为某一单位区间内的大于1的数，自动取两位小数
     *
     * @param data 传入的初始数值
     * @param unit 传入数值的单位
     * @return 转换过后的易读字符串，带单位
     */
    public static String readable(Number data, TimeUnitEnum unit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(TimeUnitEnum.orderList, unit);
        // 判定传入参数在当前单位下，是否超出其数值区间
        if (data.longValue() < TimeUnitEnum.metric.get(TimeUnitEnum.orderList.indexOf(unit)).longValue() || unit == TimeUnitEnum.orderList.get(TimeUnitEnum.orderList.size() - 1)) {
            // 判定传入的数值是否小于1，如果小于1，则进入
            if (data.longValue() < 1 && unit != TimeUnitEnum.orderList.get(0)) {
                // 对小于1的参数进行放大，向上进一位：数值放大相应进制，进制下调一位
                return readable(data1.multiply(TimeUnitEnum.metric.get(index - 1)), TimeUnitEnum.orderList.get(index - 1));
            }
            // 如果是本单位区间的大于1的值，进行返回处理
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + unit.toString();
        }
        // 超出了当前单位的取值范围
        else {
            // 对数值升位：数值除以相应的进制，单位上调一位
            return readable(data1.divide(TimeUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), TimeUnitEnum.orderList.get(index + 1));
        }
    }

    /**
     * 将时间从一种单位转换为传入的单位
     *
     * @param data     输入的初始参数
     * @param fromUnit 输入的初始参数的单位
     * @param toUnit   要转换的目标单位
     */
    public static String format(Number data, TimeUnitEnum fromUnit, TimeUnitEnum toUnit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(TimeUnitEnum.orderList, fromUnit);
        // 判别初始参数索引是否高于目标参数索引
        if (TimeUnitEnum.orderList.indexOf(fromUnit) > TimeUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数放大相应进制倍数，将单位下调一位
            return format(data1.multiply(TimeUnitEnum.metric.get(index - 1)), TimeUnitEnum.orderList.get(index - 1), toUnit);
            // 判别初始参数索引是否低于目标参数索引
        } else if (TimeUnitEnum.orderList.indexOf(fromUnit) < TimeUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数缩小相应进制倍数，将单位上调一位
            return format(data1.divide(TimeUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), TimeUnitEnum.orderList.get(index + 1), toUnit);
            // 取得fromUnit与toUnit的索引值相同的情况
        } else {
            // 进行数据处理，返回相应结果
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + fromUnit.toString();
        }
    }

    /**
     * 将传入重量的大数/小数等等转换为易读的形式
     * 易读的标准是可以展示为某一单位区间内的大于1的数，自动取两位小数
     *
     * @param data 传入的初始数值
     * @param unit 传入数值的单位
     * @return 转换过后的易读字符串，带单位
     */
    public static String readable(Number data, WeightUnitEnum unit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(WeightUnitEnum.orderList, unit);
        // 判定传入参数在当前单位下，是否超出其数值区间
        if (data.longValue() < WeightUnitEnum.metric.get(WeightUnitEnum.orderList.indexOf(unit)).longValue() || unit == WeightUnitEnum.orderList.get(WeightUnitEnum.orderList.size() - 1)) {
            // 判定传入的数值是否小于1，如果小于1，则进入
            if (data.longValue() < 1 && unit != WeightUnitEnum.orderList.get(0)) {
                // 对小于1的参数进行放大，向上进一位：数值放大相应进制，进制下调一位
                return readable(data1.multiply(WeightUnitEnum.metric.get(index - 1)), WeightUnitEnum.orderList.get(index - 1));
            }
            // 如果是本单位区间的大于1的值，进行返回处理
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + unit.toString();
        }
        // 超出了当前单位的取值范围
        else {
            // 对数值升位：数值除以相应的进制，单位上调一位
            return readable(data1.divide(WeightUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), WeightUnitEnum.orderList.get(index + 1));
        }
    }

    /**
     * 将重量从一种单位转换为传入的单位
     *
     * @param data     输入的初始参数
     * @param fromUnit 输入的初始参数的单位
     * @param toUnit   要转换的目标单位
     */
    public static String format(Number data, WeightUnitEnum fromUnit, WeightUnitEnum toUnit) {
        BigDecimal data1 = new BigDecimal(data.toString());
        // 获取初始参数的索引值
        int index = getIndex(WeightUnitEnum.orderList, fromUnit);
        // 判别初始参数索引是否高于目标参数索引
        if (WeightUnitEnum.orderList.indexOf(fromUnit) > WeightUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数放大相应进制倍数，将单位下调一位
            return format(data1.multiply(WeightUnitEnum.metric.get(index - 1)), WeightUnitEnum.orderList.get(index - 1), toUnit);
            // 判别初始参数索引是否低于目标参数索引
        } else if (WeightUnitEnum.orderList.indexOf(fromUnit) < WeightUnitEnum.orderList.indexOf(toUnit)) {
            // 递归调用方法，对参数缩小相应进制倍数，将单位上调一位
            return format(data1.divide(WeightUnitEnum.metric.get(index), 2, BigDecimal.ROUND_HALF_UP), WeightUnitEnum.orderList.get(index + 1), toUnit);
            // 取得fromUnit与toUnit的索引值相同的情况
        } else {
            // 进行数据处理，返回相应结果
            return data1.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + fromUnit.toString();
        }
    }

    public static void main(String[] args) {
        System.out.println(readable(480070426624L, DateUnitEnum.BYTE));
        System.out.println(format(1, DateUnitEnum.GB, DateUnitEnum.MB));
        System.out.println(readable(1000, MoneyUnitEnum.分));
        System.out.println(format(10000, MoneyUnitEnum.分, MoneyUnitEnum.元));
        System.out.println(readable(65655889, TimeUnitEnum.s));
        System.out.println(format(100, TimeUnitEnum.d, TimeUnitEnum.s));
    }
}
