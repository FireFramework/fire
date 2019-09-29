package com.zto.fire.common.util;

import java.math.BigDecimal;

/**
 * 数据计算工具类
 *
 * @author ChengLong 2019年9月29日 13:50:31
 */
public class MathUtils {

    /**
     * 计算百分比，并保留指定的小数位
     *
     * @param molecule    分子
     * @param denominator 分母
     * @param scale       精度
     * @return 百分比
     */
    public static double percent(long molecule, long denominator, int scale) {
        if (molecule == 0 || denominator == 0) {
            return 0.00;
        }
        return new BigDecimal(100.00 * molecule / denominator).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }

    /**
     * 将指定double类型数据以四舍五入的方式保留指定的精度
     *
     * @param data  数据
     * @param scale 精度
     * @return 四舍五入后的数据
     */
    public static double doubleScale(double data, int scale) {
        return new BigDecimal(data).setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }
}
