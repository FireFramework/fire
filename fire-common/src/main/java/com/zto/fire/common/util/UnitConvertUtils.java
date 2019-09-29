package com.zto.fire.common.util;

import java.math.BigDecimal;
import java.util.*;

/**
 * 通用的计量单位转换工具
 *
 * @author ChengLong 2019年9月29日 18:05:56
 */
public class UnitConvertUtils {

    /**
     * 计量单位枚举
     * DecimalSystem书写规则
     * 最高位为封顶，都设置为 1
     * 本位的数据num计算： num * 本单位 = 上单位
     * exp： 60 * min = h
     * 即：min位的数值为60
     * 需要其他单位可视化可以自行按照规则添加相应的orderList 及 decimalSystem，
     * 并且将confirmDataType(DataUnit unit)方法更新后即可使用。
     */
    public enum DataUnit {
        microgram, milligram, g, Kg, T,         //重量
        BYTE, KB, MB, GB, TB, PB, EB,           //数据
        zm, as, fs, ps, ns, us, ms, s, min, h, d,         //时间
        分, 角, 元, 万元, 亿元, 万亿;                 //钱


        private static List<DataUnit> orderListWeight = Arrays.asList(microgram, milligram, g, Kg, T);
        private static List<BigDecimal> weightDecimalSystem = loadDecimalSystem(1000, 1000, 1000, 1000, 1);
        private static List<DataUnit> orderListData = Arrays.asList(BYTE, KB, MB, GB, TB, PB, EB);
        private static List<BigDecimal> dataDecimalSystem = loadDecimalSystem(1024, 1024, 1024, 1024, 1024, 1024, 1);
        private static List<DataUnit> orderListTime = Arrays.asList(zm, as, fs, ps, ns, us, ms, s, min, h, d);
        private static List<BigDecimal> timeDecimalSystem = loadDecimalSystem(1000, 1000, 1000, 1000, 1000, 1000, 1000, 60, 60, 24, 1);
        private static List<DataUnit> orderListMoney = Arrays.asList(分, 角, 元, 万元, 亿元, 万亿);
        private static List<BigDecimal> moneyDecimalSystem = loadDecimalSystem(10, 10, 10000, 10000, 10000, 1);

        private static Map<List<DataUnit>, List<BigDecimal>> decimalSystemMap = new HashMap<List<DataUnit>, List<BigDecimal>>();

        private static Set<List<DataUnit>> dataTypeSet = new HashSet<List<DataUnit>>();
        private static List<DataUnit> orderList;

        /**
         * to load the decimalSystem
         */
        public static List<BigDecimal> loadDecimalSystem(Integer... data) {
            List<BigDecimal> list = new ArrayList<BigDecimal>();
            for (Integer datum : data) {
                list.add(new BigDecimal(datum));
            }
            return list;
        }

        /**
         * confirm the unit dataType;
         *
         * @param unit
         */
        public static void confirmDataType(DataUnit unit) {
            if (DataUnit.dataTypeSet.isEmpty()) {
                DataUnit.dataTypeSet.add(orderListWeight);
                DataUnit.dataTypeSet.add(orderListData);
                DataUnit.dataTypeSet.add(orderListTime);
                DataUnit.dataTypeSet.add(orderListMoney);
            }
            if (DataUnit.decimalSystemMap.isEmpty()) {
                DataUnit.decimalSystemMap.put(DataUnit.orderListData, DataUnit.dataDecimalSystem);
                DataUnit.decimalSystemMap.put(DataUnit.orderListWeight, DataUnit.weightDecimalSystem);
                DataUnit.decimalSystemMap.put(DataUnit.orderListTime, DataUnit.timeDecimalSystem);
                DataUnit.decimalSystemMap.put(DataUnit.orderListMoney, DataUnit.moneyDecimalSystem);

            }
            for (List<DataUnit> dataUnits : dataTypeSet) {
                if (dataUnits.contains(unit)) {
                    DataUnit.orderList = dataUnits;
                }
            }
        }

        /**
         * 获取计量单位的位置
         */
        public static int getIndex(DataUnit unit) {
            confirmDataType(unit);
            for (int i = 0; i < orderList.size(); i++) {
                if (orderList.get(i) == unit) {
                    return i;
                }
            }
            return 0;
        }
    }

    /**
     * 将TB单位转为人类可读的方式
     */
    public static synchronized String readable(BigDecimal data, DataUnit unit) {

        int index = DataUnit.getIndex(unit);
        if (data.longValue() < DataUnit.decimalSystemMap.get(DataUnit.orderList).get(DataUnit.orderList.indexOf(unit)).longValue() || unit == DataUnit.orderList.get(DataUnit.orderList.size() - 1)) {
            if (data.longValue() < 1 && unit != DataUnit.orderList.get(0)) {
                return readable(data.multiply(DataUnit.decimalSystemMap.get(DataUnit.orderList).get(index - 1)), DataUnit.orderList.get(index - 1));
            }
            return data.divide(new BigDecimal(1), 2, BigDecimal.ROUND_HALF_UP) + unit.toString();
        } else {
            return readable(data.divide(DataUnit.decimalSystemMap.get(DataUnit.orderList).get(index), 2, BigDecimal.ROUND_HALF_UP), DataUnit.orderList.get(index + 1));
        }
    }


    /**
     * TestMethod
     *
     * @param args
     */
    public static void main(String[] args) {
        System.out.println(UnitConvertUtils.readable(new BigDecimal(0.213), DataUnit.元));
        System.out.println(UnitConvertUtils.readable(new BigDecimal(0.213), DataUnit.元));
        System.out.println(UnitConvertUtils.readable(new BigDecimal(0.213), DataUnit.元));
    }
}
