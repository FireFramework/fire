package com.zto.fire.common.enu;

import org.apache.commons.lang3.StringUtils;

/**
 * 数据源类型
 *
 * @author ChengLong
 * @create 2020-07-07 16:36
 * @since 1.0.0
 */
public enum DataSource {
    HIVE(1), HBASE(2), KAFKA(3), ROCKETMQ(4), REDIS(5),
    ES(6), MYSQL(7), TIDB(8), ORACLE(9), SQLSERVER(10),
    DB2(11), CLICKHOUSE(12), PRESTO(13), KYLIN(14), UNKNOWN(20);

    DataSource(int type) {
    }

    /**
     * 将字符串解析成指定的枚举类型
     */
    public static DataSource parse(String dataSource) {
        if (StringUtils.isBlank(dataSource)) return UNKNOWN;
        try {
            return Enum.valueOf(DataSource.class, dataSource.trim().toUpperCase());
        } catch (Exception e) {
            return UNKNOWN;
        }
    }

}
