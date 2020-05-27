package com.zto.fire.common.util;

import com.zto.fire.common.bean.TimeCost;
import org.apache.commons.lang3.StringUtils;
import org.apache.spark.SparkEnv;

/**
 * 用于组装TimeCost消息的工具类
 *
 * @author ChengLong
 * @create 2020-05-26 14:59
 * @since 1.0.0
 */
public class TimeCostUtils {
    // 任务的applicationId
    private static String applicationId;
    // 任务的main方法
    private static String mainClass;
    // executorId
    private static String executorId;

    /**
     * 获取计算引擎运行时信息，并设置到timeCost中
     */
    public static void getEngineInfo() {
        if (SparkEnv.get() != null) {
            if (StringUtils.isBlank(TimeCost.getExecutorId())) {
                TimeCost.setExecutorId(SparkEnv.get().executorId());
            }
            if (StringUtils.isBlank(TimeCost.getApplicationId())) {
                TimeCost.setApplicationId(SparkEnv.get().conf().get("spark.app.id", ""));
            }
            if (StringUtils.isBlank(TimeCost.getMainClass())) {
                TimeCost.setMainClass(SparkEnv.get().conf().get("spark.driver.class.name", ""));
            }
        }
    }
}
