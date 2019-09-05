package com.zto.fire.common.bean;

import com.zto.fire.common.acc.AccumulatorManager;
import org.apache.spark.SparkEnv;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import spark.utils.StringUtils;

import java.io.Serializable;

/**
 * 通用父类
 *
 * @author ChengLong 2019-6-23 11:58:56
 */
public class BaseLogging implements Serializable {
    private static String mainClass;
    private static String applicationId;
    private static ThreadLocal<TimeCost> timeCostLocal = new ThreadLocal<>();
    protected static Logger logger;

    /**
     * 初始化日志记录器
     *
     * @param className 日志的类名
     */
    protected void initLogging(String className) {
        if (StringUtils.isBlank(className)) {
            className = this.getClass().getName().replace("$", "");
        }
        logger = LoggerFactory.getLogger(className);
    }

    /**
     * 开始记录日志
     */
    public void mark() {
        TimeCost timeCost = TimeCost.build();
        if (SparkEnv.get() != null) {
            if (StringUtils.isBlank(applicationId)) {
                applicationId = SparkEnv.get().conf().get("spark.app.id", "");
                timeCost.setApplicationId(applicationId);
            }
            if (StringUtils.isBlank(mainClass)) {
                mainClass = SparkEnv.get().conf().get("spark.driver.class.name", "");
                timeCost.setMainClass(mainClass);
            }
        }
        if (logger == null) this.initLogging(mainClass);
        timeCostLocal.set(timeCost);
    }

    /**
     * 用于fire框架内部日志记录
     *
     * @param msg 错误信息
     */
    protected void log(String msg) {
        this.log(msg, null);
    }

    /**
     * 用于fire框架内部日志记录
     *
     * @param msg        错误信息
     * @param module 外设（hbase、tidb、mysql）
     */
    protected void log(String msg, String module) {
        this.log(msg, module, null);
    }

    /**
     * 用于fire框架内部日志记录
     *
     * @param msg        错误信息
     * @param module 外设（hbase、tidb、mysql）
     * @param io         输入：1 输出：0
     */
    protected void log(String msg, String module, Integer io) {
        this.log(msg, module, io, null);
    }

    /**
     * 用于fire框架内部日志记录
     *
     * @param msg        错误信息
     * @param module 外设（hbase、tidb、mysql）
     * @param io         输入：1 输出：0
     * @param throwable  异常对象
     */
    protected void log(String msg, String module, Integer io, Throwable throwable) {
        this.log(msg, module, io, throwable, true);
    }

    /**
     * 日志记录
     *
     * @param msg        错误信息
     * @param module 外设（hbase、tidb、mysql）
     * @param io         输入：1 输出：0
     * @param throwable  异常对象
     * @param isFire     用于标记是否为fire框架内部埋点日志
     */
    protected void log(String msg, String module, Integer io, Throwable throwable, Boolean isFire) {
        if (timeCostLocal.get() == null) this.mark();
        TimeCost timeCost = timeCostLocal.get();
        timeCost.info(msg, module, io, isFire, throwable);
        AccumulatorManager.addLog(timeCost);
        String log = timeCost.toString();
        if (throwable == null) {
            logger.warn(log);
        } else {
            logger.error(log, throwable);
        }
        timeCostLocal.remove();
    }
}