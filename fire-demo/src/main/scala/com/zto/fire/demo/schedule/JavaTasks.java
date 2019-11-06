package com.zto.fire.demo.schedule;

import com.zto.fire.common.anno.Scheduled;
import com.zto.fire.common.util.DateFormatUtils;
import com.zto.fire.core.util.SparkUtils;

import java.io.Serializable;

/**
 * java 类定义的定时任务，要求如下：
 * 1. 可序列化
 * 2. 方法不带任何参数
 * @author ChengLong 2019年11月6日 09:50:58
 */
public class JavaTasks implements Serializable {

    @Scheduled(cron = "0/30 * * * * ?", scope = "all")
    public void test6() {
        System.out.println("executorId=" + SparkUtils.getExecutorId() + "====方法 test6() 每30秒执行====" + DateFormatUtils.formatCurrentDateTime());
    }
}
