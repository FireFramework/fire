package com.zto.fire.common.util;

/**
 * 异常处理工具类
 * @author ChengLong 2019-7-19 14:33:36
 */
@Deprecated
public class StackTraceUtils {

    private StackTraceUtils() {}

    /**
     * 将异常的堆栈信息以字符串形式返回
     * @param throwable 异常对象
     * @return 堆栈描述信息
     */
    public static String stackTraceInfo(Throwable throwable) {
        if (throwable == null) {
            return "";
        }

        StringBuilder stackTraceInfo = new StringBuilder();
        stackTraceInfo.append(throwable.toString() + "\n");
        for (StackTraceElement stackTrace : throwable.getStackTrace()) {
            stackTraceInfo.append("\tat " + stackTrace + "\n");
        }
        return stackTraceInfo.toString();
    }
}
