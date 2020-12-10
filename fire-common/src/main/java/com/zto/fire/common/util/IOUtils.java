package com.zto.fire.common.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;

/**
 * io流工具类
 *
 * @author ChengLong 2019-3-27 11:17:56
 */
public class IOUtils {
    private static final Logger logger = LoggerFactory.getLogger(IOUtils.class);

    private IOUtils() {}

    /**
     * 关闭多个流
     */
    public static void close(Closeable... closeables) {
        if (closeables != null && closeables.length > 0) {
            for (Closeable io : closeables) {
                try {
                    if (io != null) {
                        io.close();
                    }
                } catch (Exception e) {
                    logger.error("close 对象失败", e);
                }
            }
        }
    }

    /**
     * 关闭多个process对象
     */
    public static void close(Process... process) {
        if (process != null && process.length > 0) {
            for (Process pro : process) {
                try {
                    if (pro != null) {
                        pro.destroy();
                    }
                } catch (Exception e) {
                    logger.error("close process 对象失败", e);
                }
            }
        }
    }
}
