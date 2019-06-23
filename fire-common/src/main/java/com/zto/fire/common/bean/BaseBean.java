package com.zto.fire.common.bean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * 通用父类
 * @author ChengLong 2019-6-23 11:58:56
 */
public class BaseBean implements Serializable {
    // 类名
    protected String className = this.getClass().getSimpleName().replace("$", "");
    // 日志记录器
    protected Logger logger = LoggerFactory.getLogger(this.getClass());
}
