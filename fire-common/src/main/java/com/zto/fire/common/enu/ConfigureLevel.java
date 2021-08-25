package com.zto.fire.common.enu;

/**
 * 用于定义配置的级别
 *
 * @author ChengLong 2021-8-23 16:29:29
 * @since 2.2.0
 */
public enum ConfigureLevel {
    FRAMEWORK(10), // 框架级别配置，通用的配置信息
    TASK(20),      // 任务级别配置，每个任务单独的配置
    URGENT(30);  // 紧急配置，优先级高于用户级别配置

    ConfigureLevel(int level) {
    }
}