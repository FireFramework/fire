package com.zto.fire.common.enu;

import org.apache.commons.lang3.StringUtils;

/**
 * 任务运行模式（local、on yarn、standalone、k8s）
 *
 * @author ChengLong 2023-03-30 08:56:52
 * @since 2.3.5
 */
public enum RunMode {
    AUTO("auto"), LOCAL("local"), YARN("yarn"), STANDALONE("standalone"), K8S("k8s");

    RunMode(String mode) {
    }

    /**
     * 将字符串解析成指定的枚举类型
     */
    public static RunMode parse(String mode) {
        if (StringUtils.isBlank(mode)) return AUTO;
        try {
            return Enum.valueOf(RunMode.class, mode.trim().toLowerCase());
        } catch (Exception e) {
            return AUTO;
        }
    }
}