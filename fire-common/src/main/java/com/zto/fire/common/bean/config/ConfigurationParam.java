package com.zto.fire.common.bean.config;

import com.zto.fire.common.enu.ConfigureLevel;
import com.zto.fire.common.util.JSONUtils;

import java.util.HashMap;
import java.util.Map;

/**
 * 用于解析配置中心返回的配置项：
 *
 * {"code":200,"content":{"FRAMEWORK":{"fire.thread.pool.size":"5","hive.cluster":"batch"},"TASK":{"fire.user.conf":"test","fire.conf.show.enable":"false"},"URGENT":{"hdfs.ha.conf.test.dfs.nameservices":"ns1","hdfs.ha.conf.test.fs.defaultFS":"hdfs://ns1"}}}
 *
 * @author ChengLong 2021-8-23 15:26:39
 * @since 2.2.0
 */
public class ConfigurationParam {
    private Integer code;
    private Map<ConfigureLevel, Map<String, String>> content;

    public Integer getCode() {
        return code;
    }

    public void setCode(Integer code) {
        this.code = code;
    }

    public Map<ConfigureLevel, Map<String, String>> getContent() {
        return content;
    }

    public void setContent(Map<ConfigureLevel, Map<String, String>> content) {
        this.content = content;
    }

    public static void main(String[] args) {
        Map<String, String> freamworkMap = new HashMap<>();
        freamworkMap.put("hive.cluster", "batch");
        freamworkMap.put("fire.thread.pool.size", "5");

        Map<String, String> userMap = new HashMap<>();
        userMap.put("fire.conf.show.enable", "false");
        userMap.put("fire.user.conf", "test");

        Map<String, String> priorityMap = new HashMap<>();
        priorityMap.put("hdfs.ha.conf.test.fs.defaultFS", "hdfs://ns1");
        priorityMap.put("hdfs.ha.conf.test.dfs.nameservices", "ns1");

        Map<ConfigureLevel, Map<String, String>> properties = new HashMap<>();
        properties.put(ConfigureLevel.FRAMEWORK, freamworkMap);
        properties.put(ConfigureLevel.TASK, userMap);
        properties.put(ConfigureLevel.URGENT, priorityMap);

        ConfigurationParam param = new ConfigurationParam();
        param.setCode(200);
        param.setContent(properties);

        System.out.println(JSONUtils.toJSONString(param));
    }
}