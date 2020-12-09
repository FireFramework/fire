package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * 执行命令的工具
 *
 * @author ChengLong 2019-4-10 15:50:23
 */
public class ProcessUtil {
    private ProcessUtil() {}

    /**
     * 执行多条linux命令，不返回命令执行日志
     *
     * @param commands linux命令
     * @return 命令执行结果的一行数据
     */
    public static void executeCmds(String... commands) {
        ValueUtils.requireNonNull(commands, "命令不能为空");
        for (String command : commands) {
            executeCmdForLine(command);
        }
    }

    /**
     * 执行一条linux命令，仅返回命令的一行
     *
     * @param cmd linux命令
     * @return 命令执行结果的一行数据
     */
    public static String executeCmdForLine(String cmd) {
        if (!SystemInfoUtils.isLinux() || StringUtils.isBlank(cmd)) {
            // 如果是windows环境
            return " <windows environment.> ";
        }
        Process process = null;
        BufferedReader reader = null;
        String result = "";
        try {
            process = Runtime.getRuntime().exec(cmd);
            reader = new BufferedReader(new InputStreamReader(process.getInputStream()));
            String line = "";
            while ((line = reader.readLine()) != null) {
                if (StringUtils.isNotBlank(line)) {
                    result = line;
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            IOUtils.close(process);
            IOUtils.close(reader);
        }
        return result;
    }
}
