package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * 执行命令的工具
 *
 * @author ChengLong 2019-4-10 15:50:23
 */
public class ProcessUtil {
    private static final Logger logger = LoggerFactory.getLogger(ProcessUtil.class);

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

    /**
     * 执行多linux命令
     *
     * @param command
     * @return
     * @throws Exception
     */
    public static int execAndWaitFor(String... command) throws Exception {
        int exitValue = -1;
        Process p = null;
        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream();
            p = pb.start();
            drainInputStreams(p.getInputStream());
            errorInputStreams(p.getErrorStream());
            p.waitFor();
            exitValue = p.exitValue();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (p != null) {
                p.destroy();
            }
        }
        return exitValue;
    }

    /**
     * 获取错误的日志
     *
     * @param inputStreams
     */
    private static void errorInputStreams(InputStream... inputStreams) {
        for (final InputStream inputStream : inputStreams) {
            new Thread() {
                public void run() {
                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                    try {
                        StringBuffer buffer = new StringBuffer();
                        String line = null;
                        while ((line = br.readLine()) != null) {
                            if (line != null) {
                                buffer.append(line);
                            }
                        }
                        if (buffer.length() > 0) {
                            logger.info("ProcessUtil errorInputStreams is " + buffer.toString());
                        }
                    } catch (IOException e) {
                        logger.info("ProcessUtil errorInputStreams is " + e.getMessage());
                    } finally {
                        try {
                            if (inputStream != null)
                                inputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        }
    }

    /**
     * 获取执行日志
     *
     * @param inputStreams
     */
    private static void drainInputStreams(InputStream... inputStreams) {
        for (final InputStream inputStream : inputStreams) {
            new Thread() {
                public void run() {
                    BufferedReader br = new BufferedReader(new InputStreamReader(inputStream));
                    try {
                        String line = null;
                        while ((line = br.readLine()) != null) {
                            if (line != null) {
                                logger.info(line);
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    } finally {
                        try {
                            if (inputStream != null)
                                inputStream.close();
                        } catch (IOException e) {
                            e.printStackTrace();
                        }
                    }
                }
            }.start();
        }
    }

}
