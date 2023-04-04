/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.net.URL;
import java.util.List;
import java.util.Objects;

/**
 * 文件操作工具类
 *
 * @author ChengLong 2018年8月22日 13:10:03
 */
public class FileUtils {
    private static Logger logger = LoggerFactory.getLogger(FileUtils.class);

    private FileUtils() {
    }


    /**
     * 递归查找指定目录下的文件
     *
     * @param path     路径
     * @param fileName 文件名
     * @return 文件全路径
     */
    public static File findFile(String path, String fileName, List<File> fileList) {
        File searchFile = null;
        File dir = new File(path);
        if (dir.exists() && dir.isDirectory()) {
            for (File file : Objects.requireNonNull(dir.listFiles())) {
                if (file.isDirectory()) {
                    searchFile = findFile(file.getPath(), fileName, fileList);
                } else {
                    if (file.getName().equals(fileName)) {
                        searchFile = file;
                        break;
                    }
                }
            }
        }
        if (searchFile != null) {
            fileList.add(searchFile);
        }
        return searchFile;
    }

    /**
     * 读取指定的文本文件内容
     *
     * @param file 文本文件
     * @return 文件内容
     * @throws Exception
     */
    public static String readTextFile(File file) throws Exception {
        if (file == null || !file.exists() || file.isDirectory())
            throw new FileNotFoundException("文件不合法，读取内容失败！" + OSUtils.getIp() + ":/" + file);

        StringBuilder sqlBuilder = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(new FileReader(file))) {
            String readLine = "";
            while ((readLine = reader.readLine()) != null) {
                sqlBuilder.append(readLine + "\n");
            }
        } catch (Exception e) {
            throw e;
        }

        return sqlBuilder.toString();
    }

    /**
     * 判断resource路径下的文件是否存在
     *
     * @param fileName 配置文件名称
     * @return null: 不存在，否则为存在
     */
    public static InputStream resourceFileExists(String fileName) {
        return FileUtils.class.getClassLoader().getResourceAsStream(fileName);
    }

    /**
     * 获取类的jar包或路径信息，可用于jar包冲突排查
     */
    public static String getClassJarPath(Class<?> clazz) {
        try {
            String classPathName = clazz.getName().replace(".", "/");
            String resource = "/" + classPathName + ".class";
            URL url = clazz.getResource(resource);
            return url.getFile();
        } catch (Exception e) {
            logger.error("未获取到类的路径信息：" + clazz.getName(), e);
        }
        return "NOT_FOUND";
    }

    /**
     * 用于判断指定路径的文件是否存在
     *
     * @param filePath 文件路径
     */
    public static boolean exists(String filePath) {
        if (StringUtils.isBlank(filePath)) return false;

        File file = new File(filePath);
        return file.exists();
    }

    /**
     * 为指定的path路径添加斜线分隔符
     *
     * @param path 原始path字符串路径
     * @return 以斜线结尾的path
     */
    public static String appendSeparator(String path) {
        if (StringUtils.isBlank(path)) return "";

        String trimPath = StringUtils.trim(path);
        if (trimPath.endsWith(File.separator)) {
            return trimPath;
        } else {
            return trimPath + File.separator;
        }
    }

    /**
     * 将多个路径合并成一个
     *
     * @param files 多个文件路径
     * @return 以指定路径分隔符拼接后的完整路径
     */
    public static String appendPath(String... files) {
        if (files == null || files.length == 0) return "";
        StringBuilder path = new StringBuilder();

        for (String file : files) {
            path.append(appendSeparator(file));
        }

        return path.substring(0, path.length() - 1);
    }
}
