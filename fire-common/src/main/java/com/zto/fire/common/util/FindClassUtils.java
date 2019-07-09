package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.io.Serializable;
import java.net.JarURLConnection;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.LinkedList;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 查找指定包下所有的类
 * Created by ChengLong on 2018-03-23.
 */
public class FindClassUtils {
    // 接口类class 用于过滤
    private static Class<?> superStrategy = Serializable.class;
    // 默认使用的类加载器
    private static ClassLoader classLoader = FindClassUtils.class.getClassLoader();

    /**
     * 获取包下所有实现了superStrategy的类并加入list
     */
    public static List<Class<? extends Serializable>> listPackageClasses(String... packageNames) {
        List<Class<? extends Serializable>> classList = new ArrayList<Class<? extends Serializable>>();
        if (packageNames != null && packageNames.length > 0) {
            for (String packageName : packageNames) {
                if (StringUtils.isNotBlank(packageName) && packageName.contains(".")) {
                    URL url = FindClassUtils.classLoader.getResource(packageName.replace(".", "/"));
                    String protocol = url.getProtocol();
                    if ("file".equals(protocol)) {
                        // 本地自己可见的代码
                        FindClassUtils.findClassLocal(packageName, classList);
                    } else if ("jar".equals(protocol)) {
                        // 引用jar包的代码
                        FindClassUtils.findClassJar(packageName, classList);
                    }
                }
            }
        }
        return classList;
    }

    /**
     * 本地查找
     *
     * @param packName
     */
    private static void findClassLocal(final String packName, final List<Class<? extends Serializable>> list) {
        URI url = null;
        try {
            url = FindClassUtils.classLoader.getResource(packName.replace(".", "/")).toURI();
        } catch (URISyntaxException e1) {
            throw new RuntimeException("未找到相关资源");
        }

        File file = new File(url);
        file.listFiles(new FileFilter() {
            public boolean accept(File chiFile) {
                if (chiFile.isDirectory()) {
                    FindClassUtils.findClassLocal(packName + "." + chiFile.getName(), list);
                }
                if (chiFile.getName().endsWith(".class")) {
                    Class<?> clazz = null;
                    try {
                        clazz = FindClassUtils.classLoader.loadClass(packName + "." + chiFile.getName().replace(".class", ""));
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    if (FindClassUtils.superStrategy.isAssignableFrom(clazz)) {
                        list.add((Class<? extends Serializable>) clazz);
                    }
                    return true;
                }
                return false;
            }
        });

    }


    /**
     * 从jar包中查找指定包下的文件
     *
     * @param packName
     */
    private static void findClassJar(final String packName, final List<Class<? extends Serializable>> list) {
        String pathName = packName.replace(".", "/");
        JarFile jarFile = null;
        try {
            URL url = FindClassUtils.classLoader.getResource(pathName);
            JarURLConnection jarURLConnection = (JarURLConnection) url.openConnection();
            jarFile = jarURLConnection.getJarFile();
        } catch (IOException e) {
            throw new RuntimeException("未找到相关资源");
        }

        Enumeration<JarEntry> jarEntries = jarFile.entries();
        while (jarEntries.hasMoreElements()) {
            JarEntry jarEntry = jarEntries.nextElement();
            String jarEntryName = jarEntry.getName();

            if (jarEntryName.contains(pathName) && !jarEntryName.equals(pathName + "/")) {
                // 递归遍历子目录
                if (jarEntry.isDirectory()) {
                    String clazzName = jarEntry.getName().replace("/", ".");
                    int endIndex = clazzName.lastIndexOf(".");
                    String prefix = null;
                    if (endIndex > 0) {
                        prefix = clazzName.substring(0, endIndex);
                    }
                    findClassJar(prefix, list);
                }
                if (jarEntry.getName().endsWith(".class")) {
                    Class<?> clazz = null;
                    try {
                        clazz = FindClassUtils.classLoader.loadClass(jarEntry.getName().replace("/", ".").replace(".class", ""));
                    } catch (ClassNotFoundException e) {
                        e.printStackTrace();
                    }
                    if (FindClassUtils.superStrategy.isAssignableFrom(clazz)) {
                        list.add((Class<? extends Serializable>) clazz);
                    }
                }
            }
        }
    }

    /**
     * 用于判断当前以jar方式运行还是以idea方式运行
     *
     * @return true：jar方式 false：idea运行
     */
    public static boolean isJar() {
        URL url = FindClassUtils.class.getProtectionDomain().getCodeSource().getLocation();
        if (url.getPath().endsWith(".jar")) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * 获取指定文件名在jar包中的位置，兼容非jar包
     *
     * @param fileName 文件名
     * @return 路径名+文件名
     */
    public static String findFileInJar(String fileName) {
        if (StringUtils.isBlank(fileName)) {
            return null;
        }
        String fullName = "";
        URL url = FindClassUtils.class.getProtectionDomain().getCodeSource().getLocation();
        if (url.getPath().endsWith(".jar")) {
            JarFile jarFile = null;
            try {
                jarFile = new JarFile(url.getFile());
                Enumeration<JarEntry> entrys = jarFile.entries();
                while (entrys.hasMoreElements()) {
                    JarEntry jar = entrys.nextElement();
                    String name = jar.getName();
                    if (name.endsWith("/" + fileName)) {
                        fullName = name;
                        break;
                    }
                }
            } catch (IOException e) {
                e.printStackTrace();
            } finally {
                try {
                    if (jarFile != null) jarFile.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        } else {
            // 在IDEA中执行
            try {
                List<File> searchList = new LinkedList<>();
                FileUtils.findFile(FindClassUtils.class.getResource("/").getPath(), fileName, searchList);
                if (searchList != null && searchList.size() > 0) {
                    fullName = searchList.get(0).getPath();
                }
            } catch (Exception ex) {
                ex.printStackTrace();
            }
        }

        return fullName;
    }

}
