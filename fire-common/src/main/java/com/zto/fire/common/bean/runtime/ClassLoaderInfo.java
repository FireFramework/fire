package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;

import java.lang.management.ClassLoadingMXBean;
import java.lang.management.ManagementFactory;

/**
 * 获取运行时class loader信息
 * @author ChengLong 2019年9月28日 19:56:18
 */
public class ClassLoaderInfo {
    // 获取已加载的类数量
    private long loadedClassCount;
    // 获取总的类加载数
    private long totalLoadedClassCount;
    // 获取未被加载的类总数
    private long unloadedClassCount;

    private ClassLoaderInfo() {}

    public long getLoadedClassCount() {
        return loadedClassCount;
    }

    public long getTotalLoadedClassCount() {
        return totalLoadedClassCount;
    }

    public long getUnloadedClassCount() {
        return unloadedClassCount;
    }

    /**
     * 获取类加载器相关信息
     */
    public static ClassLoaderInfo getClassLoaderInfo() {
        ClassLoaderInfo classLoaderInfo = new ClassLoaderInfo();
        // 获取类加载器相关信息
        ClassLoadingMXBean classLoadingMXBean = ManagementFactory.getClassLoadingMXBean();
        classLoaderInfo.loadedClassCount = classLoadingMXBean.getLoadedClassCount();
        classLoaderInfo.totalLoadedClassCount = classLoadingMXBean.getTotalLoadedClassCount();
        classLoaderInfo.unloadedClassCount = classLoadingMXBean.getUnloadedClassCount();

        return classLoaderInfo;
    }

    public static void main(String[] args) {
        System.out.println(JSON.toJSONString(getClassLoaderInfo()));
        System.out.println(JSON.toJSONString(getClassLoaderInfo()));
        System.out.println(JSON.toJSONString(getClassLoaderInfo()));
    }
}