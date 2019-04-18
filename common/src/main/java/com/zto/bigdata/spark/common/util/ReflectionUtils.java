package com.zto.bigdata.spark.common.util;

import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.io.FileFilter;
import java.io.IOException;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.net.JarURLConnection;
import java.net.URL;
import java.net.URLDecoder;
import java.util.*;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

/**
 * 反射工具类
 * Created by ChengLong on 2017-03-30.
 */
public class ReflectionUtils {

    /**
     * 获取所有公有字段，并返回Map
     * @param clazz
     * @return
     */
    public static Map<String, Field> getFields(Class clazz) {
        if(clazz == null) {
            return Collections.emptyMap();
        }
        Field[] fields = clazz.getFields();
        if(ParamUtils.isBlank(fields)) {
            return Collections.emptyMap();
        }
        Map<String, Field> fieldMap = new HashMap<String, Field>(fields.length);
        for(Field field: fields) {
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取所有声明字段，并返回Map
     * @param clazz
     * @return
     */
    public static Map<String, Field> getDeclaredFields(Class clazz) {
        if(clazz == null) {
            return Collections.emptyMap();
        }
        Field[] fields = clazz.getDeclaredFields();
        if(ParamUtils.isBlank(fields)) {
            return Collections.emptyMap();
        }
        Map<String, Field> fieldMap = new HashMap<String, Field>(fields.length);
        for(Field field: fields) {
            field.setAccessible(true);
            fieldMap.put(field.getName(), field);
        }
        return fieldMap;
    }

    /**
     * 获取所有字段，含私有和继承而来的，并返回Map
     * @param clazz
     * @return
     */
    public static Map<String, Field> getAllFields(Class clazz) {
        Map<String, Field> fieldMap = new HashMap<String, Field>();
        fieldMap.putAll(getFields(clazz));
        fieldMap.putAll(getDeclaredFields(clazz));
        return fieldMap;
    }

    /**
     * 获取所有方法，含私有和继承而来的，并返回Map
     * @param clazz
     * @return
     */
    public static Map<String, Method> getAllMethods(Class clazz) {
        Map<String, Method> methodMap = new HashMap<String, Method>();
        methodMap.putAll(getMethods(clazz));
        methodMap.putAll(getDeclaredMethods(clazz));
        return methodMap;
    }

    /**
     * 获取所有公有方法，并返回Map
     * @param clazz
     * @return
     */
    public static Map<String, Method> getMethods(Class clazz) {
        if(clazz == null) {
            return Collections.emptyMap();
        }
        Method[] methods = clazz.getMethods();
        if(ParamUtils.isBlank(methods)) {
            return Collections.emptyMap();
        }
        Map<String, Method> methodMap = new HashMap<String, Method>(methods.length);
        for(Method method : methods) {
            methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * 获取所有声明方法，并返回Map
     * @param clazz
     * @return
     */
    public static Map<String, Method> getDeclaredMethods(Class clazz) {
        if(clazz == null) {
            return Collections.emptyMap();
        }
        Method[] methods = clazz.getDeclaredMethods();
        if(ParamUtils.isBlank(methods)) {
            return Collections.emptyMap();
        }
        Map<String, Method> methodMap = new HashMap<String, Method>(methods.length);
        for(Method method : methods) {
            method.setAccessible(true);
            methodMap.put(method.getName(), method);
        }
        return methodMap;
    }

    /**
     * 获取指定field的类型
     * @param clazz
     * @param fieldName
     * @return
     */
    public static Class getFieldType(Class clazz, String fieldName) {
        if(ParamUtils.isBlank(clazz, fieldName)) {
            return null;
        }
        try {
            Field field = clazz.getDeclaredField(fieldName);
            if(field != null) {
                field.setAccessible(true);
                return field.getType();
            }
        } catch (Exception e) {
            throw new IllegalArgumentException("指定的Field:" + fieldName + "不存在，请检查");
        }
        return null;
    }

    /**
     * 获取指定的annotation
     * @param clazz
     * @param scope
     * annotation所在的位置
     * @param memberName
     * 成员名称，指定获取指定成员的Annotation实例
     */
    private static<T extends Annotation> Annotation getAnnotation(Class clazz, ElementType scope, String memberName, Class<T> annoClass) {
        if(ParamUtils.isBlank(clazz, scope, memberName, annoClass)) {
           return null;
        }
        try {
            if(ElementType.FIELD == scope) {
                Field field = clazz.getDeclaredField(memberName);
                field.setAccessible(true);
                return field.getAnnotation(annoClass);
            } else if(ElementType.METHOD == scope) {
                Method method = clazz.getDeclaredMethod(memberName);
                method.setAccessible(true);
                return method.getAnnotation(annoClass);
            } else if(ElementType.TYPE == scope) {
                return clazz.getAnnotation(annoClass);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取指定的annotation
     * @param clazz
     * @param scope
     * annotation所在的位置
     * @param memberName
     * 成员名称，指定获取指定成员的Annotation实例
     */
    private static Annotation[] getAnnotations(Class clazz, ElementType scope, String memberName) {
        if(ParamUtils.isBlank(clazz, scope, memberName)) {
            return null;
        }
        try {
            if(ElementType.FIELD == scope) {
                Field field = clazz.getDeclaredField(memberName);
                field.setAccessible(true);
                return field.getDeclaredAnnotations();
            } else if(ElementType.METHOD == scope) {
                Method method = clazz.getDeclaredMethod(memberName);
                method.setAccessible(true);
                return method.getDeclaredAnnotations();
            } else if(ElementType.TYPE == scope) {
                return clazz.getDeclaredAnnotations();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 获取Field指定的annotation
     * @param clazz
     * @param fieldName
     * @param annoClass
     * @return
     */
    public static<T extends Annotation> Annotation getFieldAnnotation(Class clazz, String fieldName, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.FIELD, fieldName, annoClass);
    }

    /**
     * 获取Field所有annotation
     * @param clazz
     * @param fieldName
     * @return
     */
    public static Annotation[] getFieldAnnotations(Class clazz, String fieldName) {
        return getAnnotations(clazz, ElementType.FIELD, fieldName);
    }

    /**
     * 获取Method指定的annotation
     * @param clazz
     * @param methodName
     * @param annoClass
     * @return
     */
    public static<T extends Annotation> Annotation getMethodAnnotation(Class clazz, String methodName, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.METHOD, methodName, annoClass);
    }

    /**
     * 获取Method所有annotation
     * @param clazz
     * @param methodName
     * @return
     */
    public static Annotation[] getMethodAnnotations(Class clazz, String methodName) {
        return getAnnotations(clazz, ElementType.METHOD, methodName);
    }

    /**
     * 获取类指定annotation
     * @param clazz
     * @param annoClass
     * @return
     */
    public static<T extends Annotation> Annotation getClassAnnotation(Class clazz, Class<T> annoClass) {
        return getAnnotation(clazz, ElementType.TYPE, clazz.getName(), annoClass);
    }

    /**
     * 获取类所有annotation
     * @param clazz
     * @return
     */
    public static Annotation[] getClassAnnotations(Class clazz) {
        return getAnnotations(clazz, ElementType.TYPE, clazz.getName());
    }

    /**
     * 获取方法所有参数的所有annotation
     * @param clazz
     * @param methodName
     * @return
     */
    public static Annotation[][] getParamAnnotations(Class clazz, String methodName, Class<?>... parameterTypes) {
        if(ParamUtils.isBlank(clazz, methodName)) {
            return null;
        }
        try {
            Method method = clazz.getDeclaredMethod(methodName, parameterTypes);
            method.setAccessible(true);
            return method.getParameterAnnotations();
        } catch(Exception e) {
            e.printStackTrace();
        }
        return null;
    }

    /**
     * 通过包名获取包内所有类
     *
     * @param packageName
     * @return
     */
    public static List<Class<?>> getAllClassByPackageName(Package packageName) {
        if(packageName == null) {
            throw new IllegalArgumentException("包不能为空");
        }
        return getAllClassByPackageName(packageName.getName());
    }

    /**
     * 通过包名获取包内所有类
     *
     * @param packageName
     * 包名
     * @return
     */
    public static List<Class<?>> getAllClassByPackageName(String packageName) {
        if(StringUtils.isBlank(packageName)) {
            throw new IllegalArgumentException("包名不能为空");
        }
        // 获取当前包下以及子包下所以的类
        List<Class<?>> returnClassList = getClasses(packageName);
        return returnClassList;
    }

    /**
     * 通过接口名取得某个接口下所有实现这个接口的类
     */
    public static List<Class<?>> getAllClassByInterface(Class<?> c) {
        List<Class<?>> returnClassList = null;

        if (c.isInterface()) {
            // 获取当前的包名
            String packageName = c.getPackage().getName();
            // 获取当前包下以及子包下所以的类
            List<Class<?>> allClass = getClasses(packageName);
            if (allClass != null) {
                returnClassList = new ArrayList<Class<?>>();
                for (Class<?> cls : allClass) {
                    // 判断是否是同一个接口
                    if (c.isAssignableFrom(cls)) {
                        // 本身不加入进去
                        if (!c.equals(cls)) {
                            returnClassList.add(cls);
                        }
                    }
                }
            }
        }

        return returnClassList;
    }

    /**
     * 取得某一类所在包的所有类名 不含迭代
     */
    public static String[] getPackageAllClassName(String classLocation, String packageName) {
        // 将packageName分解
        String[] packagePathSplit = packageName.split("[.]");
        String realClassLocation = classLocation;
        int packageLength = packagePathSplit.length;
        for (int i = 0; i < packageLength; i++) {
            realClassLocation = realClassLocation + File.separator + packagePathSplit[i];
        }
        File packeageDir = new File(realClassLocation);
        if (packeageDir.isDirectory()) {
            String[] allClassName = packeageDir.list();
            return allClassName;
        }
        return null;
    }

    /**
     * 从包package中获取所有的Class
     *
     * @param packageName
     * 包名
     * @return
     */
    private static List<Class<?>> getClasses(String packageName) {
        // 第一个class类的集合
        List<Class<?>> classes = new ArrayList<Class<?>>();
        // 是否循环迭代
        boolean recursive = true;
        // 获取包的名字 并进行替换
        String packageDirName = packageName.replace('.', '/');
        // 定义一个枚举的集合 并进行循环来处理这个目录下的things
        Enumeration<URL> dirs;
        try {
            dirs = Thread.currentThread().getContextClassLoader().getResources(packageDirName);
            // 循环迭代下去
            while (dirs.hasMoreElements()) {
                // 获取下一个元素
                URL url = dirs.nextElement();
                // 得到协议的名称
                String protocol = url.getProtocol();
                // 如果是以文件的形式保存在服务器上
                if ("file".equals(protocol)) {
                    // 获取包的物理路径
                    String filePath = URLDecoder.decode(url.getFile(), "UTF-8");
                    // 以文件的方式扫描整个包下的文件 并添加到集合中
                    findAndAddClassesInPackageByFile(packageName, filePath, recursive, classes);
                } else if ("jar".equals(protocol)) {
                    // 如果是jar包文件
                    // 定义一个JarFile
                    JarFile jar;
                    try {
                        // 获取jar
                        jar = ((JarURLConnection) url.openConnection()).getJarFile();
                        // 从此jar包 得到一个枚举类
                        Enumeration<JarEntry> entries = jar.entries();
                        // 同样的进行循环迭代
                        while (entries.hasMoreElements()) {
                            // 获取jar里的一个实体 可以是目录 和一些jar包里的其他文件 如META-INF等文件
                            JarEntry entry = entries.nextElement();
                            String name = entry.getName();
                            // 如果是以/开头的
                            if (name.charAt(0) == '/') {
                                // 获取后面的字符串
                                name = name.substring(1);
                            }
                            // 如果前半部分和定义的包名相同
                            if (name.startsWith(packageDirName)) {
                                int idx = name.lastIndexOf('/');
                                // 如果以"/"结尾 是一个包
                                if (idx != -1) {
                                    // 获取包名 把"/"替换成"."
                                    packageName = name.substring(0, idx).replace('/', '.');
                                }
                                // 如果可以迭代下去 并且是一个包
                                if ((idx != -1) || recursive) {
                                    // 如果是一个.class文件 而且不是目录
                                    if (name.endsWith(".class") && !entry.isDirectory()) {
                                        // 去掉后面的".class" 获取真正的类名
                                        String className = name.substring(packageName.length() + 1, name.length() - 6);
                                        try {
                                            // 添加到classes
                                            classes.add(Class.forName(packageName + '.' + className));
                                        } catch (ClassNotFoundException e) {
                                            e.printStackTrace();
                                        }
                                    }
                                }
                            }
                        }
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return classes;
    }

    /**
     * 以文件的形式来获取包下的所有Class
     *
     * @param packageName
     * @param packagePath
     * @param recursive
     * @param classes
     */
    private static void findAndAddClassesInPackageByFile(String packageName, String packagePath, final boolean recursive, List<Class<?>> classes) {
        // 获取此包的目录 建立一个File
        File dir = new File(packagePath);
        // 如果不存在或者 也不是目录就直接返回
        if (!dir.exists() || !dir.isDirectory()) {
            return;
        }
        // 如果存在 就获取包下的所有文件 包括目录
        File[] dirfiles = dir.listFiles(new FileFilter() {
            // 自定义过滤规则 如果可以循环(包含子目录) 或则是以.class结尾的文件(编译好的java类文件)
            public boolean accept(File file) {
                return (recursive && file.isDirectory()) || (file.getName().endsWith(".class"));
            }
        });
        // 循环所有文件
        for (File file : dirfiles) {
            // 如果是目录 则继续扫描
            if (file.isDirectory()) {
                findAndAddClassesInPackageByFile(packageName + "." + file.getName(), file.getAbsolutePath(), recursive, classes);
            } else {
                // 如果是java类文件 去掉后面的.class 只留下类名
                String className = file.getName().substring(0, file.getName().length() - 6);
                try {
                    // 添加到集合中去
                    classes.add(Class.forName(packageName + '.' + className));
                } catch (ClassNotFoundException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 扫描指定包下所有包含指定annotation的类
     * 如果类、field或method有指定的annotation，则认为匹配成功
     * @param packageName
     * 包名
     * @param annoClass
     * 使用的annotation
     * @return
     */
    public static List<Class<?>> scanAnnotation(Package packageName, Class<? extends Annotation> annoClass) {
        return scanAnnotation(packageName.getName(), annoClass);
    }

    /**
     * 扫描指定包下所有包含指定annotation的类
     * 如果类、field或method有指定的annotation，则认为匹配成功
     * @param packageName
     * 包名
     * @param annoClass
     * 使用的annotation
     * @return
     */
    public static List<Class<?>> scanAnnotation(String packageName, Class<? extends Annotation> annoClass) {
        if(StringUtils.isBlank(packageName) || annoClass == null) {
            throw new IllegalArgumentException("参数不合法");
        }
        List<Class<?>> classList = getAllClassByPackageName(packageName);
        if(classList == null) {
            return Collections.emptyList();
        }

        List<Class<?>> annoClassList = new LinkedList<>();
        for(Class<?> clazz : classList) {
            if(clazz != null) {
                if(clazz.getAnnotation(annoClass) != null) {
                    // 类上声明了该annotation
                    annoClassList.add(clazz);
                } else {
                    // field中了annotation
                    Field[] fields = clazz.getDeclaredFields();
                    if(fields != null && fields.length > 0) {
                        for(Field field : fields) {
                            field.setAccessible(true);
                            if(field.getAnnotation(annoClass) != null) {
                                // 如果field中有指定的annotation
                                annoClassList.add(clazz);
                            }
                        }
                    }

                    // method上声明了annotation
                    Method[] methods = clazz.getDeclaredMethods();
                    if(methods != null && methods.length > 0) {
                        for(Method method : methods) {
                            method.setAccessible(true);
                            if(method.getAnnotation(annoClass) != null) {
                                // 如果method中有指定的annotation
                                annoClassList.add(clazz);
                            }
                        }
                    }
                }
            }
        }
        return annoClassList;
    }
}
