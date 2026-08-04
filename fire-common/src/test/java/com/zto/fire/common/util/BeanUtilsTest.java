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

import org.junit.Assert;
import org.junit.Test;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * {@link BeanUtils} 单元测试
 *
 * @author ChengLong
 * @since 3.0.0
 */
public class BeanUtilsTest {

    @Test
    public void testCopySameNameSameType() {
        SourceBean source = new SourceBean();
        source.setName("fire");
        source.setAge(18);
        source.setActive(true);

        TargetBean target = new TargetBean();
        target.setName("old");
        target.setAge(1);
        target.setActive(false);

        BeanUtils.copyProperties(source, target);

        Assert.assertEquals("fire", target.getName());
        Assert.assertEquals(18, target.getAge());
        Assert.assertTrue(target.isActive());
    }

    @Test
    public void testCopyPrimitiveAndWrapper() {
        WrapperSource source = new WrapperSource();
        source.setCount(Integer.valueOf(9));
        source.setFlag(Boolean.TRUE);

        PrimitiveTarget target = new PrimitiveTarget();
        target.setCount(0);
        target.setFlag(false);

        BeanUtils.copyProperties(source, target);

        Assert.assertEquals(9, target.getCount());
        Assert.assertTrue(target.isFlag());
    }

    @Test
    public void testCopyPrimitiveToWrapper() {
        PrimitiveTarget source = new PrimitiveTarget();
        source.setCount(7);
        source.setFlag(true);

        WrapperSource target = new WrapperSource();
        BeanUtils.copyProperties(source, target);

        Assert.assertEquals(Integer.valueOf(7), target.getCount());
        Assert.assertEquals(Boolean.TRUE, target.getFlag());
    }

    @Test
    public void testCopyAssignableSubtype() {
        SubtypeSource source = new SubtypeSource();
        source.setValue(new ChildValue("v1"));

        ParentTypeTarget target = new ParentTypeTarget();
        BeanUtils.copyProperties(source, target);

        Assert.assertNotNull(target.getValue());
        Assert.assertEquals("v1", target.getValue().getName());
        Assert.assertTrue(target.getValue() instanceof ChildValue);
    }

    @Test
    public void testSkipIncompatibleAndMissing() {
        SourceBean source = new SourceBean();
        source.setName("keep-me");
        source.setAge(3);

        PartialTarget target = new PartialTarget();
        target.setName("origin");
        target.setExtra("untouched");
        // age 类型不兼容：source int，target String，不应覆盖
        target.setAge("should-remain");

        BeanUtils.copyProperties(source, target);

        Assert.assertEquals("keep-me", target.getName());
        Assert.assertEquals("should-remain", target.getAge());
        Assert.assertEquals("untouched", target.getExtra());
    }

    @Test
    public void testCopyToNewInstance() {
        SourceBean source = new SourceBean();
        source.setName("new-inst");
        source.setAge(20);
        source.setActive(false);

        TargetBean target = BeanUtils.copyProperties(source, TargetBean.class);
        Assert.assertNotNull(target);
        Assert.assertEquals("new-inst", target.getName());
        Assert.assertEquals(20, target.getAge());
        Assert.assertFalse(target.isActive());
    }

    @Test
    public void testCacheReuse() {
        SourceBean source = new SourceBean();
        source.setName("cached");
        source.setAge(1);

        TargetBean first = new TargetBean();
        TargetBean second = new TargetBean();
        BeanUtils.copyProperties(source, first);
        BeanUtils.copyProperties(source, second);

        Assert.assertEquals("cached", first.getName());
        Assert.assertEquals("cached", second.getName());
        Assert.assertEquals(1, first.getAge());
        Assert.assertEquals(1, second.getAge());
    }

    @Test(expected = NullPointerException.class)
    public void testNullSource() {
        BeanUtils.copyProperties(null, new TargetBean());
    }

    /**
     * NamedValueSource：按属性精确名从 Map 取值并写入 target setter
     */
    @Test
    public void testNamedValueCopySameName() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "fire");
        map.put("age", Integer.valueOf(18));
        map.put("active", Boolean.TRUE);

        TargetBean target = BeanUtils.copyProperties(map::get, TargetBean.class);

        Assert.assertEquals("fire", target.getName());
        Assert.assertEquals(18, target.getAge());
        Assert.assertTrue(target.isActive());
    }

    /**
     * NamedValueSource：包装类型拆箱写入基本类型 setter
     */
    @Test
    public void testNamedValueWrapperToPrimitive() {
        Map<String, Object> map = new HashMap<>();
        map.put("count", Integer.valueOf(9));
        map.put("flag", Boolean.TRUE);

        PrimitiveTarget target = new PrimitiveTarget();
        BeanUtils.copyProperties(map::get, target);

        Assert.assertEquals(9, target.getCount());
        Assert.assertTrue(target.isFlag());
    }

    /**
     * NamedValueSource：null 不覆盖 primitive（跳过赋值，保留默认值）
     */
    @Test
    public void testNamedValueNullSkipsPrimitive() {
        Map<String, Object> map = new HashMap<>();
        map.put("count", null);
        map.put("flag", null);
        map.put("name", "ok");

        NamedPrimitiveTarget target = new NamedPrimitiveTarget();
        target.setCount(42);
        target.setFlag(true);
        target.setName("old");

        BeanUtils.copyProperties(map::get, target);

        Assert.assertEquals(42, target.getCount());
        Assert.assertTrue(target.isFlag());
        Assert.assertEquals("ok", target.getName());
    }

    /**
     * NamedValueSource：null 可写入引用类型 setter
     */
    @Test
    public void testNamedValueNullClearsReference() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", null);

        TargetBean target = new TargetBean();
        target.setName("old");
        BeanUtils.copyProperties(map::get, target);

        Assert.assertNull(target.getName());
    }

    /**
     * NamedValueSource：ignoreCaseAndUnderline 重载可用，首版仍按精确属性名 getObject
     */
    @Test
    public void testNamedValueIgnoreCaseAndUnderlineUsesExactName() {
        Map<String, Object> map = new HashMap<>();
        map.put("userName", "alice");
        map.put("user_name", "should-not-be-used");

        CamelTarget target = BeanUtils.copyProperties(map::get, CamelTarget.class, true);

        Assert.assertEquals("alice", target.getUserName());
    }

    @Test
    public void testNamedValueCopyToExistingTarget() {
        Map<String, Object> map = new HashMap<>();
        map.put("name", "existing");
        map.put("age", Integer.valueOf(5));

        TargetBean target = new TargetBean();
        BeanUtils.copyProperties(map::get, target);

        Assert.assertEquals("existing", target.getName());
        Assert.assertEquals(5, target.getAge());
    }

    /**
     * 默认 false：属性名不完全一致时不拷贝（与历史行为一致）
     */
    @Test
    public void testIgnoreCaseAndUnderlineDefaultOff() {
        SnakeSource source = new SnakeSource();
        source.setUser_name("alice");
        source.setUSER("u1");
        source.setUSER_NAME("should-not-affect-userName-when-off");

        CamelTarget target = new CamelTarget();
        target.setUserName("keep");
        target.setUser("keep-user");

        BeanUtils.copyProperties(source, target);

        Assert.assertEquals("keep", target.getUserName());
        Assert.assertEquals("keep-user", target.getUser());
    }

    /**
     * ignoreCaseAndUnderline=true：下划线字段与驼峰字段互相拷贝
     */
    @Test
    public void testCopySnakeToCamel() {
        SnakeSource source = new SnakeSource();
        source.setUser_name("alice");
        source.setAge_value(30);

        CamelTarget target = new CamelTarget();
        BeanUtils.copyProperties(source, target, true);

        Assert.assertEquals("alice", target.getUserName());
        Assert.assertEquals(30, target.getAgeValue());
    }

    /**
     * ignoreCaseAndUnderline=true：纯大写 / 大写+下划线 与驼峰/小写视为同一字段
     */
    @Test
    public void testCopyUpperCaseToCamel() {
        UpperSource source = new UpperSource();
        source.setUSER("bob");
        source.setUSER_NAME("bob-full");

        CamelTarget target = new CamelTarget();
        BeanUtils.copyProperties(source, target, true);

        Assert.assertEquals("bob", target.getUser());
        Assert.assertEquals("bob-full", target.getUserName());
    }

    /**
     * ignoreCaseAndUnderline=true：驼峰 → 下划线
     */
    @Test
    public void testCopyCamelToSnake() {
        CamelTarget source = new CamelTarget();
        source.setUserName("carol");
        source.setAgeValue(22);
        source.setUser("c");

        SnakeTarget target = new SnakeTarget();
        BeanUtils.copyProperties(source, target, true);

        Assert.assertEquals("carol", target.getUser_name());
        Assert.assertEquals(22, target.getAge_value());
        Assert.assertEquals("c", target.getUSER());
    }

    /**
     * 开启兼容匹配时，同名字段仍正常拷贝，且与命名兼容字段可同时生效
     */
    @Test
    public void testFlexibleMatchWithExactName() {
        MixedSource source = new MixedSource();
        source.setName("exact");
        source.setUser_name("flex");

        MixedTarget target = new MixedTarget();
        BeanUtils.copyProperties(source, target, true);

        Assert.assertEquals("exact", target.getName());
        Assert.assertEquals("flex", target.getUserName());
    }

    /**
     * 创建新实例的重载同样支持 ignoreCaseAndUnderline
     */
    @Test
    public void testCopyToNewInstanceWithFlexibleMatch() {
        SnakeSource source = new SnakeSource();
        source.setUser_name("dave");
        source.setAge_value(40);

        CamelTarget target = BeanUtils.copyProperties(source, CamelTarget.class, true);
        Assert.assertNotNull(target);
        Assert.assertEquals("dave", target.getUserName());
        Assert.assertEquals(40, target.getAgeValue());
    }

    /**
     * 精确匹配优先：source 同时存在 userName 与 user_name 时，同名 userName 优先生效
     */
    @Test
    public void testExactNamePreferredOverNormalized() {
        AmbiguousSource source = new AmbiguousSource();
        source.setUserName("exact-win");
        source.setUser_name("normalized-lose");

        CamelTarget target = new CamelTarget();
        BeanUtils.copyProperties(source, target, true);

        Assert.assertEquals("exact-win", target.getUserName());
    }

    /**
     * 性能对比：Fire BeanUtils（ByteBuddy）vs ReflectionUtils 反射字段拷贝
     * 使用独立的 100 字段 PerfSourceBean/PerfTargetBean（不影响功能单测中的 SourceBean/TargetBean）
     * 先测 BeanUtils，减少后续反射大循环 GC 对前面结果的干扰
     */
    @Test
    public void testCopyPerformanceReflectionVsBeanUtils() throws Exception {
        final int count = 10_00000;
        final int fieldCount = PerfSourceBean.FIELD_COUNT;
        List<PerfSourceBean> sources = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            PerfSourceBean source = new PerfSourceBean();
            PerfSourceBean.fill(source, i);
            sources.add(source);
        }

        // 预热：BeanCopier 字节码生成 + 反射字段缓存 + JIT
        PerfSourceBean warmSrc = sources.get(0);
        PerfTargetBean warmDest = new PerfTargetBean();
        for (int i = 0; i < 200; i++) {
            BeanUtils.copyProperties(warmSrc, warmDest);
            copyByReflection(warmSrc, warmDest);
        }

        // 1) Fire BeanUtils.copyProperties（先测，减少 GC 干扰）
        List<PerfTargetBean> beanUtilsTargets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            beanUtilsTargets.add(new PerfTargetBean());
        }
        long beanUtilsStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            BeanUtils.copyProperties(sources.get(i), beanUtilsTargets.get(i));
        }
        long beanUtilsCostMs = (System.nanoTime() - beanUtilsStart) / 1_000_000L;

        // 2) ReflectionUtils 反射字段拷贝（对齐业务侧 getAllFields + field.set 模式）
        List<PerfTargetBean> reflectionTargets = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            reflectionTargets.add(new PerfTargetBean());
        }
        long reflectionStart = System.nanoTime();
        for (int i = 0; i < count; i++) {
            copyByReflection(sources.get(i), reflectionTargets.get(i));
        }
        long reflectionCostMs = (System.nanoTime() - reflectionStart) / 1_000_000L;

        // 正确性抽检，避免优化被编译掉
        PerfSourceBean lastSrc = sources.get(count - 1);
        Assert.assertEquals(lastSrc.getField00(), beanUtilsTargets.get(count - 1).getField00());
        Assert.assertEquals(lastSrc.getField01(), beanUtilsTargets.get(count - 1).getField01());
        Assert.assertEquals(lastSrc.getField99(), beanUtilsTargets.get(count - 1).getField99());
        Assert.assertEquals(lastSrc.getField00(), reflectionTargets.get(count - 1).getField00());
        Assert.assertEquals(lastSrc.getField01(), reflectionTargets.get(count - 1).getField01());
        Assert.assertEquals(lastSrc.getField99(), reflectionTargets.get(count - 1).getField99());

        double vsReflection = beanUtilsCostMs == 0
                ? Double.POSITIVE_INFINITY
                : (double) reflectionCostMs / beanUtilsCostMs;
        String report = String.format(
                "%n========== Bean copy 性能对比（%d 次, %d 字段）==========%n"
                        + "Fire BeanUtils.copyProperties : %d ms%n"
                        + "ReflectionUtils 反射字段拷贝   : %d ms%n"
                        + "Fire 相对反射加速约           : %.2fx%n"
                        + "=================================================%n",
                count, fieldCount, beanUtilsCostMs, reflectionCostMs, vsReflection);
        System.out.println(report);

        Assert.assertTrue("beanUtils cost should be non-negative", beanUtilsCostMs >= 0);
        Assert.assertTrue("reflection cost should be non-negative", reflectionCostMs >= 0);
    }

    /**
     * 模拟业务侧：ReflectionUtils.getAllFields + field.setAccessible + field.get/set 做同名字段拷贝
     */
    private static void copyByReflection(Object source, Object target) throws Exception {
        Class<?> sourceClass = source.getClass();
        Class<?> targetClass = target.getClass();
        Collection<Field> targetFields = ReflectionUtils.getAllFields(targetClass).values();
        for (Field targetField : targetFields) {
            if (java.lang.reflect.Modifier.isStatic(targetField.getModifiers())) {
                continue;
            }
            Field sourceField = ReflectionUtils.getFieldByName(sourceClass, targetField.getName());
            if (sourceField == null) {
                continue;
            }
            ReflectionUtils.setAccessible(sourceField);
            ReflectionUtils.setAccessible(targetField);
            Object value = sourceField.get(source);
            targetField.set(target, value);
        }
    }

    public static class SourceBean {
        private String name;
        private int age;
        private boolean active;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }
    }

    public static class TargetBean {
        private String name;
        private int age;
        private boolean active;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public int getAge() {
            return age;
        }

        public void setAge(int age) {
            this.age = age;
        }

        public boolean isActive() {
            return active;
        }

        public void setActive(boolean active) {
            this.active = active;
        }
    }

    public static class WrapperSource {
        private Integer count;
        private Boolean flag;

        public Integer getCount() {
            return count;
        }

        public void setCount(Integer count) {
            this.count = count;
        }

        public Boolean getFlag() {
            return flag;
        }

        public void setFlag(Boolean flag) {
            this.flag = flag;
        }
    }

    public static class PrimitiveTarget {
        private int count;
        private boolean flag;

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public boolean isFlag() {
            return flag;
        }

        public void setFlag(boolean flag) {
            this.flag = flag;
        }
    }

    /** NamedValue 用例：同时含 primitive 与引用类型 */
    public static class NamedPrimitiveTarget {
        private int count;
        private boolean flag;
        private String name;

        public int getCount() {
            return count;
        }

        public void setCount(int count) {
            this.count = count;
        }

        public boolean isFlag() {
            return flag;
        }

        public void setFlag(boolean flag) {
            this.flag = flag;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class ParentValue {
        private String name;

        public ParentValue() {
        }

        public ParentValue(String name) {
            this.name = name;
        }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }
    }

    public static class ChildValue extends ParentValue {
        public ChildValue() {
        }

        public ChildValue(String name) {
            super(name);
        }
    }

    public static class SubtypeSource {
        private ChildValue value;

        public ChildValue getValue() {
            return value;
        }

        public void setValue(ChildValue value) {
            this.value = value;
        }
    }

    public static class ParentTypeTarget {
        private ParentValue value;

        public ParentValue getValue() {
            return value;
        }

        public void setValue(ParentValue value) {
            this.value = value;
        }
    }

    public static class PartialTarget {
        private String name;
        private String age;
        private String extra;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getAge() {
            return age;
        }

        public void setAge(String age) {
            this.age = age;
        }

        public String getExtra() {
            return extra;
        }

        public void setExtra(String extra) {
            this.extra = extra;
        }
    }

    /** 下划线命名源 Bean */
    public static class SnakeSource {
        private String user_name;
        private int age_value;
        private String USER;
        private String USER_NAME;

        public String getUser_name() {
            return user_name;
        }

        public void setUser_name(String user_name) {
            this.user_name = user_name;
        }

        public int getAge_value() {
            return age_value;
        }

        public void setAge_value(int age_value) {
            this.age_value = age_value;
        }

        public String getUSER() {
            return USER;
        }

        public void setUSER(String USER) {
            this.USER = USER;
        }

        public String getUSER_NAME() {
            return USER_NAME;
        }

        public void setUSER_NAME(String USER_NAME) {
            this.USER_NAME = USER_NAME;
        }
    }

    /** 纯大写命名源 Bean */
    public static class UpperSource {
        private String USER;
        private String USER_NAME;

        public String getUSER() {
            return USER;
        }

        public void setUSER(String USER) {
            this.USER = USER;
        }

        public String getUSER_NAME() {
            return USER_NAME;
        }

        public void setUSER_NAME(String USER_NAME) {
            this.USER_NAME = USER_NAME;
        }
    }

    /** 驼峰命名目标 Bean */
    public static class CamelTarget {
        private String userName;
        private int ageValue;
        private String user;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public int getAgeValue() {
            return ageValue;
        }

        public void setAgeValue(int ageValue) {
            this.ageValue = ageValue;
        }

        public String getUser() {
            return user;
        }

        public void setUser(String user) {
            this.user = user;
        }
    }

    /** 下划线 / 大写命名目标 Bean */
    public static class SnakeTarget {
        private String user_name;
        private int age_value;
        private String USER;

        public String getUser_name() {
            return user_name;
        }

        public void setUser_name(String user_name) {
            this.user_name = user_name;
        }

        public int getAge_value() {
            return age_value;
        }

        public void setAge_value(int age_value) {
            this.age_value = age_value;
        }

        public String getUSER() {
            return USER;
        }

        public void setUSER(String USER) {
            this.USER = USER;
        }
    }

    public static class MixedSource {
        private String name;
        private String user_name;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUser_name() {
            return user_name;
        }

        public void setUser_name(String user_name) {
            this.user_name = user_name;
        }
    }

    public static class MixedTarget {
        private String name;
        private String userName;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }
    }

    /** 同时含同名与归一化等价字段，用于验证精确匹配优先 */
    public static class AmbiguousSource {
        private String userName;
        private String user_name;

        public String getUserName() {
            return userName;
        }

        public void setUserName(String userName) {
            this.userName = userName;
        }

        public String getUser_name() {
            return user_name;
        }

        public void setUser_name(String user_name) {
            this.user_name = user_name;
        }
    }
}
