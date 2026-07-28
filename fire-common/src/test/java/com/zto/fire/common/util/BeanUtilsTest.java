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
import java.util.List;

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
}
