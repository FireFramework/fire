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

/**
 * {@link NamedValueSource} 路径下基本类型赋值的拆箱辅助方法
 * <p>
 * 放在普通 Java 类中由 javac 生成带 stackmap 的字节码，供 ByteBuddy 生成的 Copier 直接
 * {@code invokestatic} 调用，从而避免在动态字节码里手写 {@code ifnull} 分支（易触发 VerifyError）。
 * </p>
 * <p>
 * 语义：值为 {@code null} 时返回 {@code whenNull}（通常为 target 当前值），实现「null 不覆盖 primitive」。
 * </p>
 *
 * @author ChengLong
 * @since 3.0.0
 */
public final class NamedValueAssigns {

    private NamedValueAssigns() {
    }

    public static boolean toBoolean(Object value, boolean whenNull) {
        return value == null ? whenNull : (Boolean) value;
    }

    public static byte toByte(Object value, byte whenNull) {
        return value == null ? whenNull : ((Number) value).byteValue();
    }

    public static short toShort(Object value, short whenNull) {
        return value == null ? whenNull : ((Number) value).shortValue();
    }

    public static int toInt(Object value, int whenNull) {
        return value == null ? whenNull : ((Number) value).intValue();
    }

    public static long toLong(Object value, long whenNull) {
        return value == null ? whenNull : ((Number) value).longValue();
    }

    public static float toFloat(Object value, float whenNull) {
        return value == null ? whenNull : ((Number) value).floatValue();
    }

    public static double toDouble(Object value, double whenNull) {
        return value == null ? whenNull : ((Number) value).doubleValue();
    }

    public static char toChar(Object value, char whenNull) {
        return value == null ? whenNull : (Character) value;
    }

    /**
     * 按 primitive 类型返回对应的拆箱方法名（用于生成 {@code invokestatic}）
     */
    static String methodName(Class<?> primitiveType) {
        if (primitiveType == Boolean.TYPE) {
            return "toBoolean";
        }
        if (primitiveType == Byte.TYPE) {
            return "toByte";
        }
        if (primitiveType == Short.TYPE) {
            return "toShort";
        }
        if (primitiveType == Integer.TYPE) {
            return "toInt";
        }
        if (primitiveType == Long.TYPE) {
            return "toLong";
        }
        if (primitiveType == Float.TYPE) {
            return "toFloat";
        }
        if (primitiveType == Double.TYPE) {
            return "toDouble";
        }
        if (primitiveType == Character.TYPE) {
            return "toChar";
        }
        throw new IllegalArgumentException("Not a supported primitive: " + primitiveType);
    }
}
