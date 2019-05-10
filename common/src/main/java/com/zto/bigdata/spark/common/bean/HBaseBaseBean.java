package com.zto.bigdata.spark.common.bean;

import com.zto.bigdata.spark.common.anno.FieldName;
import com.zto.bigdata.spark.common.util.GlobalConstants;
import com.zto.bigdata.spark.common.util.ReflectionUtils;
import org.apache.commons.lang.StringUtils;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.Map;

/**
 * HBase封装bean需实现该接口
 * Created by ChengLong on 2017-03-27.
 */
public abstract class HBaseBaseBean<T> implements Serializable {
    /**
     * rowkey字段
     */
    @FieldName(value = "rowKey", disuse = true)
    public String rowKey;

    /**
     * 子类包名+类名
     */
    @FieldName(value = "className", disuse = true)
    public final String className = this.getClass().getSimpleName();

    /**
     * 构建业务需要，构建rowkey
     *
     * @return
     */
    public abstract T buildRowKey();

    /**
     * 构建Hive与HBase的Mapping HQL
     *
     * @return
     */
    public String hive2HBaseMapping(String tableName) {
        Map<String, Field> fieldMap = ReflectionUtils.getAllFields(this.getClass());
        StringBuilder hql = new StringBuilder("CREATE EXTERNAL TABLE " + tableName + "_mapping(\n");
        StringBuilder hiveStr = new StringBuilder("\tkey string,\n");
        StringBuilder hbaseStr = new StringBuilder("");

        for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            String fieldName = entry.getKey();
            Field field = entry.getValue();
            FieldName anno =  field.getAnnotation(FieldName.class);
            String familyName = GlobalConstants.familyName();
            if(anno != null) {
                if(!anno.mapping()) {
                    continue;
                }
                fieldName = anno.value();
                if(StringUtils.isNotBlank(anno.family())) {
                    familyName = anno.family();
                }
            }

            Type fieldType = field.getType();
            if (fieldType == String.class) {
                hiveStr.append("\t" + fieldName + " string,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + ",\n");
            } else if (fieldType == Integer.class) {
                hiveStr.append("\t" + fieldName + " int,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            } else if (fieldType == Double.class) {
                hiveStr.append("\t" + fieldName + " double,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            } else if (fieldType == Long.class) {
                hiveStr.append("\t" + fieldName + " bigint,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            } else if (fieldType == BigDecimal.class) {
                hiveStr.append("\t" + fieldName + " decimal(38,18),\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            } else if (fieldType == Float.class) {
                hiveStr.append("\t" + fieldName + " float,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            } else if (fieldType == Boolean.class) {
                hiveStr.append("\t" + fieldName + " boolean,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            } else if (fieldType == Short.class) {
                hiveStr.append("\t" + fieldName + " smallint,\n");
                hbaseStr.append("\t" + familyName + ":" + fieldName + "#b,\n");
            }
        }
        hql.append(hiveStr.substring(0, hiveStr.length() - 2) + "\n");
        hql.append(") STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler' \n");
        hql.append(" WITH SERDEPROPERTIES \n");
        hql.append("(\"hbase.columns.mapping\" = \":key,\n");
        hql.append(hbaseStr.substring(0, hbaseStr.length() - 2) + "\n");
        hql.append(" \")TBLPROPERTIES(\"hbase.table.name\" = \"" + tableName + "\")");

        return hql.toString();
    }

    /**
     * 构建Hive与HBase的Mapping HQL
     *
     * @return
     */
    public String hiveCreateSql(String tableName, String ... partitions) {
        Map<String, Field> fieldMap = ReflectionUtils.getAllFields(this.getClass());
        StringBuilder hql = new StringBuilder("CREATE TABLE " + tableName + "(\n");
        StringBuilder hiveStr = new StringBuilder("");

        for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
            String fieldName = entry.getKey();
            Field field = entry.getValue();
            FieldName anno =  field.getAnnotation(FieldName.class);
            String familyName = GlobalConstants.familyName();
            if(anno != null) {
                if(!anno.mapping()) {
                    continue;
                }
                fieldName = anno.value();
                if(StringUtils.isNotBlank(anno.family())) {
                    familyName = anno.family();
                }
            }

            Type fieldType = field.getType();
            if (fieldType == String.class) {
                hiveStr.append("\t" + fieldName + " string,\n");
            } else if (fieldType == Integer.class) {
                hiveStr.append("\t" + fieldName + " int,\n");
            } else if (fieldType == Double.class) {
                hiveStr.append("\t" + fieldName + " double,\n");
            } else if (fieldType == Long.class) {
                hiveStr.append("\t" + fieldName + " bigint,\n");
            } else if (fieldType == BigDecimal.class) {
                hiveStr.append("\t" + fieldName + " decimal(38,18),\n");
            } else if (fieldType == Float.class) {
                hiveStr.append("\t" + fieldName + " float,\n");
            } else if (fieldType == Boolean.class) {
                hiveStr.append("\t" + fieldName + " boolean,\n");
            } else if (fieldType == Short.class) {
                hiveStr.append("\t" + fieldName + " smallint,\n");
            }
        }
        hql.append(hiveStr.substring(0, hiveStr.length() - 2) + ") \n");
        if(partitions != null && partitions.length > 0) {
            hql.append("PARTITIONED BY ( ");
            StringBuilder tmp = new StringBuilder("");
            for(String partition : partitions) {
                tmp.append(partition + ",");
            }
            hql.append(tmp.substring(0, tmp.length() - 1));
            hql.append(")\n");
        }
        hql.append("stored as textfile \n");

        return hql.toString();
    }

    public String getRowKey() {
        return rowKey;
    }

    public void setRowKey(String rowKey) {
        this.rowKey = rowKey;
    }

    /**
     * 根据JavaBean构建catalog
     *
     * @param bean 继承自HBaseBaseBean的JavaBean
     * @param <T>
     * @return catalog
     */
    public static <T extends HBaseBaseBean<T>> String catalog(HBaseBaseBean<T> bean) {
        if (bean == null) {
            throw new IllegalArgumentException("参数不能为空");
        }
        StringBuilder catalog = new StringBuilder("{\n");
        try {
            Class clazz = bean.getClass();
            FieldName fieldName = (FieldName) clazz.getAnnotation(FieldName.class);
            if (fieldName == null) {
                throw new RuntimeException("JavaBean中未发现@FieldName");
            }
            if (StringUtils.isBlank(fieldName.tableName()) || StringUtils.isBlank(fieldName.tableName())) {
                throw new RuntimeException("请使用@FieldName指定namespace或tableName");
            }
            // 拼接表空间和表名
            catalog.append("\"table\":{\"namespace\":\"" + fieldName.namespace() + "\", \"name\":\"" + fieldName.tableName() + "\"},\n");
            // 拼接rowkey信息
            if(StringUtils.isBlank(bean.rowKey)) {
                clazz.getMethod("buildRowKey").invoke(bean);
            }
            catalog.append("\"rowkey\":\"rowKey\",\n");
            // 拼接字段信息
            catalog.append("\"columns\":{\n");

            StringBuilder fieldStr = new StringBuilder("");
            Map<String, Field> fieldMap = ReflectionUtils.getAllFields(clazz);
            for (Map.Entry<String, Field> entry : fieldMap.entrySet()) {
                String fName = entry.getKey();
                Field field = entry.getValue();
                FieldName anno = field.getAnnotation(FieldName.class);
                String familyName = GlobalConstants.familyName();
                if (anno != null) {
                    if (!anno.mapping() || anno.disuse()) {
                        continue;
                    }
                    if(StringUtils.isNotBlank(anno.value())) {
                        fName = anno.value();
                    }
                    if (StringUtils.isNotBlank(anno.family())) {
                        familyName = anno.family();
                    }
                }

                Type fieldType = field.getType();
                // fieldStr.append("\"rowKey\":{\"cf\":\"" + familyName + "\", \"col\":\"rowKey\", \"type\":\"string\"},\n");
                if (fieldType == String.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"string\"},\n");
                } else if (fieldType == Integer.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"int\"},\n");
                } else if (fieldType == Double.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"double\"},\n");
                } else if (fieldType == Long.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"bigint\"},\n");
                } else if (fieldType == BigDecimal.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"decimal(38,18)\"},\n");
                } else if (fieldType == Float.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"float\"},\n");
                } else if (fieldType == Boolean.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"boolean\"},\n");
                } else if (fieldType == Short.class) {
                    fieldStr.append("\"" + fName + "\":{\"cf\":\"" + familyName + "\", \"col\":\"" + fName + "\", \"type\":\"smallint\"},\n");
                }
            }
            catalog.append(fieldStr.substring(0, fieldStr.length() - 2) + "\n");
            catalog.append("}\n");
        } catch (Exception e) {
            e.printStackTrace();
        }
        catalog.append("}\n");
        return catalog.toString();
    }

}
