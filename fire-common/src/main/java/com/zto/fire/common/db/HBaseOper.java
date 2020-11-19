package com.zto.fire.common.db;

import com.google.gson.Gson;
import com.zto.fire.common.anno.FieldName;
import com.zto.fire.common.bean.HBaseBaseBean;
import com.zto.fire.common.bean.MultiVersionsBean;
import com.zto.fire.common.conf.FireHBaseConf;
import com.zto.fire.common.util.PropUtils;
import com.zto.fire.common.util.ReflectionUtils;
import com.zto.fire.common.util.StackTraceUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;
import scala.collection.JavaConversions;
import scala.collection.mutable.ListBuffer;

import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.math.BigDecimal;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/**
 * HBase操作工具类，除了涵盖CRUD等常用操作外，还提供以下功能：
 * 1. static <T extends HBaseBaseBean> void insert(String tableName, String family, List<T> list)
 * 将自定义的javabean集合批量插入到表中
 * 2. static <T> List<T extends HBaseBaseBean> scan(String tableName, String startRow, String stopRow, Class<T> clazz, FilterList.Operator operator, Filter... filters)
 * 指定查询条件，将查询结果以List<T>形式返回
 * 注：自定义bean中的field需与hbase中的qualifier对应
 * <p>
 * Created by ChengLong on 2017-02-20.
 */
@Deprecated
public class HBaseOper {
    private static Configuration conf;
    private static Connection connection;
    private static Gson gson = new Gson();
    private static final Map<Class, Map<String, Field>> cacheFieldMap = new ConcurrentHashMap<>();
    private static final Logger logger = LoggerFactory.getLogger(HBaseOper.class);
    private static Durability durability = null;

    /**
     * 根据conf信息获取一个单例的连接
     *
     * @return hbase connection
     */
    public static Connection getConnection() {
        if (connection == null) {
            try {
                connection = ConnectionFactory.createConnection(getConfiguration());
                logger.info("获取hbase connection成功");
            } catch (IOException e) {
                logger.error("获取hbase connection失败", e);
            }
        }

        return connection;
    }

    /**
     * 获取Configuration实例
     *
     * @return HBase Configuration对象
     */
    public static Configuration getConfiguration() {
        if (conf == null) {
            conf = HBaseConfiguration.create();
            String clusterName = FireHBaseConf.hbaseCluster(1);
            if (FireHBaseConf.hbaseClusterMap().containsKey(clusterName)) {
                conf.set("hbase.zookeeper.quorum", FireHBaseConf.hbaseClusterMap().get(clusterName));
            } else if (StringUtils.isNotBlank(clusterName)) {
                conf.set("hbase.zookeeper.quorum", clusterName);
            } else {
                throw new IllegalArgumentException("未配置HBase集群信息，请通过以下参数指定：spark.hbase.cluster=xxx");
            }
            // 以spark.hbase.conf.为前缀的配置项为hbase client专项配置，统一设置到hbase的Configuration中
            JavaConversions.mapAsJavaMap(PropUtils.sliceKeys(FireHBaseConf.hbaseConfPrefix())).forEach((k, v) -> {
                logger.info("hbase configuration: key={} value={}", k, v);
                conf.set(k, v);
            });
        }
        return conf;
    }

    /**
     * 插入操作
     *
     * @param tableName 表名
     * @param rowKey    rowKey值
     * @param family    列族
     * @param qualifier 字段
     * @param value     值
     */
    public static void insert(String tableName, String rowKey, String family, String qualifier, String value) {
        if (isExists(tableName) && StringUtils.isNotBlank(rowKey)) {
            try {
                Put put = new Put(rowKey.getBytes());
                put.addColumn(family.getBytes(), qualifier.getBytes(), value.getBytes());
                TableName tbName = TableName.valueOf(tableName);
                Table table = getConnection().getTable(tbName);
                table.put(put);
                table.close();
                logger.info("数据成功写入到hbase中, cluster={} tableName={} rowKey={} family={} qualifier={} value={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey, family, qualifier, value);
            } catch (Exception e) {
                logger.error("数据写入hbase失败, cluster={} tableName={} rowKey={} family={} qualifier={} value={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey, family, qualifier, value, StackTraceUtils.stackTraceInfo(e));
            }
        }
    }

    /**
     * 插入操作
     *
     * @param tableName 表名
     * @param put       Put实例
     */
    public static void insert(String tableName, Put put) {
        if (isExists(tableName) && put != null) {
            try {
                TableName tbName = TableName.valueOf(tableName);
                Table table = getConnection().getTable(tbName);
                table.put(put);
                table.close();
                logger.info("数据成功写入到hbase中, cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
            } catch (Exception e) {
                logger.error("数据写入hbase失败, cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            }
        }
    }

    /**
     * 批量插入多行多列
     *
     * @param tableName 表名
     * @param putList   Put集合
     */
    public static void insertPut(String tableName, List<Put> putList) {
        if (isExists(tableName) && putList != null && putList.size() > 0) {
            Table table = null;
            try {
                TableName tbName = TableName.valueOf(tableName);
                table = getConnection().getTable(tbName);
                table.put(putList);
                logger.info("数据成功写入到hbase中, cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, putList.size());
            } catch (Exception e) {
                logger.error("数据写入hbase失败, cluster={} tableName={} size={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, putList.size(), StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeTable(table);
            }
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     *
     * @param tableName 表名
     * @param obj       继承自HBaseBaseBean的子类实例对象
     */
    public static <T extends HBaseBaseBean> void insert(String tableName, T obj) {
        if (isExists(tableName)) {
            // 将obj转为put对象
            Put put = convert2Put(obj, true);
            if (put != null) {
                insert(tableName, put);
            }
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     *
     * @param tableName 表名
     * @param obj       继承自HBaseBaseBean的子类实例对象
     */
    public static <T extends HBaseBaseBean> void insertIgnoreNull(String tableName, T obj) {
        if (isExists(tableName)) {
            // 将obj转为put对象
            Put put = convert2Put(obj, false);
            if (put != null) {
                insert(tableName, put);
            }
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     *
     * @param tableName    表名
     * @param list         list中的数据必须是HBaseBaseBean的子类实例集合
     * @param insertEmpty  为空的列是否插入到hbase中
     * @param multiVersion 是否以多版本形式插入
     */
    public static <T extends HBaseBaseBean> void insert(String tableName, List<T> list, boolean insertEmpty, boolean multiVersion) {
        if (multiVersion) {
            HBaseOper.insertMultiVersions(tableName, list);
        } else {
            if (insertEmpty) {
                HBaseOper.insert(tableName, list);
            } else {
                HBaseOper.insertIgnoreNull(tableName, list);
            }
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     * 注：仅支持一个列族
     *
     * @param tableName 表名
     * @param list      list中的数据必须是HBaseBaseBean的子类实例集合
     */
    public static <T extends HBaseBaseBean> void insert(String tableName, List<T> list) {
        if (list != null && list.size() > 0) {
            List<Put> putList = new LinkedList<>();
            for (Object obj : list) {
                Put put = convert2Put((T) obj, true);
                if (put != null) {
                    putList.add(put);
                }
            }
            insertPut(tableName, putList);
        }
    }

    /**
     * 多版本数据的插入，会通过反射将所有列转为json保存
     * 注：仅支持一个列族
     *
     * @param tableName 表名
     * @param list      list中的数据必须是HBaseBaseBean的子类实例集合
     */
    public static <T extends HBaseBaseBean> void insertMultiVersions(String tableName, List<T> list) {
        if (StringUtils.isNotBlank(tableName) && list != null && list.size() > 0) {
            List multiBean = list.stream().map(bean -> new MultiVersionsBean(bean)).collect(Collectors.toList());
            insert(tableName, multiBean);
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     * 注：仅支持一个列族
     *
     * @param tableName 表名
     * @param list      list中的数据必须是HBaseBaseBean的子类实例集合
     */
    public static <T extends HBaseBaseBean> void insert(String tableName, ListBuffer<T> list) {
        if (list != null && list.size() > 0) {
            List<Put> putList = new LinkedList<>();
            scala.collection.Iterator<T> it = list.iterator();
            while (it.hasNext()) {
                Put put = convert2Put((T) it.next(), true);
                if (put != null) {
                    // put.setWriteToWAL(false);
                    put.setDurability(getHBaseDurability());
                    putList.add(put);
                }
            }
            insertPut(tableName, putList);
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     * 注：仅支持一个列族
     *
     * @param tableName 表名
     * @param list      list中的数据必须是HBaseBaseBean的子类实例集合
     */
    public static <T extends HBaseBaseBean> void insertIgnoreNull(String tableName, ListBuffer<T> list) {
        HBaseOper.insertIgnoreNull(tableName, list.toList());
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     * 注：仅支持一个列族
     *
     * @param tableName 表名
     * @param list      list中的数据必须是HBaseBaseBean的子类实例集合
     */
    public static <T extends HBaseBaseBean> void insertIgnoreNull(String tableName, List<T> list) {
        if (list != null && list.size() > 0) {
            List<Put> putList = new LinkedList<>();
            Iterator<T> it = list.iterator();
            while (it.hasNext()) {
                Put put = convert2Put((T) it.next(), false);
                if (put != null) {
                    put.setDurability(getHBaseDurability());
                    putList.add(put);
                }
            }
            insertPut(tableName, putList);
        }
    }

    /**
     * 通过反射获取字段数据，保存到HBase中
     * 注：仅支持一个列族
     *
     * @param tableName 表名
     * @param list      list中的数据必须是HBaseBaseBean的子类实例集合
     */
    public static <T extends HBaseBaseBean> void insertIgnoreNull(String tableName, scala.collection.immutable.List<T> list) {
        if (list != null && list.size() > 0) {
            List<Put> putList = new LinkedList<>();
            scala.collection.Iterator<T> it = list.iterator();
            while (it.hasNext()) {
                Put put = convert2Put((T) it.next(), false);
                if (put != null) {
                    put.setDurability(getHBaseDurability());
                    putList.add(put);
                }
            }
            insertPut(tableName, putList);
        }
    }

    /**
     * 获取一行
     *
     * @param tableName    表名
     * @param versionCount 指定获取的历史版本数
     * @param rowKey       rowKey
     * @return HBase记录原生结果
     */
    public static Result get(String tableName, Integer versionCount, String rowKey) {
        if (isExists(tableName) && StringUtils.isNotBlank(rowKey)) {
            Table table = null;
            try {
                table = getConnection().getTable(TableName.valueOf(tableName));
                Get get = new Get(rowKey.getBytes());
                get.setMaxVersions(versionCount);
                logger.info("hbase get成功. cluster={} tableName={} rowKey={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey);
                return table.get(get);
            } catch (Exception e) {
                logger.error("hbase get失败. cluster={} tableName={} rowKey={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeTable(table);
            }
        }
        return null;
    }

    /**
     * 获取一行，并组装为对象
     *
     * @param tableName    表名
     * @param versionCount 版本号
     * @param rowKey       HBase单行rowKey
     * @param clazz        目标类类型，必须是HBaseBaseBean的子类
     * @param <T>          目标类型泛型，必须是HBaseBaseBean的子类
     * @return 目标对象实例
     */
    public static <T extends HBaseBaseBean> T get(String tableName, Integer versionCount, String rowKey, Class clazz) {
        Result rs = get(tableName, versionCount, rowKey);
        return (T) hbaseRow2Bean(rs, clazz);
    }

    /**
     * 获取一条数据
     *
     * @param tableName 表名
     * @param get       HBase的get对象实例
     * @return
     */
    public static Result get(String tableName, Get get) {
        if (isExists(tableName) && get != null) {
            Table table = null;
            try {
                table = getConnection().getTable(TableName.valueOf(tableName));
                logger.info("hbase get成功. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
                return table.get(get);
            } catch (Exception e) {
                logger.error("hbase get失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeTable(table);
            }
        }
        logger.warn("hbase get失败，未找到表. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * 获取一条数据
     *
     * @param tableName 表名
     * @param getList   HBase的get对象实例
     * @return
     */
    public static Result[] get(String tableName, List<Get> getList) {
        if (isExists(tableName) && getList != null && getList.size() > 0) {
            Table table = null;
            try {
                table = getConnection().getTable(TableName.valueOf(tableName));
                logger.info("hbase get成功. cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, getList.size());
                return table.get(getList);
            } catch (Exception e) {
                logger.error("hbase get失败. cluster={} tableName={} size={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, getList.size(), StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeTable(table);
            }
        }
        logger.warn("hbase get失败，请检查表是否存在，或getList可能为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * 获取一条数据，并转为自定义bean
     *
     * @param tableName 表名
     * @param get       HBase的get对象实例
     * @param clazz     类类型
     * @param <T>       泛型
     * @return
     */
    public static <T extends HBaseBaseBean> T get(String tableName, Get get, Class<T> clazz) {
        Result rs = get(tableName, get);
        if (rs == null || rs.isEmpty()) {
            logger.warn("hbase get失败，未get到结果. cluster={} tableName={} rowKey={}", FireHBaseConf.hbaseCluster(1), tableName, new String(get.getRow()));
            return null;
        }
        return hbaseRow2Bean(rs, clazz);
    }

    /**
     * 获取一条数据，并转为自定义bean
     *
     * @param tableName 表名
     * @param get       HBase的get对象实例
     * @param clazz     类类型
     * @param <T>       泛型
     * @return
     */
    public static <T extends HBaseBaseBean> List<T> getMultiVersions(String tableName, Get get, Class<T> clazz) {
        Result rs = get(tableName, get);
        if (rs == null || rs.isEmpty()) {
            logger.warn("hbase get多版本失败，未get到结果. cluster={} tableName={} rowKey={}", FireHBaseConf.hbaseCluster(1), tableName, new String(get.getRow()));
            return null;
        }
        return hbaseMultiRow2Bean(rs, clazz);
    }

    /**
     * 获取一条数据对应的所有历史版本，并转为自定义bean
     *
     * @param tableName 表名
     * @param rowKey    HBase的get对象实例
     * @param clazz     类类型
     * @param <T>       泛型
     * @return
     */
    public static <T extends HBaseBaseBean> List<T> getMultiVersions(String tableName, String rowKey, Class<T> clazz) {
        return getMultiVersions(tableName, Integer.MAX_VALUE, rowKey, clazz);
    }

    /**
     * 获取一条数据对应的多个历史版本，并转为自定义bean
     *
     * @param tableName    表名
     * @param versionCount 获取的版本数
     * @param rowKey       rowKey字符串
     * @param clazz        类类型
     * @param <T>          泛型
     * @return
     */
    public static <T extends HBaseBaseBean> List<T> getMultiVersions(String tableName, Integer versionCount, String rowKey, Class<T> clazz) {
        try {
            Get get = new Get(rowKey.getBytes());
            get.setMaxVersions(versionCount);
            Result rs = get(tableName, get);
            if (rs == null || rs.isEmpty()) {
                logger.warn("hbase get失败，未get到结果. cluster={} tableName={} rowKey={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey);
                return null;
            }
            return hbaseMultiRow2Bean(rs, clazz);
        } catch (Exception e) {
            logger.error("hbase get失败. cluster={} tableName={} rowKey={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey, StackTraceUtils.stackTraceInfo(e));
        }
        return Collections.emptyList();
    }

    /**
     * 获取多条数据，并转为自定义bean List
     *
     * @param tableName 表名
     * @param getList   HBase的get对象实例
     * @param clazz     类类型
     * @param <T>       泛型
     * @return
     */
    public static <T extends HBaseBaseBean> List<T> get(String tableName, List<Get> getList, Class<T> clazz) {
        Result[] rsArr = get(tableName, getList);
        if (rsArr == null || rsArr.length == 0) {
            logger.warn("hbase get多版本失败，未get到结果. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
            return null;
        }
        return hbaseRow2Bean(rsArr, clazz);
    }

    /**
     * 获取多条数据的rowkey集合
     *
     * @param tableName 表名
     * @param getList   HBase的get对象实例
     * @return rowkey集合
     */
    public static List<String> getRowkeyList(String tableName, List<Get> getList) {
        Result[] rsArr = get(tableName, getList);
        if (rsArr == null || rsArr.length == 0) {
            return Collections.emptyList();
        }
        List<String> rowKeyList = new LinkedList<>();
        try {
            byte[] row = null;
            for (Result rs : rsArr) {
                row = rs.getRow();
                if (row != null) {
                    rowKeyList.add(new String(row));
                }
            }
        } catch (Exception e) {
            logger.error("hbase getRowkeyList失败", e);
        }
        return rowKeyList;
    }

    /**
     * 获取多条数据的所有版本，并转为自定义bean List
     *
     * @param tableName 表名
     * @param getList   HBase的get对象实例
     * @param clazz     类类型
     * @param <T>       泛型
     * @return
     */
    public static <T extends HBaseBaseBean> List<T> getMultiVersions(String tableName, List<Get> getList, Class<T> clazz) {
        Result[] rsArr = get(tableName, getList);
        if (rsArr == null || rsArr.length == 0) {
            logger.warn("hbase get多版本失败，未get到结果. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
            return null;
        }
        return hbaseMultiRow2Bean(rsArr, clazz);
    }

    /**
     * 获取一行最新版数据
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @return HBase原生结果
     */
    public static Result get(String tableName, String rowKey) {
        return get(tableName, 1, rowKey);
    }

    /**
     * 获取一行最新版数据，并组装为自定义bean
     *
     * @param tableName 表名
     * @param rowKey    rowKey字段
     * @param clazz     类类型
     * @param <T>       泛型
     * @return 转换后的clazz对象实例
     */
    public static <T extends HBaseBaseBean> T get(String tableName, String rowKey, Class<T> clazz) {
        return get(tableName, 1, rowKey, clazz);
    }

    /**
     * 获取多行数据
     *
     * @param tableName    表名
     * @param versionCount 历史版本数
     * @param rowKey       rowKey字段
     * @return HBase结果集
     */
    public static Result[] gets(String tableName, Integer versionCount, String... rowKey) {
        if (isExists(tableName) && rowKey != null && rowKey.length > 0) {
            Table table = null;
            try {
                table = getConnection().getTable(TableName.valueOf(tableName));
                List<Get> gets = new LinkedList<>();
                for (String key : rowKey) {
                    Get get = new Get(key.getBytes());
                    get.setMaxVersions(versionCount);
                    gets.add(get);
                }
                logger.info("hbase gets成功. cluster={} tableName={} getSize={}", FireHBaseConf.hbaseCluster(1), tableName, gets.size());
                return table.get(gets);
            } catch (Exception e) {
                logger.error("hbase gets失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeTable(table);
            }
        }
        logger.warn("hbase gets失败，请检查表是否存在，或rowKey可能为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * 获取多行
     *
     * @param tableName 表名
     * @param rowKey    多个rowKey
     * @return 结果集
     */
    public static Result[] gets(String tableName, String... rowKey) {
        return gets(tableName, 1, rowKey);
    }

    /**
     * 扫描指定的表，指定过滤器
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @param operator  and或or
     * @param filters   过滤器
     * @return scan结果集
     */
    public static ResultScanner scan(String tableName, String startRow, String stopRow, FilterList.Operator operator, Filter... filters) {
        Table table = getTable(tableName);
        if (table != null && StringUtils.isNotBlank(startRow) && StringUtils.isNotBlank(stopRow)) {
            try {
                Scan scan = new Scan(startRow.getBytes(), stopRow.getBytes());
                if (filters != null && filters.length > 0) {
                    Filter filterList = new FilterList(operator, filters);
                    scan.setFilter(filterList);
                }
                return table.getScanner(scan);
            } catch (Exception e) {
                logger.error("hbase scan失败. cluster={} tableName={} startRow={} endRow={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, startRow, stopRow, StackTraceUtils.stackTraceInfo(e));
            }
        }
        logger.warn("hbase scan失败，请检查表是否存在，或rowKey的起止是否为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * 表扫描
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @return scan结果集
     */
    public static ResultScanner scan(String tableName, String startRow, String stopRow) {
        return scan(tableName, startRow, stopRow, null, null);
    }

    /**
     * scan某张表
     *
     * @param tableName 表名
     * @param scan      scan对象
     * @return scan结果集
     */
    public static ResultScanner scan(String tableName, Scan scan) {
        Table table = getTable(tableName);
        if (table != null && scan != null) {
            try {
                return table.getScanner(scan);
            } catch (Exception e) {
                logger.error("hbase scan失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeTable(table);
            }
        }
        logger.warn("hbase scan失败，请检查表是否存在，或scan对象是否为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * scan表，并返回List<T>
     *
     * @param tableName 表名
     * @param scan      scan对象
     * @param clazz     目标类型
     * @param <T>       目标类型泛型
     * @return 目标类型实例集合
     */
    public static <T extends HBaseBaseBean> List<T> scan(String tableName, Scan scan, Class<T> clazz) {
        Table table = getTable(tableName);
        if (table != null && scan != null && clazz != null) {
            ResultScanner rsScanner = null;
            try {
                List<T> list = new LinkedList<>();
                rsScanner = table.getScanner(scan);
                for (Result rs : rsScanner) {
                    T obj = hbaseRow2Bean(rs, clazz);
                    if (obj != null) {
                        list.add(obj);
                    }
                }
                logger.info("hbase scan成功. cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, list.size());
                return list;
            } catch (Exception e) {
                logger.error("hbase scan失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeResultAndTable(rsScanner, table);
            }
        }
        logger.warn("hbase scan失败，请检查表是否存在，或scan对象是否为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return Collections.emptyList();
    }

    /**
     * scan表，查询记录的所有版本，并返回List<T>
     *
     * @param tableName 表名
     * @param scan      scan对象
     * @param clazz     目标类型
     * @param <T>       目标类型泛型
     * @return 目标类型实例集合
     */
    public static <T extends HBaseBaseBean> List<T> scanMaxVersions(String tableName, Scan scan, Class<T> clazz) {
        Table table = getTable(tableName);
        if (table != null && scan != null && clazz != null) {
            ResultScanner rsScanner = null;
            try {
                List<T> list = new LinkedList<>();
                scan.setMaxVersions();
                rsScanner = table.getScanner(scan);
                for (Result rs : rsScanner) {
                    T obj = hbaseRow2Bean(rs, clazz);
                    if (obj != null) {
                        list.add(obj);
                    }
                }
                logger.info("hbase scanMaxVersions成功. cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, list.size());
                return list;
            } catch (Exception e) {
                logger.error("hbase scanMaxVersions失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeResultAndTable(rsScanner, table);
            }
        }
        logger.warn("hbase scan失败，请检查表是否存在，或scan对象是否为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return Collections.emptyList();
    }

    /**
     * 查询全表，并返回List<T>
     *
     * @param tableName 表名
     * @param clazz     目标类型
     * @param <T>       目标类型泛型
     * @return 目标类型实例集合
     */
    public static <T extends HBaseBaseBean> List<T> scanAll(String tableName, Class<T> clazz) {
        Table table = getTable(tableName);
        if (table != null && clazz != null) {
            ResultScanner rsScanner = null;
            try {
                List<T> list = new LinkedList<>();
                Scan scan = new Scan();
                rsScanner = table.getScanner(scan);
                for (Result rs : rsScanner) {
                    T obj = hbaseRow2Bean(rs, clazz);
                    if (obj != null) {
                        list.add(obj);
                    }
                }
                logger.info("hbase scanAll成功. cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, list.size());
                return list;
            } catch (Exception e) {
                logger.error("hbase scanAll失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeResultAndTable(rsScanner, table);
            }
        }
        logger.warn("hbase scan失败，请检查表是否存在，或scan对象是否为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return Collections.emptyList();
    }

    /**
     * 表扫描，将查询后的数据封装到List中
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @param clazz     类型
     * @param operator  条件and或or
     * @param filters   过滤器
     * @param <T>       目标泛型类型
     * @return 指定类型的List
     */
    public static <T extends HBaseBaseBean> List<T> scan(String tableName, String startRow, String stopRow, Class<T> clazz, FilterList.Operator operator, Filter... filters) {
        Table table = getTable(tableName);
        if (table != null && StringUtils.isNotBlank(startRow) && StringUtils.isNotBlank(stopRow)) {
            ResultScanner rsScanner = null;
            try {
                Scan scan = new Scan(startRow.getBytes(), stopRow.getBytes());
                if (filters != null && filters.length > 0) {
                    Filter filterList = new FilterList(operator, filters);
                    scan.setFilter(filterList);
                }
                rsScanner = table.getScanner(scan);
                List<T> list = new LinkedList<>();
                if (rsScanner != null) {
                    for (Result rs : rsScanner) {
                        T obj = hbaseRow2Bean(rs, clazz);
                        if (obj != null) {
                            list.add(obj);
                        }
                    }
                }
                logger.info("hbase scan成功. cluster={} tableName={} startRow={} stopRow={} size={}", FireHBaseConf.hbaseCluster(1), tableName, startRow, stopRow, list.size());
                return list;
            } catch (Exception e) {
                logger.error("hbase scan失败. cluster={} tableName={} startRow={} stopRow={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, startRow, stopRow, StackTraceUtils.stackTraceInfo(e));
            } finally {
                closeResultAndTable(rsScanner, table);
            }
        }
        logger.warn("hbase scan失败，请检查表是否存在，或startRow、stopRow对象为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * 表多版本扫描，将查询后的数据封装到List中
     *
     * @param tableName 表名
     * @param scan      scan对象
     * @param clazz     类型
     * @param <T>       目标泛型类型
     * @return 指定类型的List
     */
    public static <T extends HBaseBaseBean> List<T> scanMultiVersions(String tableName, Scan scan, Class<T> clazz) {
        if (StringUtils.isBlank(tableName) || scan == null || clazz == null) {
            return null;
        }
        Table table = getTable(tableName);
        ResultScanner rsScanner = null;
        try {
            rsScanner = table.getScanner(scan);
            List<T> list = new LinkedList<>();
            if (rsScanner != null) {
                for (Result rs : rsScanner) {
                    List<T> objList = hbaseMultiRow2Bean(rs, clazz);
                    if (objList != null && objList.size() > 0) {
                        list.addAll(objList);
                    }
                }
            }
            logger.info("hbase scan成功. cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, list.size());
            return list;
        } catch (Exception e) {
            logger.error("hbase scan失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeResultAndTable(rsScanner, table);
        }
        logger.warn("hbase scan失败，请检查表是否存在，或scan对象为空. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
        return null;
    }

    /**
     * 表多版本扫描，将查询后的数据封装到List中
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @param clazz     类型
     * @param operator  条件and或or
     * @param filters   过滤器
     * @param <T>       目标泛型类型
     * @return 指定类型的List
     */
    public static <T extends HBaseBaseBean> List<T> scanMultiVersions(String tableName, String startRow, String stopRow, Class<T> clazz, Integer versionCount, FilterList.Operator operator, Filter... filters) {
        if (StringUtils.isBlank(tableName) || StringUtils.isBlank(startRow) || StringUtils.isBlank(stopRow) || clazz == null) {
            return null;
        }
        if (versionCount == null) {
            versionCount = Integer.MAX_VALUE;
        }
        Scan scan = HBaseOper.buildScan(startRow, stopRow);
        if (operator != null && filters != null && filters.length > 0) {
            scan.setFilter(new FilterList(operator, filters));
        }
        scan.setMaxVersions(versionCount);
        return scanMultiVersions(tableName, scan, clazz);
    }


    /**
     * 表扫描，将查询后的数据封装到List中
     *
     * @param tableName 表名
     * @param startRow  开始行
     * @param stopRow   结束行
     * @param clazz     自定义的javabean类型（暂只支持简单成员）
     * @param <T>       目标泛型类型
     * @return 指定类型的List
     */
    public static <T extends HBaseBaseBean> List<T> scan(String tableName, String startRow, String stopRow, Class<T> clazz) {
        return scan(tableName, startRow, stopRow, clazz, null, null);
    }

    /**
     * 根据多个rowkey删除对应的整行记录
     *
     * @param tableName 表名
     * @param rowKey    多个rowKey
     */
    public static void deleteRow(String tableName, String... rowKey) {
        try {
            List<String> rowKeyList = Arrays.asList(rowKey);
            deleteRow(tableName, rowKeyList);
        } catch (Exception e) {
            logger.error("delete row失败.", e);
        }
    }

    /**
     * 根据多个rowkey删除对应的整行记录
     *
     * @param tableName  表名
     * @param rowKeyList rowKey集合
     */
    public static void deleteRow(String tableName, List<String> rowKeyList) {
        try {
            if (StringUtils.isNotBlank(tableName) && rowKeyList != null && rowKeyList.size() > 0) {
                TableName tbName = TableName.valueOf(tableName);
                Table table = getConnection().getTable(tbName);
                List<Delete> deletes = new LinkedList<>();
                for (String key : rowKeyList) {
                    Delete delete = new Delete(key.getBytes());
                    deletes.add(delete);
                }
                table.delete(deletes);
                table.close();
                logger.info("delete row成功. cluster={} tableName={} size={}", FireHBaseConf.hbaseCluster(1), tableName, deletes.size());
            }
        } catch (Exception e) {
            logger.error("delete row失败.", e);
        }
    }


    /**
     * 判断表是否存在
     *
     * @param tableName HBase表名
     * @return 是否存在
     * @throws IOException IO异常
     */
    public static boolean isExists(String tableName) {
        Admin admin = null;
        Boolean isExist = false;
        try {
            admin = getConnection().getAdmin();
            isExist = admin.tableExists(TableName.valueOf(tableName));
        } catch (Exception e) {
            logger.error("判断HBase表存在失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeAdmin(admin);
        }
        return isExist;
    }

    /**
     * 创建多列族的HBase表
     *
     * @param tableName     表名
     * @param columnFamilys 多个列族
     */
    private static void createTable(String tableName, String... columnFamilys) {
        Admin admin = null;
        try {
            admin = getConnection().getAdmin();
            TableName tbName = TableName.valueOf(tableName);
            if (!admin.tableExists(tbName)) {
                // 新建一个students表的描述
                HTableDescriptor tableDesc = new HTableDescriptor(tbName);
                // 在描述里添加列族
                for (String columnFamily : columnFamilys) {
                    HColumnDescriptor desc = new HColumnDescriptor(columnFamily);
                    // 启用压缩
                    desc.setCompressionType(Compression.Algorithm.SNAPPY);
                    tableDesc.addFamily(desc);
                }
                admin.createTable(tableDesc);
            }
        } catch (Exception e) {
            logger.error("创建HBase表失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeAdmin(admin);
        }
    }

    /**
     * 删除表
     *
     * @param tableName 表名
     */
    private static void dropTable(String tableName) {
        Admin admin = null;
        try {
            admin = getConnection().getAdmin();
            TableName tbName = TableName.valueOf(tableName);
            if (admin.tableExists(tbName)) {
                admin.disableTable(tbName);
                admin.deleteTable(tbName);
            }
        } catch (Exception e) {
            logger.error("drop HBase表存在失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeAdmin(admin);
        }
    }

    /**
     * 启用表
     *
     * @param tableName 表名
     */
    private static void enableTable(String tableName) {
        Admin admin = null;
        try {
            admin = getConnection().getAdmin();
            TableName tbName = TableName.valueOf(tableName);
            if (admin.tableExists(tbName) && !admin.isTableEnabled(tbName)) {
                admin.enableTable(tbName);
            }
        } catch (Exception e) {
            logger.error("Enable HBase表存在失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeAdmin(admin);
        }
    }

    /**
     * 关闭表
     *
     * @param tableName 表名
     */
    private static void disable(String tableName) {
        Admin admin = null;
        try {
            admin = getConnection().getAdmin();
            TableName tbName = TableName.valueOf(tableName);
            if (admin.tableExists(tbName) && admin.isTableEnabled(tbName)) {
                admin.disableTable(tbName);
            }
        } catch (Exception e) {
            logger.error("Disable HBase表存在失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeAdmin(admin);
        }
    }

    /**
     * 清空指定表
     *
     * @param tableName 表名
     */
    private static void truncate(String tableName) {
        Admin admin = null;
        try {
            admin = getConnection().getAdmin();
            TableName tbName = TableName.valueOf(tableName);
            if (admin.tableExists(tbName)) {
                if (admin.isTableEnabled(tbName)) {
                    admin.disableTable(tbName);
                }
                admin.truncateTable(tbName, true);
            }
        } catch (Exception e) {
            logger.error("Truncate HBase表存在失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        } finally {
            closeAdmin(admin);
        }
    }

    /**
     * 批量删除列族
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @param family    多个列族
     */
    public static void deleteFamily(String tableName, String rowKey, String... family) {
        if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(rowKey) && isExists(tableName)) {
            Delete delete = new Delete(rowKey.getBytes());
            if (family != null && family.length > 0) {
                for (String cf : family) {
                    delete.addFamily(cf.getBytes());
                }
                try {
                    TableName tbName = TableName.valueOf(tableName);
                    Table table = getConnection().getTable(tbName);
                    table.delete(delete);
                    table.close();
                } catch (Exception e) {
                    logger.error("Delete HBase表Family失败. cluster={} tableName={} rowKey={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey, StackTraceUtils.stackTraceInfo(e));
                }
            }
        }
    }

    /**
     * 批量单个列族下的多个字段的所有版本数据
     *
     * @param tableName 表名
     * @param rowKey    rowKey字段
     * @param family    列族
     * @param qualifier 字段名
     */
    public static void deleteColumnsMult(String tableName, String rowKey, String family, String... qualifier) {
        deleteColumns(tableName, rowKey, family, true, qualifier);
    }

    /**
     * 批量单个列族下的多个字段的最新版数据
     *
     * @param tableName 表名
     * @param rowKey    rowKey
     * @param family    列族
     * @param qualifier 字段名
     */
    public static void deleteColumnsSingle(String tableName, String rowKey, String family, String... qualifier) {
        deleteColumns(tableName, rowKey, family, false, qualifier);
    }

    /**
     * 批量单个列族下的多个字段
     *
     * @param tableName    表名
     * @param rowKey       rowKey字段
     * @param family       列族
     * @param multVersions 是否删除所有版本
     * @param qualifier    列名
     */
    private static void deleteColumns(String tableName, String rowKey, String family, boolean multVersions, String... qualifier) {
        try {
            if (StringUtils.isNotBlank(tableName) && StringUtils.isNotBlank(rowKey) && StringUtils.isNotBlank(family) && qualifier != null && qualifier.length > 0) {
                Delete delete = new Delete(rowKey.getBytes());
                if (multVersions) {
                    // 删除所有版本
                    for (String qua : qualifier) {
                        delete.addColumns(family.getBytes(), qua.getBytes());
                    }
                } else {
                    // 删除最新版本
                    for (String qua : qualifier) {
                        delete.addColumn(family.getBytes(), qua.getBytes());
                    }
                }
                TableName tbName = TableName.valueOf(tableName);
                Table table = getConnection().getTable(tbName);
                table.delete(delete);
                table.close();
            }
        } catch (Exception e) {
            logger.error("删除HBase列失败. cluster={} tableName={} rowKey={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, rowKey, StackTraceUtils.stackTraceInfo(e));
        }
    }

    /**
     * 关闭ResultScanner与Table对象
     */
    public static void closeResultAndTable(ResultScanner rsScanner, Table table) {
        try {
            if (rsScanner != null) {
                rsScanner.close();
            }
        } catch (Exception e) {
            logger.error("关闭ResultScanner失败", e);
        } finally {
            closeTable(table);
        }
    }

    /**
     * 关闭table对象
     */
    public static void closeTable(Table table) {
        if (table != null) {
            try {
                table.close();
            } catch (Exception e) {
                logger.error("关闭hbase table对象失败", e);
            }
        }
    }

    /**
     * 释放对象
     *
     * @param admin admin对象实例
     */
    private static void closeAdmin(Admin admin) {
        if (admin != null) {
            try {
                admin.close();
            } catch (Exception e) {
                logger.error("关闭hbase admin对象失败", e);
            }
        }
    }

    /**
     * 根据表名获取Table实例
     *
     * @param tableName 表名
     */
    private static Table getTable(String tableName) {
        try {
            if (isExists(tableName)) {
                logger.info("HBase getTable成功. cluster={} tableName={}", FireHBaseConf.hbaseCluster(1), tableName);
                return getConnection().getTable(TableName.valueOf(tableName));
            }
        } catch (Exception e) {
            logger.error("HBase getTable失败. cluster={} tableName={} exception={}", FireHBaseConf.hbaseCluster(1), tableName, StackTraceUtils.stackTraceInfo(e));
        }
        return null;
    }

    /**
     * 构建Get对象
     *
     * @param rowKey
     * @return
     */
    public static Get buildGet(String rowKey) {
        Get get = new Get(rowKey.getBytes());
        get.addFamily(FireHBaseConf.familyName(1).getBytes());
        return get;
    }

    /**
     * 构建Scan对象
     *
     * @param startRow 指定起始rowkey
     * @param stopRow  指定结束rowkey
     * @return scan实例
     */
    public static Scan buildScan(String startRow, String stopRow) {
        return buildScan(startRow, stopRow, null);
    }

    /**
     * 构建Scan对象
     *
     * @param startRow 指定起始rowkey
     * @param stopRow  指定结束rowkey
     * @param filter   过滤器
     * @return scan实例
     */
    public static Scan buildScan(String startRow, String stopRow, Filter filter) {
        Scan scan = new Scan();
        if (StringUtils.isNotBlank(startRow)) {
            scan.setStartRow(startRow.getBytes());
        }
        if (StringUtils.isNotBlank(stopRow)) {
            scan.setStopRow(stopRow.getBytes());
        }
        if (filter != null) {
            scan.setFilter(filter);
        }
        scan.setCaching(1000);

        return scan;
    }


    /**
     * 将class中的field转为map映射
     *
     * @param clazz Class类型
     * @return 名称与字段的映射map
     */
    private static <T extends HBaseBaseBean> Map<String, Field> getFieldNameMap(Class<T> clazz) {
        if (!HBaseOper.cacheFieldMap.containsKey(clazz)) {
            Map<String, Field> allFields = ReflectionUtils.getAllFields(clazz);
            Map<String, Field> fieldMap = new HashMap<>(allFields.size());
            for (Field field : allFields.values()) {
                if (field != null) {
                    field.setAccessible(true);
                    FieldName fieldName = field.getAnnotation(FieldName.class);
                    String family = "";
                    String qualifier = "";
                    if (fieldName != null) {
                        family = fieldName.family();
                        qualifier = fieldName.value();
                    }
                    if (StringUtils.isBlank(family)) {
                        family = FireHBaseConf.familyName(1);
                    }
                    if (StringUtils.isBlank(qualifier)) {
                        qualifier = field.getName();
                    }
                    fieldMap.put(family + ":" + qualifier, field);
                }
            }
            HBaseOper.cacheFieldMap.put(clazz, fieldMap);
        }
        return HBaseOper.cacheFieldMap.get(clazz);
    }

    /**
     * 为指定对象的field赋值
     *
     * @param obj   目标对象
     * @param field 指定filed
     * @param value byte类型的数据
     */
    private static <T extends HBaseBaseBean> void setFieldBytesValue(T obj, Field field, byte[] value) throws IllegalAccessException {
        if (field != null && value != null && value.length > 0) {
            field.setAccessible(true);
            Type fieldType = field.getType();
            if (fieldType == String.class) {
                field.set(obj, Bytes.toString(value));
            } else if (fieldType == Integer.class) {
                field.set(obj, Bytes.toInt(value));
            } else if (fieldType == Double.class) {
                field.set(obj, Bytes.toDouble(value));
            } else if (fieldType == Long.class) {
                field.set(obj, Bytes.toLong(value));
            } else if (fieldType == BigDecimal.class) {
                field.set(obj, Bytes.toBigDecimal(value));
            } else if (fieldType == Float.class) {
                field.set(obj, Bytes.toFloat(value));
            } else if (fieldType == Boolean.class) {
                field.set(obj, Bytes.toBoolean(value));
            } else if (fieldType == Short.class) {
                field.set(obj, Bytes.toShort(value));
            }
        } else {
            if (field != null) {
                field.set(obj, null);
            }
        }
    }

    /**
     * 将含有多版本的cell映射为field
     *
     * @param rs       hbase 结果集
     * @param clazz    目标类型
     * @param fieldMap 字段映射信息
     */
    private static <T extends HBaseBaseBean> List<T> multiCell2Field(Result rs, Class<T> clazz, Map<String, Field> fieldMap) {
        List<T> objList = new LinkedList<>();
        try {
            Cell[] cells = rs.rawCells();
            for (Cell cell : cells) {
                MultiVersionsBean obj = new MultiVersionsBean();
                String rowKey = new String(CellUtil.cloneRow(cell));
                String family = new String(CellUtil.cloneFamily(cell));
                String qualifier = new String(CellUtil.cloneQualifier(cell));
                byte[] value = CellUtil.cloneValue(cell);
                Field field = fieldMap.get(family + ":" + qualifier);
                HBaseOper.setFieldBytesValue(obj, field, value);
                Field idField = ReflectionUtils.getFieldByName(clazz, "rowKey");
                if (idField == null) {
                    throw new IllegalArgumentException(clazz.getName() + " 必须有名为rowKey的成员");
                }
                idField.setAccessible(true);
                idField.set(obj, rowKey);
                if (StringUtils.isNotBlank(obj.getMultiFields())) {
                    objList.add(gson.fromJson(obj.getMultiFields(), clazz));
                }
            }
        } catch (Exception e) {
            logger.error("HBase multiCell2Field失败. cluster={} exception={}", FireHBaseConf.hbaseCluster(1), StackTraceUtils.stackTraceInfo(e));
        }
        return objList;
    }

    /**
     * 将cell中的值转为File的值
     *
     * @param clazz    类类型
     * @param fieldMap 成员变量信息
     * @param rs       hbase查询结果集
     * @return clazz对应的结果实例
     */
    private static <T extends HBaseBaseBean> T cell2Field(Class<T> clazz, Map<String, Field> fieldMap, Result rs) {
        T obj = null;
        try {
            obj = clazz.newInstance();
            Cell[] cells = rs.rawCells();
            String rowKey = convertCells2Fields(fieldMap, obj, cells);
            Field idField = ReflectionUtils.getFieldByName(clazz, "rowKey");
            if (idField == null) {
                throw new IllegalArgumentException(clazz.getName() + " 必须有名为rowKey的成员");
            }
            idField.setAccessible(true);
            idField.set(obj, rowKey);
        } catch (Exception e) {
            logger.error("HBase cell2Field失败. cluster={} exception={}", FireHBaseConf.hbaseCluster(1), StackTraceUtils.stackTraceInfo(e));
        }
        return obj;
    }

    /**
     * 一次循环取出cell中的值赋值给各个field
     *
     * @param obj   对象实例
     * @param cells hbase结果集中的cells集合
     * @return rowkey
     */
    private static <T extends HBaseBaseBean> String convertCells2Fields(Map<String, Field> fieldMap, T obj, Cell[] cells) throws IllegalAccessException {
        String rowKey = "";
        for (Cell cell : cells) {
            rowKey = new String(CellUtil.cloneRow(cell));
            String family = new String(CellUtil.cloneFamily(cell));
            String qualifier = new String(CellUtil.cloneQualifier(cell));
            byte[] value = CellUtil.cloneValue(cell);
            Field field = fieldMap.get(family + ":" + qualifier);
            HBaseOper.setFieldBytesValue(obj, field, value);
        }
        return rowKey;
    }

    /**
     * 将结果映射到自定义bean中
     *
     * @param rs  HBase查询结果集
     * @param <T> 映射的目标Class类型
     * @return 目标类型实例
     */
    public static <T extends HBaseBaseBean> T hbaseRow2Bean(Result rs, Class<T> clazz) {
        if (rs == null || clazz == null || rs.isEmpty()) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldNameMap(clazz);
        if (fieldMap == null || fieldMap.size() == 0) {
            throw new RuntimeException(clazz.getName() + " 中的field为空或没有使用@FieldName");
        }

        return cell2Field(clazz, fieldMap, rs);
    }

    /**
     * 将结果映射到自定义bean中
     *
     * @param rs  HBase查询结果集
     * @param <T> 映射的目标Class类型
     * @return 目标类型实例
     */
    public static <T extends HBaseBaseBean> List<T> hbaseMultiRow2Bean(Result rs, Class<T> clazz) {
        if (rs == null || clazz == null || rs.isEmpty()) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldNameMap(MultiVersionsBean.class);
        if (fieldMap == null || fieldMap.size() == 0) {
            throw new RuntimeException(clazz.getName() + " 中的field为空或没有使用@FieldName");
        }
        return multiCell2Field(rs, clazz, fieldMap);
    }

    /**
     * 将结果映射到自定义bean中
     *
     * @param rsArr HBase查询结果集
     * @param <T>   映射的目标Class类型
     * @return 目标类型实例
     */
    public static <T extends HBaseBaseBean> List<T> hbaseRow2Bean(Result[] rsArr, Class<T> clazz) {
        if (rsArr == null || rsArr.length == 0 || clazz == null) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldNameMap(clazz);
        if (fieldMap == null || fieldMap.size() == 0) {
            throw new RuntimeException(clazz.getName() + " 中的field为空或没有使用@FieldName");
        }

        List<T> objList = new LinkedList<>();
        for (Result rs : rsArr) {
            if (rs.isEmpty()) {
                continue;
            }
            T obj = cell2Field(clazz, fieldMap, rs);
            if (obj != null) {
                objList.add(obj);
            }
        }
        return objList;
    }

    /**
     * 将结果映射到自定义bean中
     *
     * @param rsArr HBase查询结果集
     * @param <T>   映射的目标Class类型
     * @return 目标类型实例
     */
    public static <T extends HBaseBaseBean> List<T> hbaseMultiRow2Bean(Result[] rsArr, Class<T> clazz) {
        if (rsArr == null || rsArr.length == 0 || clazz == null) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldNameMap(MultiVersionsBean.class);
        if (fieldMap == null || fieldMap.size() == 0) {
            throw new RuntimeException(MultiVersionsBean.class.getName() + " 中的field为空或没有使用@FieldName");
        }

        List<T> objList = new LinkedList<>();
        for (Result rs : rsArr) {
            if (rs.isEmpty()) {
                continue;
            }
            objList.addAll(HBaseOper.multiCell2Field(rs, clazz, fieldMap));
        }
        return objList;
    }

    /**
     * 将结果映射到自定义bean中
     *
     * @param it  HBase查询结果集
     * @param <T> 映射的目标Class类型
     * @return 目标类型实例
     */
    public static <T extends HBaseBaseBean> scala.collection.Iterator<T> hbaseRow2BeanList(scala.collection.Iterator<Tuple2<ImmutableBytesWritable, Result>> it, Class<T> clazz) {
        if (it == null || clazz == null) {
            return null;
        }
        Map<String, Field> fieldMap = getFieldNameMap(clazz);
        if (fieldMap == null || fieldMap.size() == 0) {
            throw new RuntimeException(clazz.getName() + " 中的field为空或没有使用@FieldName");
        }
        ListBuffer<T> beanList = new ListBuffer<T>();
        try {
            while (it.hasNext()) {
                T obj = clazz.newInstance();
                Cell[] cells = it.next()._2.rawCells();
                String rowKey = convertCells2Fields(fieldMap, (T) obj, cells);
                Field idField = ReflectionUtils.getFieldByName(clazz, "rowKey");
                if (idField == null) {
                    throw new IllegalArgumentException(clazz.getName() + " 必须有名为rowKey的成员");
                }
                idField.setAccessible(true);
                idField.set(obj, rowKey);
                beanList.$plus$eq(obj);
            }
        } catch (Exception e) {
            logger.error("HBase hbaseRow2BeanList失败. cluster={} exception={}", FireHBaseConf.hbaseCluster(1), StackTraceUtils.stackTraceInfo(e));
        }
        return beanList.iterator();
    }

    /**
     * 将多版本结果映射到自定义bean中
     *
     * @param it  HBase查询结果集
     * @param <T> 映射的目标Class类型
     * @return 目标类型实例
     */
    public static <T extends HBaseBaseBean> List<T> hbaseMultiVersionRow2BeanList(scala.collection.Iterator<Tuple2<ImmutableBytesWritable, Result>> it, Class<T> clazz) {
        List<T> beanList = new LinkedList<>();
        try {
            while (it.hasNext()) {
                beanList.addAll(hbaseMultiRow2Bean(it.next()._2, clazz));
            }
        } catch (Exception e) {
            logger.error("HBase hbaseMultiVersionRow2BeanList失败. cluster={} exception={}", FireHBaseConf.hbaseCluster(1), StackTraceUtils.stackTraceInfo(e));
        }
        return beanList;
    }

    /**
     * 将Javabean转为put对象
     *
     * @param obj         对象
     * @param <T>         继承自HBaseBaseBean类的实例
     * @param insertEmpty true:插入null字段，false：不插入空字段
     * @return put对象实例
     */
    public static <T extends HBaseBaseBean> Put convert2Put(T obj, boolean insertEmpty) {
        if (obj != null) {
            try {
                Class clazz = obj.getClass();
                // 获取RowKey字段中的值
                Field rowKeyField = ReflectionUtils.getFieldByName(clazz, "rowKey");
                rowKeyField.setAccessible(true);
                Object rowKeyObj = rowKeyField.get(obj);
                if (rowKeyObj == null) {
                    Method method = ReflectionUtils.getMethodByName(clazz, "buildRowKey");
                    obj = (T) method.invoke(obj);
                    rowKeyObj = rowKeyField.get(obj);
                    if (rowKeyObj == null) {
                        throw new IllegalArgumentException("rowkey不能为空！请检查" + clazz.getName() + " 中是否实现buildRowKey()方法");
                    }
                }
                byte[] rowKey = rowKeyObj.toString().getBytes();
                Map<String, Field> allFields = ReflectionUtils.getAllFields(obj.getClass());
                Put put = new Put(rowKey);
                put.setDurability(getHBaseDurability());
                // put.setWriteToWAL(false);
                if (allFields != null && allFields.size() > 0) {
                    for (Field field : allFields.values()) {
                        field.setAccessible(true);
                        Object objValue = field.get(obj);
                        if (!insertEmpty && objValue == null) {
                            continue;
                        }
                        FieldName fieldName = field.getAnnotation(FieldName.class);
                        String name = "";
                        String familyName = "";
                        if (fieldName != null) {
                            if (fieldName.disuse()) continue;
                            familyName = fieldName.family();
                            name = fieldName.value();
                        }
                        if (StringUtils.isBlank(familyName)) {
                            familyName = FireHBaseConf.familyName(1);
                        }
                        if (StringUtils.isBlank(name)) {
                            name = field.getName();
                        }
                        byte[] famliyByte = familyName.getBytes();
                        Type fieldType = field.getType();
                        if (objValue != null) {
                            String objValueStr = objValue.toString();
                            if (fieldType == String.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(objValueStr));
                            } else if (fieldType == Integer.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(Integer.parseInt(objValueStr)));
                            } else if (fieldType == Double.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(Double.parseDouble(objValueStr)));
                            } else if (fieldType == Long.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(Long.parseLong(objValueStr)));
                            } else if (fieldType == BigDecimal.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(new BigDecimal(objValueStr)));
                            } else if (fieldType == Float.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(Float.parseFloat(objValueStr)));
                            } else if (fieldType == Boolean.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(Boolean.parseBoolean(objValueStr)));
                            } else if (fieldType == Short.class) {
                                put.addColumn(famliyByte, name.getBytes(), Bytes.toBytes(Short.parseShort(objValueStr)));
                            }
                        } else {
                            put.addColumn(famliyByte, name.getBytes(), null);
                        }
                    }
                }
                return put;
            } catch (Exception e) {
                logger.error("HBase convert2Put失败. cluster={} exception={}", FireHBaseConf.hbaseCluster(1), StackTraceUtils.stackTraceInfo(e));
            }
        }
        return null;
    }

    /**
     * 提供给Spark
     *
     * @param obj 继承自HBaseBaseBean的子类实例
     * @param <T> HBaseBaseBean的子类
     * @return HBaseBaseBean的子类实例
     */
    public static <T extends HBaseBaseBean> Tuple2<ImmutableBytesWritable, Put> convert2PutTuple(T obj) {
        return new Tuple2(new ImmutableBytesWritable(), convert2Put(obj, true));
    }

    /**
     * 获取hbase的durability
     */
    public static Durability getHBaseDurability() {
        if (durability == null) {
            String hbaseDurability = FireHBaseConf.hbaseDurability(1);
            if (StringUtils.isBlank(hbaseDurability)) {
                durability = Durability.USE_DEFAULT;
            } else {
                if ("ASYNC_WAL".equalsIgnoreCase(hbaseDurability)) {
                    durability = Durability.ASYNC_WAL;
                } else if ("FSYNC_WAL".equalsIgnoreCase(hbaseDurability)) {
                    durability = Durability.FSYNC_WAL;
                } else if ("SKIP_WAL".equalsIgnoreCase(hbaseDurability)) {
                    durability = Durability.SKIP_WAL;
                } else if ("SYNC_WAL".equalsIgnoreCase(hbaseDurability)) {
                    durability = Durability.SYNC_WAL;
                } else {
                    durability = Durability.USE_DEFAULT;
                }
            }
        }
        return durability;
    }
}