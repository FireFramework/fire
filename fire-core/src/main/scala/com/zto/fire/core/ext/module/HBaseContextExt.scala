package com.zto.fire.core.ext.module

import com.zto.fire.common.bean.{HBaseBaseBean, MultiVersionsBean}
import com.zto.fire.common.db.HBaseOper
import com.zto.fire.common.util.{GlobalConstants, ParamUtils}
import com.zto.fire.core.util.{SingletonFactory, SparkUtils}
import org.apache.commons.lang3.StringUtils
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.TableName
import org.apache.hadoop.hbase.client._
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat
import org.apache.hadoop.hbase.spark.HBaseContext
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.{DataFrame, Dataset, Encoders, Row}
import org.apache.spark.streaming.dstream.DStream

import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag

/**
  * HBase直连工具类，基于HBase-Spark API开发
  * 具有更强大的性能和更低的资源开销，适用于
  * 与Spark相结合的大数据量操作，优点体现在并行
  * 和大数据量。如果数据量不大，仍推荐使用
  * HBaseOper进行相关的操作
  *
  * @param sc
  * SparkContext实例
  * @param config
  * HBase相关配置参数
  * @author ChengLong 2018年4月10日 10:39:28
  */
class HBaseContextExt(@scala.transient sc: SparkContext, @scala.transient config: Configuration = HBaseOper.getConfiguration) extends HBaseContext(sc, config) {
  lazy val batchSize = GlobalConstants.HBaseConf.hbaseBatchSize

  /**
    * 根据RDD[String]批量删除，rdd是rowkey的集合
    * 类型为String
    *
    * @param rdd
    * 类型为String的RDD数据集
    * @param tableName
    * HBase表名
    * @param batchSize
    * 批量删除的大小，默认为10000条
    */
  def bulkDeleteRDD(tableName: String, rdd: RDD[String], batchSize: Integer = this.batchSize): Unit = {
    ParamUtils.requireNonNullForce(tableName, "表名不能为空")
    ParamUtils.requireNonNull(rdd, "rdd不能为空")
    ParamUtils.requireNonNull(batchSize, "批次大小不能为空")

    val rowKeyRDD = rdd.filter(rowkey => StringUtils.isNotBlank(rowkey)).map(rowKey => Bytes.toBytes(rowKey))
    this.bulkDelete[Array[Byte]](rowKeyRDD, TableName.valueOf(tableName), rec => new Delete(rec), batchSize)
  }

  /**
    * 根据Dataset[String]批量删除，Dataset是rowkey的集合
    * 类型为String
    *
    * @param dataset
    * 类型为String的Dataset集合
    * @param tableName
    * HBase表名
    * @param batchSize
    * 批量删除的大小，默认为10000条
    */
  def bulkDeleteDS(tableName: String, dataset: Dataset[String], batchSize: Integer = this.batchSize): Unit = {
    ParamUtils.isNotBlank(dataset, "dataset不能为空")
    this.bulkDeleteRDD(tableName, dataset.rdd, batchSize)
  }

  /**
    * 指定rowkey集合，进行批量删除操作内部会将这个集合转为RDD
    * 推荐在较大量数据时使用，小数据量的删除操作仍推荐使用HBaseOper
    *
    * @param tableName
    * HBase表名
    * @param seq
    * 待删除的rowKey集合
    */
  def bulkDeleteList(tableName: String, seq: Seq[String]): Unit = {
    ParamUtils.isNotBlank(seq, "seq不能为空")
    val rdd = sc.parallelize(seq, math.max(1, math.min(seq.length / 2, GlobalConstants.SparkConf.parallelism)))
    this.bulkDeleteRDD(tableName, rdd)
  }

  /**
    * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
    *
    * @param tableName
    * HBase表名
    * @param rdd
    * rowKey集合，类型为RDD[String]
    * @param clazz
    * 获取后的记录转换为目标类型（自定义的JavaBean类型）
    * @param batchSize
    * 用于指定一次获取多少条记录，默认10000条
    * @tparam E
    * 自定义JavaBean类型，必须继承自HBaseBaseBean
    * @return
    * 自定义JavaBean的对象结果集
    */
  def bulkGetRDD[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E], batchSize: Integer = this.batchSize): RDD[E] = {
    ParamUtils.requireNonNullForce(tableName, "参数不合法：表名不能为空")
    ParamUtils.requireNonNull(rdd, "参数不合法：rdd不能为空")
    ParamUtils.requireNonNull(clazz, "参数不合法：clazz不能为空")
    ParamUtils.requireNonNull(batchSize, "参数不合法：批次大小不能为空")

    val rowKeyRDD = rdd.filter(StringUtils.isNotBlank(_)).map(rowKey => Bytes.toBytes(rowKey))
    this.bulkGet[Array[Byte], E](TableName.valueOf(tableName), batchSize, rowKeyRDD, rowKey => new Get(rowKey), (result: Result) => {
      HBaseOper.hbaseRow2Bean(result, clazz)
    }).filter(bean => bean != null)
  }

  /**
    * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
    *
    * @param tableName
    * HBase表名
    * @param rdd
    * rowKey集合，类型为RDD[String]
    * @param clazz
    * 获取后的记录转换为目标类型（自定义的JavaBean类型）
    * @param batchSize
    * 用于指定一次获取多少条记录，默认10000条
    * @tparam E
    * 自定义JavaBean类型，必须继承自HBaseBaseBean
    * @return
    * 自定义JavaBean的对象结果集
    */
  def bulkGetDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E], batchSize: Integer = this.batchSize): DataFrame = {
    val resultRdd = this.bulkGetRDD[E](tableName, rdd, clazz, batchSize)
    val sqlContext = SingletonFactory.getSQLContextInstance(this.sc)
    sqlContext.createDataFrame(resultRdd, clazz)
  }

  /**
    * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
    *
    * @param tableName
    * HBase表名
    * @param rdd
    * rowKey集合，类型为RDD[String]
    * @param clazz
    * 获取后的记录转换为目标类型（自定义的JavaBean类型）
    * @param batchSize
    * 用于指定一次获取多少条记录，默认10000条
    * @tparam E
    * 自定义JavaBean类型，必须继承自HBaseBaseBean
    * @return
    * 自定义JavaBean的对象结果集
    */
  def bulkGetDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, rdd: RDD[String], clazz: Class[E], batchSize: Integer = this.batchSize): Dataset[E] = {
    val resultRdd = this.bulkGetRDD[E](tableName, rdd, clazz, batchSize)
    val sqlContext = SingletonFactory.getSQLContextInstance(this.sc)
    sqlContext.createDataset(resultRdd)(Encoders.bean(clazz))
  }

  /**
    * 根据rowKey集合批量获取数据，并映射为自定义的JavaBean类型
    * 内部实现是将rowkey集合转为RDD[String]，推荐在数据量较大
    * 时使用。数据量较小请优先使用HBaseOper
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * 具体类型
    * @param seq
    * rowKey集合
    * @tparam E
    * 自定义JavaBean类型，必须继承自HBaseBaseBean
    * @return
    * 自定义JavaBean的对象结果集
    */
  def bulkGetSeq[E <: HBaseBaseBean[E] : ClassTag](tableName: String, seq: Seq[String], clazz: Class[E]): RDD[E] = {
    ParamUtils.requireNonNull(seq, "参数不合法：seq不能为空")

    val rdd = sc.parallelize(seq, math.max(1, math.min(seq.length / 2, GlobalConstants.SparkConf.parallelism)))
    this.bulkGetRDD(tableName, rdd, clazz)
  }

  /**
    * 批量写入，将自定义的JavaBean数据集批量并行写入
    * 到HBase的指定表中。内部会将自定义JavaBean的相应
    * 字段一一映射为Put对象，并完成一次写入
    *
    * @param tableName
    * HBase表名
    * @param rdd
    * 数据集合，数类型需继承自HBaseBaseBean
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 数据类型为HBaseBaseBean的子类
    */
  def bulkPutRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    ParamUtils.requireNonNull(insertEmpty, "参数不合法：insertEmpty不能为空")
    ParamUtils.requireNonNull(multiVersion, "参数不合法：multiVersion不能为空")

    this.bulkPut[T](rdd,
      TableName.valueOf(tableName),
      (putRecord: T) => {
        HBaseOper.convert2Put(if (multiVersion) new MultiVersionsBean(putRecord) else putRecord, insertEmpty)
      })
  }

  /**
    * 批量写入，将自定义的JavaBean数据集批量并行写入
    * 到HBase的指定表中。内部会将自定义JavaBean的相应
    * 字段一一映射为Put对象，并完成一次写入。如果数据量
    * 较大，推荐使用。数据量过小则推荐使用HBaseOper
    *
    * @param tableName
    * HBase表名
    * @param seq
    * 数据集，类型为HBaseBaseBean的子类
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 对象类型必须是HBaseBaseBean的子类
    */
  def bulkPutSeq[T <: HBaseBaseBean[T] : ClassTag](tableName: String, seq: Seq[T], insertEmpty: Boolean, multiVersion: Boolean = false): Unit = {
    ParamUtils.requireNonNull(seq, "参数不合法：seq不能为空")

    val rdd = this.sc.parallelize(seq, math.max(1, math.min(seq.length / 2, GlobalConstants.SparkConf.parallelism)))
    this.bulkPutRDD(tableName, rdd, insertEmpty, multiVersion)
  }

  /**
    * 定制化scan设置后从指定的表中scan数据
    * 并将scan到的结果集映射为自定义JavaBean对象
    *
    * @param tableName
    * HBase表名
    * @param scan
    * scan对象
    * @param clazz
    * 自定义JavaBean的Class对象
    * @tparam T
    * 对象类型必须是HBaseBaseBean的子类
    * @return
    * scan获取到的结果集，类型为RDD[T]
    */
  def bulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, scan: Scan, clazz: Class[T]): RDD[T] = {
    ParamUtils.requireNonNullForce(tableName, "参数不合法：表名不能为空")
    ParamUtils.requireNonNull(scan, "参数不合法：scan不能为空")
    ParamUtils.requireNonNull(clazz, "参数不合法：clazz不能为空")

    if (scan.getCaching == -1) {
      scan.setCaching(this.batchSize)
    }
    this.hbaseRDD(TableName.valueOf(tableName), scan).mapPartitions(it => HBaseOper.hbaseRow2BeanList(it, clazz))
  }

  /**
    * 指定startRow和stopRow后自动创建scan对象完成数据扫描
    * 并将scan到的结果集映射为自定义JavaBean对象
    *
    * @param tableName
    * HBase表名
    * @param startRow
    * rowkey的起始
    * @param stopRow
    * rowkey的结束
    * @param clazz
    * 自定义JavaBean的Class对象
    * @tparam T
    * 对象类型必须是HBaseBaseBean的子类
    * @return
    * scan获取到的结果集，类型为RDD[T]
    */
  def bulkScanRDD[T <: HBaseBaseBean[T] : ClassTag](tableName: String, startRow: String, stopRow: String, clazz: Class[T]): RDD[T] = {
    ParamUtils.requireNonNullForce(startRow, "参数不合法：startRow不能为空")
    ParamUtils.requireNonNullForce(stopRow, "参数不合法：stopRow不能为空")

    this.bulkScanRDD(tableName, HBaseOper.buildScan(startRow, stopRow), clazz)
  }

  /**
    * 批量写入，将自定义的JavaBean数据集批量并行写入
    * 到HBase的指定表中。内部会将自定义JavaBean的相应
    * 字段一一映射为Put对象，并完成一次写入
    *
    * @param tableName
    * HBase表名
    * @param dataFrame
    * dataFrame实例，数类型需继承自HBaseBaseBean
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 数据类型为HBaseBaseBean的子类
    */
  def bulkPutDF[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    ParamUtils.requireNonNull(dataFrame, "参数不合法：dataFrame不能为空")

    val rdd = dataFrame.rdd.mapPartitions(it => SparkUtils.sparkRowToBean(it, clazz))
    this.bulkPutRDD[T](tableName, rdd, insertEmpty, multiVersion)
  }

  /**
    * 批量写入，将自定义的JavaBean数据集批量并行写入
    * 到HBase的指定表中。内部会将自定义JavaBean的相应
    * 字段一一映射为Put对象，并完成一次写入
    *
    * @param tableName
    * HBase表名
    * @param dataset
    * dataFrame实例，数类型需继承自HBaseBaseBean
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 数据类型为HBaseBaseBean的子类
    */
  def bulkPutDS[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dataset: Dataset[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    ParamUtils.requireNonNull(dataset, "参数不合法：dataset不能为空")

    this.bulkPutRDD[T](tableName, dataset.rdd, insertEmpty, multiVersion)
  }

  /**
    * 用于已经映射为指定类型的DStream实时
    * 批量写入至HBase表中
    *
    * @param tableName
    * HBase表名
    * @param dstream
    * 类型为自定义JavaBean的DStream流
    * @param insertEmpty
    * 对象中值为空的字段是否覆盖HBase中已有的field值
    * 默认为覆盖
    * @tparam T
    * 对象类型必须是HBaseBaseBean的子类
    */
  def bulkPutStream[T <: HBaseBaseBean[T] : ClassTag](tableName: String, dstream: DStream[T], insertEmpty: Boolean = true, multiVersion: Boolean = false): Unit = {
    ParamUtils.requireNonNullForce(tableName, "参数不合法：表名不能为空")
    ParamUtils.requireNonNull(dstream, "参数不合法：dstream不能为空")

    this.streamBulkPut[T](dstream, TableName.valueOf(tableName), (putRecord: T) => {
      HBaseOper.convert2Put(if (multiVersion) new MultiVersionsBean(putRecord) else putRecord, insertEmpty)
    })
  }

  /**
    * 以spark 方式批量将rdd数据写入到hbase中
    *
    * @param rdd
    * 类型为HBaseBaseBean子类的rdd
    * @param tableName
    * hbase表名
    * @param insertEmpty
    * 为空的字段是否写入hbase
    * @tparam T
    * 数据类型
    */
  def hadoopPut[T <: HBaseBaseBean[T] : ClassTag](tableName: String, rdd: RDD[T], insertEmpty: Boolean = true): Unit = {
    ParamUtils.requireNonNullForce(tableName, "参数不合法：表名不能为空")
    ParamUtils.requireNonNull(rdd, "参数不合法：rdd不能为空")
    ParamUtils.requireNonNull(insertEmpty, "参数不合法：insertEmpty不能为空")

    rdd.mapPartitions(it => {
      val putList = ListBuffer[(ImmutableBytesWritable, Put)]()
      it.foreach(t => {
        putList += Tuple2(new ImmutableBytesWritable(), HBaseOper.convert2Put(t, insertEmpty))
      })
      putList.iterator
    }).saveAsNewAPIHadoopDataset(this.getConfiguration(tableName))
  }

  /**
    * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param clazz
    * JavaBean类型，为HBaseBaseBean的子类
    */
  def hadoopPutDF[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataFrame: DataFrame, clazz: Class[E], insertEmpty: Boolean = true): Unit = {
    ParamUtils.requireNonNull(dataFrame, "参数不合法：dataFrame不能为空")

    val rdd = dataFrame.rdd.mapPartitions(it => SparkUtils.sparkRowToBean(it, clazz))
    this.hadoopPut[E](tableName, rdd, insertEmpty)
  }

  /**
    * 使用spark API的方式将DataFrame中的数据分多个批次插入到HBase中
    *
    * @param tableName
    * HBase表名
    * @param dataset
    * JavaBean类型，待插入到hbase的数据集
    */
  def hadoopPutDS[E <: HBaseBaseBean[E] : ClassTag](tableName: String, dataset: Dataset[E], insertEmpty: Boolean = true): Unit = {
    ParamUtils.requireNonNull(dataset, "参数不合法：dataset不能为空")

    this.hadoopPut[E](tableName, dataset.rdd, insertEmpty)
  }

  /**
    * 以spark 方式批量将DataFrame数据写入到hbase中
    * 注：此方法与hbaseHadoopPutDF不同之处在于，它不强制要求该DataFrame一定要与HBaseBaseBean的子类对应
    * 但需要指定rowKey的构建规则，相对与hbaseHadoopPutDF来说，少了中间的两次转换，性能会更高
    *
    * @param df
    * spark的DataFrame
    * @param tableName
    * hbase表名
    * @param insertEmpty
    * 为空的字段是否写入hbase
    * @tparam T
    * JavaBean类型
    */
  def hadoopPutDFRow[T <: HBaseBaseBean[T] : ClassTag](tableName: String, df: DataFrame, buildRowKey: (Row) => String, insertEmpty: Boolean = true): Unit = {
    ParamUtils.requireNonNullForce(tableName, "参数不合法：表名不能为空")
    ParamUtils.requireNonNull(df, "参数不合法：dataFrame不能为空")
    ParamUtils.requireNonNull(insertEmpty, "参数不合法：insertEmpty不能为空")

    val fields = df.schema.fields
    df.rdd.mapPartitions(it => {
      val putList = ListBuffer[(ImmutableBytesWritable, Put)]()
      it.foreach(row => {
        val put = new Put(Bytes.toBytes(buildRowKey(row)))
        fields.foreach(field => {
          val fieldName = field.name
          val fieldIndex = row.fieldIndex(fieldName)
          val dataType = field.dataType.getClass.getSimpleName
          var fieldValue: Any = null
          if (!row.isNullAt(fieldIndex)) {
            fieldValue = row.get(fieldIndex)
            if (dataType.contains("StringType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.String]))
            } else if (dataType.contains("IntegerType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Integer]))
            } else if (dataType.contains("DoubleType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Double]))
            } else if (dataType.contains("LongType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Long]))
            } else if (dataType.contains("DecimalType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.math.BigDecimal]))
            } else if (dataType.contains("FloatType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Float]))
            } else if (dataType.contains("BooleanType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Boolean]))
            } else if (dataType.contains("ShortType")) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), Bytes.toBytes(fieldValue.asInstanceOf[java.lang.Short]))
            } else if (dataType.contains("NullType") && insertEmpty) {
              put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), null)
            }
          } else if (insertEmpty) {
            put.addColumn(Bytes.toBytes("info"), Bytes.toBytes(fieldName), null)
          }
        })
        putList += Tuple2(new ImmutableBytesWritable, put)
      })
      putList.iterator
    }).saveAsNewAPIHadoopDataset(this.getConfiguration(tableName))
  }

  /**
    * 根据表名构建hadoop configuration
    *
    * @param tableName
    * HBase表名
    * @return
    * hadoop configuration
    */
  private def getConfiguration(tableName: String): Configuration = {
    ParamUtils.requireNonNullForce(tableName, "参数不合法：表名不能为空")

    val hadoopConfiguration = HBaseOper.getConfiguration
    hadoopConfiguration.set(TableOutputFormat.OUTPUT_TABLE, tableName)
    val job = Job.getInstance(hadoopConfiguration)
    job.setOutputKeyClass(classOf[ImmutableBytesWritable])
    job.setOutputValueClass(classOf[Result])
    job.setOutputFormatClass(classOf[TableOutputFormat[ImmutableBytesWritable]])
    job.getConfiguration()
  }

  /*
    /**
      * 将大批量的数据直接生成HFile并上传至相应的
      * RegionServer中，适用于海量数据写入到HBase的场景
      * 此种方式的优点是降低了RegionServer的cpu和内存开销
      * 原理是Spark遵循HBase的HFile规范直接生成HFile，并
      * 上传到RegionServer中
      *
      * @param tableName
      * HBase表名
      * @param rdd
      * rdd数据集，类型为继承自HBaseBaseBean的自定义JavaBean
      * @param stagingDir
      * 用于存放Spark生成的HFile的临时本地路径，非HDFS路径
      * 上传至RegionServer后会自动删除该HFile文件
      * @param insertEmpty
      * 对象中值为空的字段是否覆盖HBase中已有的field值
      * 默认为覆盖
      * @tparam T
      * 对象类型必须是HBaseBaseBean的子类
      */
    def bulkLoadThinRows[T <: HBaseBaseBean[T] : ClassTag](tableName: String,
                                                           rdd: RDD[T],
                                                           stagingDir: String, insertEmpty: Boolean = true): Unit = {

      val hbaseTable = TableName.valueOf(tableName)
      var map: collection.mutable.Map[String, Field] = null

      /**
        * 将自定义JavaBean转为HFile所需的格式
        *
        * @param bean
        * RDD中的每条记录
        * @return
        */
      def bean2QualifiersValues(bean: HBaseBaseBean[T]): (ByteArrayWrapper, FamiliesQualifiersValues) = {
        val rowKey = if (StringUtils.isNotBlank(bean.getRowKey)) bean.getRowKey
        else {
          val tmpBean = bean.buildRowKey()
          tmpBean.getRowKey
        }

        val familyQualifiersValues = new FamiliesQualifiersValues
        if (map == null) {
          map = ReflectionUtils.getAllFields(bean.getClass).toScalaMap
        }

        map.foreach(t => {
          var fName: String = t._1
          val field = t._2
          val anno: FieldName = field.getAnnotation(classOf[FieldName])
          var familyName: String = GlobalConstants.familyName
          if (anno != null) {
            if (!anno.disuse()) {
              if (StringUtils.isNotBlank(anno.value)) fName = anno.value
              if (StringUtils.isNotBlank(anno.family)) familyName = anno.family
            }
          }
          field.setAccessible(true)
          val fieldType: Type = field.getType
          val objValue = field.get(bean)
          if (objValue != null) {
            val objValueStr: String = objValue.toString
            val valueArray: Array[Byte] = if (fieldType eq classOf[String]) Bytes.toBytes(objValueStr)
            else if (fieldType eq classOf[Integer]) Bytes.toBytes(Integer.parseInt(objValueStr))
            else if (fieldType eq classOf[Double]) Bytes.toBytes(java.lang.Double.parseDouble(objValueStr))
            else if (fieldType eq classOf[Long]) Bytes.toBytes(java.lang.Long.parseLong(objValueStr))
            else if (fieldType eq classOf[BigDecimal]) Bytes.toBytes(new BigDecimal(objValueStr))
            else if (fieldType eq classOf[Float]) Bytes.toBytes(java.lang.Float.parseFloat(objValueStr))
            else if (fieldType eq classOf[Boolean]) Bytes.toBytes(java.lang.Boolean.parseBoolean(objValueStr))
            else if (fieldType eq classOf[Short]) Bytes.toBytes(java.lang.Short.parseShort(objValueStr))
            else null
            familyQualifiersValues += (Bytes.toBytes(familyName), Bytes.toBytes(fName), valueArray)
          } else {
            if (insertEmpty) {
              familyQualifiersValues += (Bytes.toBytes(familyName), Bytes.toBytes(fName), null)
            }
          }
        })
        (new ByteArrayWrapper(Bytes.toBytes(rowKey)), familyQualifiersValues)
      }

      this.bulkLoadThinRows[T](rdd,
        hbaseTable,
        record => bean2QualifiersValues(record),
        stagingDir,
        new util.HashMap[Array[Byte], FamilyHFileWriteOptions],
        false,
        HConstants.DEFAULT_MAX_FILE_SIZE)

      val load = new LoadIncrementalHFiles(this.config)
      val conn = ConnectionFactory.createConnection(this.config)
      load.doBulkLoad(new Path(stagingDir), conn.getAdmin, conn.getTable(hbaseTable),
        conn.getRegionLocator(hbaseTable))
    }*/

  /*
    /**
      * 将HBase表映射为DataFrame
      * @param clazz
      *              表对应的JavaBean
      * @tparam T
      * @return
      */
    def hbase2DataFrame[T <: HBaseBaseBean[T] : ClassTag](clazz: Class[T]): DataFrame = {
      val sqlContext = SingletonFactory.getSQLContextInstance(sc)
      sqlContext.read.options(Map(HBaseTableCatalog.tableCatalog -> HBaseBaseBean.catalog(clazz.newInstance()))).format("org.apache.hadoop.hbase.spark").load()
    }

    /**
      * 将HBase表映射为DataFrame
      * @param tableName
      *            HBase表名
      * @tparam T
      * @return
      */
    def hbase2DataFrame[T <: HBaseBaseBean[T] : ClassTag](tableName: String): DataFrame = {
      if(StringUtils.isBlank(tableName)) throw new IllegalArgumentException("HBase表名不能为空")
      val sqlContext = SingletonFactory.getSQLContextInstance(sc)
      sqlContext.read.options(Map(HBaseTableCatalog.tableCatalog -> tableName)).format("org.apache.hadoop.hbase.spark").load()
    }

    /**
      * 对HBase表执行sql运算
      * @param sql
      *            待执行的sql语句，sql中引用的表名需与tableName参数所指定的一致
      * @param clazz
      *              HBase表对应的JavaBean类型
      * @param tableName
      *                  Spark SQL临时表名，如果为空，则取JavaBean中的tableName注解
      * @tparam T
      * @return
      *         结果集
      */
    def sql[T <: HBaseBaseBean[T] : ClassTag](sql: String, clazz: Class[T], tableName: String = ""): DataFrame = {
      val hbaseDF = this.hbase2DataFrame(clazz)
      val tmpTablename = if(StringUtils.isNotBlank(tableName)) {
        tableName
      } else {
        val fieldName = ReflectionUtils.getClassAnnotation(clazz, classOf[FieldName])
        if (fieldName == null || StringUtils.isBlank(fieldName.asInstanceOf[FieldName].tableName())) {
          throw new IllegalArgumentException("@FieldName必须指定，且tableName不能为空")
        }
        fieldName.asInstanceOf[FieldName].tableName
      }

      hbaseDF.registerTempTable(tmpTablename)
      val sqlContext = SingletonFactory.getSQLContextInstance(sc)
      sqlContext.sql(sql)
    }

*/
}