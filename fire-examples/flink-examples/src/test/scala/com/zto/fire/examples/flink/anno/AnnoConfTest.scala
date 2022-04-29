package com.zto.fire.examples.flink.anno

import com.zto.fire.common.conf.FireHiveConf
import com.zto.fire.common.enu.JdbcDriver
import com.zto.fire.core.anno._
import com.zto.fire.flink.BaseFlinkStreaming
import com.zto.fire.flink.anno.Checkpoint
import com.zto.fire.hbase.conf.FireHBaseConf
import org.junit.Test

/**
 * 基于Fire注解进行任务参数设置
 */
@Hive(value = "test", catalog = "hive_catalog", version = "1.1.1", partition = "dt")
@HBase(value = "batch-new1", durability = "off", scanPartitions = 12, config = Array("hbase.zookeeper.property.clientPort=2181", "zookeeper.znode.parent = /hbase"))
@HBase2(value = "batch-new2", tableMetaCache = false, batchSize = 10, storageLevel = "memory_only", config = Array("hbase.zookeeper.property.clientPort=2182", "zookeeper.znode.parent = /hbase2"))
@HBase3(value = "batch-new3", family = "data", maxRetries = 5, config = Array("hbase.zookeeper.property.clientPort=2183", "zookeeper.znode.parent = /hbase3"))
@Checkpoint(interval = 100, unaligned = true)
@Kafka(brokers = "kafka", topics = "fire", groupId = "fire")
@RocketMQ(brokers = "rocketmq", topics = "fire", groupId = "fire")
@Jdbc(url = "jdbc:mysql://10.9.46.xxx:3306", driver = JdbcDriver.mysql, username = "root", password = "root")
class AnnoConfTest extends BaseFlinkStreaming {

  /**
   * hive 注解断言
   */
  @Test
  def assertHive: Unit = {
    assert(FireHiveConf.hiveCluster.equals("test"))
    assert(FireHiveConf.hiveVersion.equals("1.1.1"))
    assert(FireHiveConf.hiveCatalogName.equals("hive_catalog"))
    assert(FireHiveConf.partitionName.equals("dt"))
    this.logInfo("assert hive annotation success.")
  }

  /**
   * hbase 注解断言
   */
  @Test
  def assertHBase: Unit = {
    assert(FireHBaseConf.hbaseCluster().equals("batch-new1"))
    assert(FireHBaseConf.hbaseCluster(2).equals("batch-new2"))
    assert(FireHBaseConf.hbaseCluster(3).equals("batch-new3"))

    assert(FireHBaseConf.hbaseDurability(1).equals("off"))
    assert(!FireHBaseConf.tableExistsCache(2))
    assert(FireHBaseConf.familyName(3).equals("data"))

    assert(FireHBaseConf.hbaseHadoopScanPartitions() == 12)
    assert(FireHBaseConf.hbaseBatchSize() == 10000)
    assert(FireHBaseConf.hbaseBatchSize(2) == 10)
    assert(FireHBaseConf.hbaseMaxRetry(3) == 5)
    assert(FireHBaseConf.hbaseMaxRetry(2) == 3)
    assert(FireHBaseConf.hbaseStorageLevel(2).equals("MEMORY_ONLY"))

    assert(this.conf.getString("flink.fire.hbase.conf.hbase.zookeeper.property.clientPort").equals("2181"))
    assert(this.conf.getString("fire.hbase.conf.zookeeper.znode.parent2").equals("/hbase2"))
    assert(this.conf.getString("flink.fire.hbase.conf.hbase.zookeeper.property.clientPort3").equals("2183"))
  }
}
