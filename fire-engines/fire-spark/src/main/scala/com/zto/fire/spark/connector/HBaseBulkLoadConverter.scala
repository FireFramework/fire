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

package com.zto.fire.spark.connector

import com.zto.fire.hbase.HBaseConnector
import com.zto.fire.hbase.bean.{HBaseBaseBean, MultiVersionsBean}
import org.apache.hadoop.hbase.CellUtil
import org.apache.hadoop.hbase.spark.KeyFamilyQualifier

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
 * BulkLoad 用的 Bean→Cell 转换器。
 * 独立可序列化类，避免闭包捕获 HBaseBulkConnector / HBaseBulkConnector$ 导致
 * yarn-client 下 Driver/Executor serialVersionUID 不一致。
 */
@SerialVersionUID(1L)
private[connector] class HBaseBulkLoadConverter[T <: HBaseBaseBean[T]](keyNum: Int)(implicit tag: ClassTag[T])
  extends (T => Iterator[(KeyFamilyQualifier, Array[Byte])]) with Serializable {

  override def apply(bean: T): Iterator[(KeyFamilyQualifier, Array[Byte])] = {
    val connector = HBaseConnector(keyNum = keyNum)
    val record = if (connector.getMultiVersion[T]) new MultiVersionsBean(bean).asInstanceOf[T] else bean
    val put = connector.convert2Put[T](record, connector.getNullable[T])
    put.getFamilyCellMap.values().asScala.flatMap(_.asScala).iterator.map { cell =>
      (new KeyFamilyQualifier(CellUtil.cloneRow(cell), CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell)),
        CellUtil.cloneValue(cell))
    }
  }
}
