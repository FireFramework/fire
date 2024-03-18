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

package com.zto.fire.examples.flink

import com.zto.VersionTest
import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.common.enu.Operation
import com.zto.fire.common.lineage.parser.connector.MQDatasource
import com.zto.fire.core.anno.connector._
import com.zto.fire.core.anno.lifecycle.Process
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random


@Config(
  """
    |fire.lineage.debug.enable=true
    |""")
@Streaming(interval = 20, disableOperatorChaining = true, parallelism = 2)
@Kafka(brokers = "bigdata_test", topics = "fire", groupId = "fire")
object Test extends FlinkStreaming {

  @Process
  def kafkaSource: Unit = {
    println("version=" + VersionTest.version())
    val stream = this.fire.addSourceLineage(new SourceFunction[Int] {
      val random = new Random()
      override def run(ctx: SourceFunction.SourceContext[Int]): Unit = {
        while (true) {
          ctx.collect(random.nextInt(1000))
          Thread.sleep(1000)
        }
      }

      override def cancel(): Unit = ???
    })(MQDatasource("kafka", "localhost:9092", "fire", "fire"), Operation.SOURCE)


    stream.print()
  }
}