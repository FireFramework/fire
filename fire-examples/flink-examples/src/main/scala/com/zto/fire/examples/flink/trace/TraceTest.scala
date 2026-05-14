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

package com.zto.fire.examples.flink.trace

import com.zto.fire._
import com.zto.fire.common.anno.Config
import com.zto.fire.core.anno.connector._
import com.zto.fire.examples.bean.TraceTest
import com.zto.fire.flink.FlinkStreaming
import com.zto.fire.flink.anno.Streaming
import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.streaming.api.scala._

@Config(
  """
    |fire.lineage.debug.enable=false
    |fire.debug.class.code.resource=com.fasterxml.jackson.annotation.JsonInclude,com.fasterxml.jackson.annotation.JsonInclude$Value,com.fasterxml.jackson.databind.ObjectMapper,com.fasterxml.jackson.databind.cfg.ConfigOverrides,com.fasterxml.jackson.core.JsonFactory
    |fire.config_center.enable  =false
    |fire.rest.url.show.enable  =true
    |fire.rest.filter.enable    =false
    |fire.lineage.send.mq.enable=false
    |""")
@Streaming(interval = 10, disableOperatorChaining = true, parallelism = 2)
object TraceTest extends FlinkStreaming {

  override def process(): Unit = {
    val dstream = this.fire.createRandomIntStream(100)
    dstream.map(new RichMapFunction[Int, Int] {
      override def map(value: Int): Int = {
        val trace = new TraceTest
        trace.sleep(2000)
        trace.execute(101)
        value
      }
    }).addSink(println _)
  }
}