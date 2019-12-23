package org.apache.spark.sql.execution.streaming.sinks.jdbc

import com.zto.fire.core.sink.JdbcStreamSink
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.execution.streaming.Sink
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode

/**
 * jdbc的Provider
 *
 * @author ChengLong 2019-12-23 15:56:56
 * @since 0.4.1
 */
class JdbcStreamSinkProvider extends StreamSinkProvider with DataSourceRegister {

  override def shortName(): String = "fire-jdbc"

  override def createSink(sqlContext: SQLContext,
                          parameters: Map[String, String],
                          partitionColumns: Seq[String],
                          outputMode: OutputMode): Sink = {
    new JdbcStreamSink(parameters)
  }
}