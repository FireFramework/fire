package com.zto.fire.flink.ext.function

import org.apache.flink.api.common.functions.RuntimeContext
import org.slf4j.LoggerFactory

/**
 * RuntimeContext扩展
 *
 * @author ChengLong 2021-9-13 14:26:28
 * @since 2.2.0
 */
class RuntimeContextExt(runtimeContext: RuntimeContext) {
  protected lazy val logger = LoggerFactory.getLogger(this.getClass)

}
