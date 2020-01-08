package com.zto.fire.flink.ext.core

import org.apache.flink.table.api.Table
import org.apache.flink.table.api.scala._

class TableExt(table: Table) {

  def show: Unit = {
    this.table.addSink(row => println(row))
  }
}
