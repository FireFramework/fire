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

package com.zto.fire.common.util

import com.zto.fire.predef._
import com.zto.fire.common.conf.FireHDFSConf._
import org.apache.hadoop.fs.{FSDataInputStream, FileSystem, Path}

import java.io.{BufferedReader, FileNotFoundException, InputStreamReader}
import java.net.URI

/**
 * HDFS工具类
 *
 * @author ChengLong 2022-11-04 10:23:38
 * @since 2.3.2
 */
object HDFSUtils extends Logging {

  /**
   * 执行hdfs相关命令
   * @param fun
   * 业务逻辑
   */
  def execute[T](fun: FileSystem => T, hdfsUrl: String = hdfsUrl, user: String = hdfsUser): T = {
    var fs: FileSystem = null
    try {
      fs = this.getFileSystem(hdfsUrl, user)
      fun(fs)
    } catch {
      case e: Throwable =>
        logError("执行hdfs命令发生异常", e)
        throw e
    } finally this.close(fs)
  }

  /**
   * 读取指定hdfs路径下的文本文件
   * @param path
   * hdfs文件路径
   * @return
   * 文本内容
   */
  def readTextFile(path: String): String = {
    if (isEmpty(path)) throw new IllegalArgumentException("文件路径不能为空")

    val sqlBuilder = new JStringBuilder()
    execute(fs => {
      var fsd: FSDataInputStream = null
      var reader: BufferedReader = null
      try {
        val filePath = new Path(path)
        if (!fs.exists(filePath) || fs.isDirectory(filePath)) throw new FileNotFoundException(s"文件路径不存在，请检查！$path")

        fsd = fs.open(new Path(path))
        reader = new BufferedReader(new InputStreamReader(fsd))
        var readLine = reader.readLine()
        while (readLine != null) {
          sqlBuilder.append(readLine + "\n")
          readLine = reader.readLine()
        }
      } catch {
        case e: Throwable =>
          logError(s"读取hdfs文件失败：$path", e)
      } finally {
        IOUtils.close(reader)
        if (fsd != null) fsd.close()
      }
    })
    sqlBuilder.toString
  }

  /**
   * 获取HDFS的FileSystem对象
   */
  def getFileSystem(hdfsUrl: String, hdfsUser: String): FileSystem = {
    val fs = FileSystem.get(new URI(hdfsUrl), hdfsConf, hdfsUser)
    fs.setWorkingDirectory(new Path("/"))
    fs
  }

  /**
   * 关闭FileSystem对象
   */
  def close(fs: FileSystem): Unit = {
    if (fs != null) {
      try {
        fs.close()
      } catch {
        case e: Throwable => logError("关闭FileSystem发生异常", e)
      }
    }
  }
}
