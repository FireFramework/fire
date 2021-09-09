package com.zto.fire.examples.flink

import com.zto.fire._
import com.zto.fire.common.util.UnitFormatUtils.DateUnitEnum
import com.zto.fire.common.util.{DateFormatUtils, UnitFormatUtils}
import org.apache.flink.runtime.checkpoint.{Checkpoints, OperatorSubtaskState}
import org.apache.flink.runtime.state.IncrementalRemoteKeyedStateHandle
import org.apache.flink.runtime.state.filesystem.FileStateHandle
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, LocatedFileStatus, Path}
import org.slf4j
import org.slf4j.LoggerFactory

import java.io.{BufferedInputStream, DataInputStream, File, FileInputStream}
import java.net.URI
import java.util.Date
import scala.collection.mutable.ListBuffer

/**
 * flink历史失效状态清理工具
 * 清理策略：
 * conservativeModel：筛选出不再使用的checkpoint文件，将这些文件归档至指定的目录中，并定期删除指定时间的数据
 * 直接删除模式：直接删除不再需要的checkpoint文件
 *
 * @author ChengLong 2021-9-6 15:06:21
 */
object StateCleaner {
  private val logger: slf4j.Logger = LoggerFactory.getLogger(this.getClass)
  private val hdfs = "hdfs://10.7.69.237:8020"
  private val checkpointDir = "/user/flink/checkpoint"
  private val hdfsUser = "hadoop"
  private val localCheckpointBaseDir = "D:/home/checkpoint"
  private val recycleBin = "/user/flink/recyclebin"
  // 用于存放当前线上flink任务需要使用到的状态绝对路径
  private val inuserSet = new JHashSet[String]()
  // download到本地的metadata文件是否采用覆盖的方式避免本地磁盘存放过多的文件
  private val overwrite = true
  // 是否将失效的状态文件移到到回收站，等待后续清理
  private val conservativeModel = true
  // 用于存放遍历的checkpoint文件，避免二次遍历导致漏分析的文件被标记为删除
  private val files = ListBuffer[LocatedFileStatus]()
  // 默认清理多少天之前的归档checkpoint文件
  private val ttl = 31
  // 用于指定是否删除过期的checkpoint归档文件
  private val doDelete = true

  /**
   * 删除过期的归档文件
   */
  def delete(): Unit = {
    if (!this.doDelete || !this.conservativeModel) return
    val ttlDay = DateFormatUtils.formatDate(DateFormatUtils.formatDateTime(DateFormatUtils.addDays(new Date, -this.ttl)))
    val ttlPath = s"${this.recycleBin}/${ttlDay}/"
    var fs: FileSystem = null
    tryFinally {
      fs = this.getFileSystem
      this.logger.warn(s"开始删除checkpoint归档目录：$ttlPath ...")
      fs.delete(new Path(ttlPath), true)
    }(if (fs != null) fs.close())(this.logger, s"删除checkpoint归档目录成功，路径：$ttlPath", s"删除checkpoint归档目录失败，路径：$ttlPath", "FileSystem.close()失败")
  }

  /**
   * 解析 operatorSubtaskState 的 ManagedKeyedState
   *
   * @param operatorSubtaskState operatorSubtaskState
   */
  def parseManagedKeyedState(operatorSubtaskState: OperatorSubtaskState): Unit = {
    if (noEmpty(operatorSubtaskState)) {
      // 本案例针对 Flink RocksDB 的增量 Checkpoint 引发的问题，
      // 因此仅处理 IncrementalRemoteKeyedStateHandle
      operatorSubtaskState.getManagedKeyedState.filter(_.isInstanceOf[IncrementalRemoteKeyedStateHandle])
        .map(_.asInstanceOf[IncrementalRemoteKeyedStateHandle]).foreach(keyedStateHandle => {
        // 获取 RocksDB 的 sharedState
        val sharedState = keyedStateHandle.getSharedState
        if (noEmpty(sharedState)) {
          sharedState.map(t => t._2).filter(_.isInstanceOf[FileStateHandle]).map(_.asInstanceOf[FileStateHandle])
            .foreach(t => {
              val filePath = t.getFilePath
              this.inuserSet.add(filePath.getPath)
              this.logger.warn(s"sstable 文件对应的 hdfs 位置：${filePath}")
            })
        }
      })
    }
  }

  /**
   * 解析 operatorSubtaskState 的 ManagedOperatorState
   *
   * @param operatorSubtaskState operatorSubtaskState
   */
  def parseManagedOperatorState(operatorSubtaskState: OperatorSubtaskState): Unit = {
    if (isEmpty(operatorSubtaskState)) {
      operatorSubtaskState.getManagedOperatorState.map(_.getDelegateStateHandle).filter(_.isInstanceOf[FileStateHandle]).map(_.asInstanceOf[FileStateHandle]).foreach(fileStateHandle => {
        val filePath = fileStateHandle.getFilePath
        this.inuserSet.add(filePath.getPath)
        this.logger.warn(s"ManagedOperatorState 路径：${filePath}")
      })
    }
  }

  /**
   * 递归遍历checkpoint目录下所有的_metadata文件
   */
  def recursionCheckpointDir(): Unit = {
    var count = 0
    var fs: FileSystem = null
    tryFinally {
      fs = this.getFileSystem
      val path = new Path(this.checkpointDir)
      val it = fs.listFiles(path, true)

      while (it.hasNext) {
        val status = it.next()
        this.files += status
        if (status.getPath.getName.endsWith("_metadata")) {
          // 获取metadata在hdfs上的相对路径
          val metadataPath = status.getPath.toString.replace(this.hdfs, "")
          this.inuserSet.add(metadataPath)
          this.logger.warn(s"开始分析metadata文件：${metadataPath}")

          // 是否复用同一个本地元数据的路径，如果复用，则分析完成后就会被下一个元数据文件覆盖，否则会保留所有的metadata文件
          val localPath = if (this.overwrite) this.localCheckpointBaseDir + "/_metadata" else this.localCheckpointBaseDir + metadataPath
          // 将metadata文件拷贝到本地进行分析
          fs.copyToLocalFile(status.getPath, new Path(localPath))
          this.analyzeMetadata(localPath)
          count += 1
        }
      }
      this.logger.warn(s"此次分析metadata文件数共计：${count}")
    }(if (fs != null) fs.close())(this.logger, catchLog = "分析metadata文件发生异常", finallyCatchLog = "FileSystem.close()失败")
  }

  /**
   * 获取HDFS的FileSystem对象
   */
  def getFileSystem: FileSystem = {
    val fs = FileSystem.get(new URI(this.hdfs), new Configuration(), this.hdfsUser)
    fs.setWorkingDirectory(new Path("/"))
    fs
  }

  /**
   * 清理不再被使用的状态数据
   */
  def clean(): Unit = {
    var count = 0
    var blockSize = 0L
    var fs: FileSystem = null

    tryFinally {
      fs = this.getFileSystem
      val newFilePath = new Path(s"${this.recycleBin}/${DateFormatUtils.formatCurrentDate()}")
      fs.mkdirs(newFilePath)

      this.files.foreach(status => {
        val currentFile = status.getPath.toString.replace(this.hdfs, "")
        if (!this.inuserSet.contains(currentFile)) {
          if (this.conservativeModel) {
            // 保守模式下仅将过期的状态文件移动至指定的文件夹中，等待后续的单独处理
            val subPath = status.getPath.getParent.toString.replace(this.hdfs, "").replace(this.checkpointDir + "/", "")
            val destPath = new Path(s"${this.recycleBin}/${DateFormatUtils.formatCurrentDate()}/$subPath")
            fs.mkdirs(destPath)
            fs.rename(status.getPath, destPath)
            this.logger.warn(s"移动状态文件：${status.getPath.toString} to ${destPath.toString}")
          } else {
            // 非保守模式下，直接删除失效的状态文件
            fs.delete(status.getPath, true)
            this.logger.warn(s"删除状态文件：${status.getPath}")
          }
          count += 1
          blockSize += status.getBlockSize
        }
      })

      this.logger.warn(s"清理过期文件数：${count}，释放磁盘空间：${UnitFormatUtils.readable(blockSize, DateUnitEnum.BYTE)}")
    }(if (fs != null) fs.close())(this.logger, catchLog = "删除/归档checkpoint文件过程中发生异常", finallyCatchLog = "FileSystem.close()失败")
  }

  /**
   * 通过解析指定的_metadata分析还在被使用的checkpoint文件
   *
   * @param path
   * metadata的绝对路径
   */
  def analyzeMetadata(path: String): Unit = {
    //  读取元数据文件
    val metadataFile = new File(path)
    var fis: FileInputStream = null
    var bis: BufferedInputStream = null
    var dis: DataInputStream = null

    tryFinally {
      // 通过IO流获取本地的metadata文件
      fis = new FileInputStream(metadataFile)
      bis = new BufferedInputStream(fis)
      dis = new DataInputStream(bis)

      val checkpointMetadata = Checkpoints.loadCheckpointMetadata(dis, this.getClass.getClassLoader, null)
      this.logger.warn(s"当前checkpoint id ${checkpointMetadata.getCheckpointId}")

      // 遍历 OperatorState，这里的每个 OperatorState 对应一个 Flink 任务的 Operator 算子
      // 不要与 OperatorState  和 KeyedState 混淆，不是一个层级的概念
      checkpointMetadata.getOperatorStates.filter(_.getStateSize > 0).foreach(operatorState => {
        this.logger.warn(s"算子状态：${operatorState}")
        // 遍历当前算子的所有 subtask
        operatorState.getStates.foreach(operatorSubtaskState => {
          // 解析 operatorSubtaskState 的 ManagedKeyedState
          this.parseManagedKeyedState(operatorSubtaskState)
          // 解析 operatorSubtaskState 的 ManagedOperatorState
          this.parseManagedOperatorState(operatorSubtaskState)
        })
      })
    }(if (dis != null) dis.close())(this.logger, catchLog = "解析metadata文件过程中出现异常", finallyCatchLog = "关闭IO流过程中出现异常")
  }


  def main(args: Array[String]): Unit = {
    elapsed[Unit]("Flink状态数据清理成功", this.logger) {
      this.recursionCheckpointDir()
      this.clean()
      this.delete()
    }
  }
}
