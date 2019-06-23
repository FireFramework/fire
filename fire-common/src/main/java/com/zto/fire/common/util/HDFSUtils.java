package com.zto.fire.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.*;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * HDFS工具类
 *
 * @author ChengLong
 * @date 2018年7月6日 10:51:29
 */
public class HDFSUtils {
    private Configuration conf;
    private FileSystem fileSystem;

    /**
     * 根据指定的hdfs路径:端口初始化FileSystem对象
     *
     * @param hdfsUrl
     */
    public HDFSUtils(String hdfsUrl) {
        this.conf = new Configuration();
        if (StringUtils.isNotBlank(hdfsUrl)) {
            try {
                this.conf.set("fs.defaultFS", hdfsUrl);
                this.fileSystem = FileSystem.get(this.conf);
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    public Configuration getConf() {
        return conf;
    }

    public FileSystem getFileSystem() {
        return fileSystem;
    }

    /**
     * 释放连接
     */
    public void close() {
        if (this.fileSystem != null) {
            try {
                this.fileSystem.close();
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 判断目录或文件是否存在
     */
    public boolean exist(String filePath) {
        if (StringUtils.isBlank(filePath)) {
            return false;
        }
        boolean exist = false;
        try {
            Path path = new Path(filePath);
            if (this.fileSystem.exists(path)) {
                exist = true;
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            return exist;
        }
    }

    /**
     * 创建文件夹
     *
     * @param path
     */
    public void mkdir(String path) {
        if (StringUtils.isBlank(path)) {
            return;
        }
        try {
            if (!this.exist(path)) {
                this.fileSystem.mkdirs(new Path(path));
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 删除文件或目录
     *
     * @param path
     */
    /*public void delete(String path, boolean recursive) {
        if (StringUtils.isBlank(path) || "/".equals(path)) {
            return;
        }
        try {
            if (this.exist(path)) {
                this.fileSystem.delete(new Path(path), recursive);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    /**
     * 根据filter获取目录下的所有文件
     *
     * @param path       目录路径
     * @param pathFilter 过滤器
     * @return String[]
     */
    public List<String> listFiles(String path, PathFilter pathFilter) {
        List<String> fileList = null;

        try {
            // 返回FileSystem对象
            FileStatus[] status;
            if (pathFilter != null) {
                // 根据filter列出目录内容
                status = this.fileSystem.listStatus(new Path(path), pathFilter);
            } else {
                // 列出目录内容
                status = this.fileSystem.listStatus(new Path(path));
            }

            // 获取目录下的所有文件路径
            Path[] listedPaths = FileUtil.stat2Paths(status);
            // 转换String[]
            if (listedPaths != null && listedPaths.length > 0) {
                fileList = new ArrayList<String>(listedPaths.length);
                for (int i = 0; i < status.length; i++) {
                    fileList.add(listedPaths[i].toString());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        return fileList;
    }

    /**
     * 文件重命名
     *
     * @param srcPath
     * @param dstPath
     */
    /*public boolean rename(String srcPath, String dstPath) {
        boolean flag = false;
        try {
            flag = this.fileSystem.rename(new Path(srcPath), new Path(dstPath));
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return flag;
        }
    }*/

    /**
     * 查找某个文件（非目录）在 HDFS集群的位置
     *
     * @param filePath
     * @return BlockLocation[]
     */
    public BlockLocation[] getFileBlockLocations(String filePath) {
        // 文件路径
        Path path = new Path(filePath);
        // 文件块位置列表
        BlockLocation[] blkLocations = new BlockLocation[0];
        try {
            if (this.fileSystem.isDirectory(path)) {
                return blkLocations;
            }
            // 获取文件目录
            FileStatus filestatus = this.fileSystem.getFileStatus(path);
            //获取文件块位置列表
            blkLocations = this.fileSystem.getFileBlockLocations(filestatus, 0, filestatus.getLen());
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return blkLocations;
        }
    }

    /**
     * 获取 HDFS 集群节点信息
     *
     * @return DatanodeInfo[]
     */
    /*public DatanodeInfo[] getHDFSNodes() {
        // 获取所有节点
        DatanodeInfo[] dataNodeStats = new DatanodeInfo[0];
        try {
            // 获取分布式文件系统
            DistributedFileSystem hdfs = (DistributedFileSystem) this.fileSystem;
            dataNodeStats = hdfs.getDataNodeStats();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            return dataNodeStats;
        }
    }*/

    /**
     * 文件上传至HDFS
     *
     * @param srcFile  待上传的本地文件
     * @param destPath HDFS目标路径
     * @param delSrc 是否删除本地文件
     * @param overwrite 时候覆盖
     */
    public void fileUpload(String srcFile, String destPath, boolean delSrc, boolean overwrite) {
        if (StringUtils.isBlank(srcFile) || StringUtils.isBlank(destPath) || !new File(srcFile).exists()) {
            throw new IllegalArgumentException("文件或路径不合法，上传失败");
        }
        try {
            this.fileSystem.copyFromLocalFile(delSrc, overwrite, new Path(srcFile), new Path(destPath));
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 文件从hdfs下载至本地
     * @param hdfsFile
     * hdfs文件路径
     * @param localPath
     * 本地路径
     * @param delSrc
     * 下载完成后时候上传hdfs中的文件
     */
    public void download(String hdfsFile, String localPath, boolean delSrc) {
        if(StringUtils.isNotBlank(hdfsFile) && StringUtils.isNotBlank(localPath)) {
            try {
                this.fileSystem.copyToLocalFile(delSrc, new Path(hdfsFile), new Path(localPath));
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }

}
