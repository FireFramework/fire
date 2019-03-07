package com.zto.bigdata.spark.common.util;

import org.apache.commons.io.FileUtils;

import java.io.File;

/**
 * svn 操作工具类，实现方式是调用linux中的svn命令
 * @author ChengLong 2018年8月20日 07:50:08
 */
public class SvnUtils {
	// svn 提交锁
	private static final String SVN_LOCK = "svnlock";

	/**
	 * 添加并提交文件到svn（文件由content字符串生成）
	 * @param logMsg
	 * 日志信息
	 * @param commitFile
	 * 提交的文件
	 * @return
	 * @throws Exception
	 */
	public static String addAndCommitSvn(String logMsg, String commitFile) throws Exception {
		RedisUtils.lock(SvnUtils.SVN_LOCK);
		String resMsg = "";
		try {
			int exitVal = ProcessUtil.execAndWaitFor("svn", "add", commitFile);
			if (exitVal != 0) {
				resMsg = commitFile + "add file failed!";
			}
			exitVal = ProcessUtil.execAndWaitFor("svn", "commit", "-m", "\\\"" + logMsg + "\\\"", commitFile);
			if (exitVal != 0) {
				resMsg = commitFile + "commit file failed!";
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			RedisUtils.unlock(SvnUtils.SVN_LOCK);
		}
		return resMsg;
	}

	/**
	 * 添加并提交文件到svn（文件由content字符串生成）
	 * @param logMsg
	 * svn 提交日志
	 * @param commitFile
	 * 提交到svn的文件全路径字符串
	 * @param content
	 * 写入到文件中的内容
	 * @return
	 * 提交结果信息
	 * @throws Exception
	 */
	public static String addAndCommitSvn(String logMsg, String commitFile, String content) throws Exception {
		// 将字符串（content）写入到指定的文件中
		FileUtils.writeStringToFile(new File(commitFile), content);
		return SvnUtils.addAndCommitSvn(logMsg, commitFile);
	}

	/**
	 * 更新并提交文件到svn（文件由content字符串生成）
	 * @param logMsg
	 * svn操作日志
	 * @param commitFile
	 * 提交的文件全路径
	 * @return
	 * 操作结果信息
	 * @throws Exception
	 */
	public static String updateAndCommitSvn(String logMsg, String commitFile) throws Exception {
		RedisUtils.lock(SvnUtils.SVN_LOCK);
		String resMsg = "";
		try {
			int exitVal = ProcessUtil.execAndWaitFor("svn", "update", commitFile);
			if (exitVal != 0) {
				resMsg = commitFile + "update file failed!";
			}

			exitVal = ProcessUtil.execAndWaitFor("svn", "commit", "-m", "\\\"" + logMsg + "\\\"", commitFile);
			if (exitVal != 0) {
				resMsg = commitFile + "commit file failed!";
			}
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		} finally {
			RedisUtils.unlock(SvnUtils.SVN_LOCK);
		}
		return resMsg;
	}

	/**
	 * 更新并提交文件到svn（文件由content字符串生成）
	 * @param logMsg
	 * svn 提交日志
	 * @param commitFile
	 * 提交到svn的文件全路径字符串
	 * @param content
	 * 写入到文件中的内容
	 * @return
	 * 提交结果信息
	 * @throws Exception
	 */
	public static String updateAndCommitSvn(String logMsg, String commitFile, String content) throws Exception {
		// 将字符串（content）写入到指定的文件中
		FileUtils.writeStringToFile(new File(commitFile), content);
		return SvnUtils.updateAndCommitSvn(logMsg, commitFile);
	}
}
