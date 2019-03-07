package com.zto.bigdata.spark.common.util;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.log4j.Logger;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.List;

public class ZkUtils {

	private static final Logger logger = Logger.getLogger(ZkUtils.class.getName());

	public static final String LEADER_PATH = "/datacloud/leader";

	public static final String DATACLOUD_PATH = "/datacloud/scheduler";

	private boolean leader = false;

	private static final ZkUtils single = new ZkUtils();

	public static ZkUtils getInstance() {
		return single;
	}

	private ZkUtils() {
	}

	public interface CallBack {
		public void isLeader();
	}

	private static CuratorFramework client = null;

	public static CuratorFramework getClient() {
		return client;
	}

	private LeaderLatch leaderLatch = null;

	private static String ip;

	static {
		try {
			ip = InetAddress.getLocalHost().getHostAddress();
		} catch (UnknownHostException e) {
			ip = "0.0.0.0";
		}
//		client = CuratorFrameworkFactory.builder().connectString(Config.getString("zk.server"))
//				.retryPolicy(new RetryNTimes(Integer.MAX_VALUE, 1000)).connectionTimeoutMs(60000).sessionTimeoutMs(60000).build();
//		client.start();
	}

	public boolean isLeader() {
		return leader;
	}

	public void setLeader(boolean leader) {
		this.leader = leader;
	}

	public String getLeaderServer() throws Exception {
		byte[] datas = null;
		try {
			datas = client.getData().forPath(LEADER_PATH);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		return new String(datas);
	}

	public String getServerList() throws Exception {
		List<String> list = null;
		try {
			list = client.getChildren().forPath(DATACLOUD_PATH);
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}
		StringBuilder sb = new StringBuilder();
		if(list != null && list.size() > 0) {
			for(String str : list) {
				sb.append(str + ";");
			}
		}
		return sb.toString();
	}

	/**
	 * 注册到zk
	 */
	public void registerZk() {
		try {
			if (client.checkExists().forPath(DATACLOUD_PATH + "/" + ip) == null) {
				client.create()// 创建一个路径
						.creatingParentsIfNeeded()// 如果指定的节点的父节点不存在，递归创建父节点
						.withMode(CreateMode.EPHEMERAL)// 存储类型（临时的还是持久的）
						.forPath(DATACLOUD_PATH + "/"+ ip);
			}
		} catch (Exception e) {
			logger.error("=================== registerZk failed =======================", e);
		}
	}

	/**
	 * 进行选举
	 */
	public void startLeaderLatch(final CallBack callBack) {
		leaderLatch = new LeaderLatch(client, LEADER_PATH);
		leaderLatch.addListener(new LeaderLatchListener() {

			public void notLeader() {
				setLeader(false);
				logger.info("=================== get leader fail =======================");
			}

			public void isLeader() {
				setLeader(true);
				try {
					client.setData().forPath(LEADER_PATH, ip.getBytes());
				} catch (Exception e) {
					e.printStackTrace();
				}
				callBack.isLeader();
				logger.info("=================== get leader success =======================");
			}
		});
		try {
			leaderLatch.start();
		} catch (Exception e) {
			try {
				leaderLatch.close();
			} catch (IOException e1) {
				e1.printStackTrace();
			}
			logger.error("=================== leaderLatch failed =======================");
		}
	}

	/**
	 * 关闭zk
	 */
	public void close() {
		if (null != leaderLatch) {
			try {
				leaderLatch.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		client.close();
	}

}
