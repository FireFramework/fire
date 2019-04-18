package com.zto.bigdata.spark.common.db;

import com.zto.bigdata.spark.common.util.GlobalConstants;
import org.apache.commons.lang3.StringUtils;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * JDBC管理类，同时提供单例的数据库连接池实现
 *
 * @author ChengLong 2016-10-15
 */
public class JDBCHelper {

	// 数据库连接池
	private LinkedList<Connection> dataSource = new LinkedList<Connection>();
	private static JDBCHelper instance = null; // 单例的数据库操作实例

	static {
		try {
			String driverName = GlobalConstants.driverClass();
			if (StringUtils.isNotBlank(driverName)) {
				Class.forName(driverName);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 初始化数据库连接池
	 */
	private JDBCHelper() {
		String url = GlobalConstants.rdburl();
		String user = GlobalConstants.user();
		String password = GlobalConstants.password();
		int size = 2;

		try {
			for (int i = 0; i < size; i++) {
				Connection conn = DriverManager.getConnection(url, user, password);
				this.dataSource.push(conn);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	/*
	 * 获取数据库连接池实例对象
	 */
	public static JDBCHelper getInstance() {
		if (instance == null) {
			synchronized (JDBCHelper.class) {
				if (instance == null) {
					instance = new JDBCHelper();
				}
			}
		}
		return instance;
	}

	/*
	 * 从数据库连接池中获取一个连接
	 */
	public synchronized Connection getConnection() {
		// 如果当前连接池中没有连接，则一直等待，直到获取到连接
		while (this.dataSource.size() == 0) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
		return this.dataSource.poll();
	}

	/**
	 * 执行更新操作
	 *
	 * @param sql
	 * @param params
	 * @return
	 */
	public int executeUpdate(String sql, Object[] params) {
		int retVal = 0;
		Connection conn = null;
		PreparedStatement stat = null;
		try {
			conn = this.getConnection();
			conn.setAutoCommit(false);
			stat = conn.prepareStatement(sql);
			// 设置值参数
			if (params != null && params.length > 0) {
				for (int i = 0; i < params.length; i++) {
					stat.setObject(i + 1, params[i]);
				}
			}
			retVal = stat.executeUpdate();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				this.dataSource.push(conn);
			}
			if(stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return retVal;
	}

	/**
	 * 执行批量更新操作
	 *
	 * @param sql
     *              待执行的sql语句
	 * @param paramsList
     *              sql的参数列表
     * @return
     *              影响的记录数
	 */
	public int[] executeBatch(String sql, List<Object[]> paramsList) {
		int[] retVal = null;
		Connection conn = null;
		PreparedStatement stat = null;
		try {
			conn = this.getConnection();
			conn.setAutoCommit(false);
			stat = conn.prepareStatement(sql);
			if (paramsList != null && paramsList.size() > 0) {
				for (Object[] objArr : paramsList) {
					for (int i = 0; i < objArr.length; i++) {
						stat.setObject(i + 1, objArr[i]);
					}
					// 这段代码必须放在这个位置
					stat.addBatch();
				}
			}
			// 执行批量更新
			retVal = stat.executeBatch();
			conn.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				this.dataSource.push(conn);
			}
			if(stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
		return retVal;
	}

	/**
	 * 执行查询操作
	 *
	 * @param sql
	 * @param params
	 * @param callback
	 */
	public void executeQuery(String sql, Object[] params, QueryCallback callback) {
		Connection conn = null;
		PreparedStatement stat = null;
		ResultSet rs = null;
		try {
			conn = this.getConnection();
			stat = conn.prepareStatement(sql);

			if (params != null && params.length > 0) {
				for (int i = 0; i < params.length; i++) {
					stat.setObject(i + 1, params[i]);
				}
			}
			rs = stat.executeQuery();
			if(callback != null) {
				callback.process(rs);
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (conn != null) {
				this.dataSource.push(conn);
			}
			if(rs != null) {
				try {
					rs.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if(stat != null) {
				try {
					stat.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
		}
	}

	/**
	 * 内部回调接口
	 *
	 * @author Administrator 2016-07-06
	 */
	public static interface QueryCallback {
		// 回调方法，对返回结果进行处理
		void process(ResultSet rs) throws Exception;
	}

	public static void main(String[] args) {
		JDBCHelper instance = JDBCHelper.getInstance();
		/*int count = instance.executeUpdate("insert into OrderRealtimeStatistics values(?,?,?)", new Object[]{"2016-10-31", 1210, "2016-10-31 10:59:11"});
		System.out.println(count);*/

		int count = instance.executeUpdate("update OrderRealtimeStatistics set count=? where datetime=?", new Object[] {111222, "2016-10-31"});
		System.out.println(count);
	}
}
