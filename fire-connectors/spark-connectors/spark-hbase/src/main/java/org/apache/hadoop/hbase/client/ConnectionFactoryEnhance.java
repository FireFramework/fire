/**
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.client;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.classification.InterfaceAudience;
import org.apache.hadoop.hbase.classification.InterfaceStability;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;

import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;

import static org.apache.hadoop.hbase.client.ConnectionManager.MAX_CACHED_CONNECTION_INSTANCES;


/**
 * A non-instantiable class that manages creation of {@link Connection}s.
 * Managing the lifecycle of the {@link Connection}s to the cluster is the responsibility of
 * the caller.
 * From a {@link Connection}, {@link Table} implementations are retrieved
 * with {@link Connection#getTable(TableName)}. Example:
 * <pre>
 * Connection connection = ConnectionFactory.createConnection(config);
 * Table table = connection.getTable(TableName.valueOf("table1"));
 * try {
 *   // Use the table as needed, for a single operation and a single thread
 * } finally {
 *   table.close();
 *   connection.close();
 * }
 * </pre>
 *
 * Similarly, {@link Connection} also returns {@link Admin} and {@link RegionLocator}
 * implementations.
 *
 * This class replaces {@link HConnectionManager}, which is now deprecated.
 * @see Connection
 * @since 0.99.0
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ConnectionFactoryEnhance {
  public static final Log LOG = LogFactory.getLog(ConnectionFactoryEnhance.class);

  /** No public c.tors */
  protected ConnectionFactoryEnhance() {
  }

  /**
   * Create a new Connection instance using default HBaseConfiguration. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection();
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection() throws IOException {
    return createConnection(HBaseConfiguration.create(), null, null);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf) throws IOException {
    return createConnection(conf, null, null);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("mytable"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @param pool the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool)
          throws IOException {
    return createConnection(conf, pool, null);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @param user the user the connection is for
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, User user)
  throws IOException {
    return createConnection(conf, null, user);
  }

  /**
   * Create a new Connection instance using the passed <code>conf</code> instance. Connection
   * encapsulates all housekeeping for a connection to the cluster. All tables and interfaces
   * created from returned connection share zookeeper connection, meta cache, and connections
   * to region servers and masters.
   * <br>
   * The caller is responsible for calling {@link Connection#close()} on the returned
   * connection instance.
   *
   * Typical usage:
   * <pre>
   * Connection connection = ConnectionFactory.createConnection(conf);
   * Table table = connection.getTable(TableName.valueOf("table1"));
   * try {
   *   table.get(...);
   *   ...
   * } finally {
   *   table.close();
   *   connection.close();
   * }
   * </pre>
   *
   * @param conf configuration
   * @param user the user the connection is for
   * @param pool the thread pool to use for batch operations
   * @return Connection object for <code>conf</code>
   */
  public static Connection createConnection(Configuration conf, ExecutorService pool, User user)
  throws IOException {
    if (user == null) {
      UserProvider provider = UserProvider.instantiate(conf);
      user = provider.getCurrent();
    }

    return createConnection(conf, false, pool, user);
  }

  static Connection createConnection(final Configuration conf, final boolean managed,
      final ExecutorService pool, final User user)
  throws IOException {
    String className = conf.get(HConnection.HBASE_CLIENT_CONNECTION_IMPL,
      ConnectionManager.HConnectionImplementation.class.getName());
    Class<?> clazz = null;
    try {
      clazz = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException(e);
    }
    try {
      // Default HCM#HCI is not accessible; make it so before invoking.
      Constructor<?> constructor =
        clazz.getDeclaredConstructor(Configuration.class,
          boolean.class, ExecutorService.class, User.class);
      constructor.setAccessible(true);
      return (Connection) constructor.newInstance(conf, managed, pool, user);
    } catch (Exception e) {
      throw new IOException(e);
    }
  }

  //clean up all hbase connection
  public static void cleanup() {
    deleteAllConnections();
  }

  //clean up all hbase connection
  public void cleanupInstance() {
    deleteAllConnectionsInstance();
  }

  public static Connection getConnection(final Configuration conf) throws IOException {
    return getConnectionInternal(conf);
  }

  public Connection getConnectionInstance(final Configuration conf) throws IOException {
    return getConnectionInternalInstance(conf);
  }

  // An LRU Map of HConnectionKey -> HConnection (TableServer).  All
  // access must be synchronized.  This map is not private because tests
  // need to be able to tinker with it.
  private static final Map<HConnectionKey, Connection> CONNECTION_INSTANCES_CACHE;

  static {
    CONNECTION_INSTANCES_CACHE = new LinkedHashMap<HConnectionKey, Connection>(
            (int) (MAX_CACHED_CONNECTION_INSTANCES / 0.75F) + 1, 0.75F, true) {
      @Override
      protected boolean removeEldestEntry(
              Map.Entry<HConnectionKey, Connection> eldest) {
        return size() > MAX_CACHED_CONNECTION_INSTANCES;
      }
    };
  }

  private static Connection getConnectionInternal(final Configuration conf)
          throws IOException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (CONNECTION_INSTANCES_CACHE) {
      Connection connection = CONNECTION_INSTANCES_CACHE.get(connectionKey);
      if (connection == null) {
        connection = ConnectionFactoryEnhance.createConnection(conf);
        CONNECTION_INSTANCES_CACHE.put(connectionKey, connection);
      } else if (connection.isClosed()) {
        deleteConnection(connectionKey);
        connection = ConnectionFactoryEnhance.createConnection(conf);
        CONNECTION_INSTANCES_CACHE.put(connectionKey, connection);
      }
      return connection;
    }
  }

  private Connection getConnectionInternalInstance(final Configuration conf)
          throws IOException {
    HConnectionKey connectionKey = new HConnectionKey(conf);
    synchronized (CONNECTION_INSTANCES_CACHE) {
      Connection connection = CONNECTION_INSTANCES_CACHE.get(connectionKey);
      if (connection == null) {
        connection = ConnectionFactoryEnhance.createConnection(conf);
        CONNECTION_INSTANCES_CACHE.put(connectionKey, connection);
      } else if (connection.isClosed()) {
        deleteConnection(connectionKey);
        connection = ConnectionFactoryEnhance.createConnection(conf);
        CONNECTION_INSTANCES_CACHE.put(connectionKey, connection);
      }
      return connection;
    }
  }

  private static void deleteConnection(HConnectionKey connectionKey) {
    synchronized (CONNECTION_INSTANCES_CACHE) {
      Connection connection = CONNECTION_INSTANCES_CACHE.get(connectionKey);
      if (connection != null) {
        CONNECTION_INSTANCES_CACHE.remove(connectionKey);
        try {
          connection.close();
        } catch (IOException e) {
          LOG.error("Failed to close connection in the ConnectionFactory list", e);
        }
      } else {
        LOG.error("Connection not found in the ConnectionFactory list, can't delete it " +
                "(connection key=" + connectionKey + "). May be the key was modified?", new Exception());
      }
    }
  }

  private static void deleteAllConnections() {
    synchronized (CONNECTION_INSTANCES_CACHE) {
      Set<HConnectionKey> connectionKeys = new HashSet<HConnectionKey>();
      connectionKeys.addAll(CONNECTION_INSTANCES_CACHE.keySet());
      for (HConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey);
      }
      CONNECTION_INSTANCES_CACHE.clear();
    }
  }

  private void deleteAllConnectionsInstance() {
    synchronized (CONNECTION_INSTANCES_CACHE) {
      Set<HConnectionKey> connectionKeys = new HashSet<HConnectionKey>();
      connectionKeys.addAll(CONNECTION_INSTANCES_CACHE.keySet());
      for (HConnectionKey connectionKey : connectionKeys) {
        deleteConnection(connectionKey);
      }
      CONNECTION_INSTANCES_CACHE.clear();
    }
  }
}
