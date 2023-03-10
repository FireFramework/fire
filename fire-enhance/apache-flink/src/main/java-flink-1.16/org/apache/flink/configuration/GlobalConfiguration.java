/*
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

package org.apache.flink.configuration;

import com.zto.fire.common.conf.FireFrameworkConf;
import com.zto.fire.common.util.OSUtils;
import com.zto.fire.common.util.PropUtils;
import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConversions;

import javax.annotation.Nullable;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Method;
import java.net.ServerSocket;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Global configuration object for Flink. Similar to Java properties configuration objects it
 * includes key-value pairs which represent the framework's configuration.
 */
@Internal
public final class GlobalConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(GlobalConfiguration.class);

    public static final String FLINK_CONF_FILENAME = "flink-conf.yaml";

    // the keys whose values should be hidden
    private static final String[] SENSITIVE_KEYS =
            new String[] {"password", "secret", "fs.azure.account.key", "apikey"};

    // the hidden content to be displayed
    public static final String HIDDEN_CONTENT = "******";

    // --------------------------------------------------------------------------------------------

    private GlobalConfiguration() {}

    // --------------------------------------------------------------------------------------------

    /**
     * Loads the global configuration from the environment. Fails if an error occurs during loading.
     * Returns an empty configuration object if the environment variable is not set. In production
     * this variable is set but tests and local execution/debugging don't have this environment
     * variable set. That's why we should fail if it is not set.
     *
     * @return Returns the Configuration
     */
    public static Configuration loadConfiguration() {
        return loadConfiguration(new Configuration());
    }

    /**
     * Loads the global configuration and adds the given dynamic properties configuration.
     *
     * @param dynamicProperties The given dynamic properties
     * @return Returns the loaded global configuration with dynamic properties
     */
    public static Configuration loadConfiguration(Configuration dynamicProperties) {
        final String configDir = System.getenv(ConfigConstants.ENV_FLINK_CONF_DIR);
        if (configDir == null) {
            return new Configuration(dynamicProperties);
        }

        return loadConfiguration(configDir, dynamicProperties);
    }

    /**
     * Loads the configuration files from the specified directory.
     *
     * <p>YAML files are supported as configuration files.
     *
     * @param configDir the directory which contains the configuration files
     */
    public static Configuration loadConfiguration(final String configDir) {
        return loadConfiguration(configDir, null);
    }

    /**
     * Loads the configuration files from the specified directory. If the dynamic properties
     * configuration is not null, then it is added to the loaded configuration.
     *
     * @param configDir directory to load the configuration from
     * @param dynamicProperties configuration file containing the dynamic properties. Null if none.
     * @return The configuration loaded from the given configuration directory
     */
    public static Configuration loadConfiguration(
            final String configDir, @Nullable final Configuration dynamicProperties) {

        if (configDir == null) {
            throw new IllegalArgumentException(
                    "Given configuration directory is null, cannot load configuration");
        }

        final File confDirFile = new File(configDir);
        if (!(confDirFile.exists())) {
            throw new IllegalConfigurationException(
                    "The given configuration directory name '"
                            + configDir
                            + "' ("
                            + confDirFile.getAbsolutePath()
                            + ") does not describe an existing directory.");
        }

        // get Flink yaml configuration file
        final File yamlConfigFile = new File(confDirFile, FLINK_CONF_FILENAME);

        if (!yamlConfigFile.exists()) {
            throw new IllegalConfigurationException(
                    "The Flink config file '"
                            + yamlConfigFile
                            + "' ("
                            + yamlConfigFile.getAbsolutePath()
                            + ") does not exist.");
        }

        Configuration configuration = loadYAMLResource(yamlConfigFile);

        logConfiguration("Loading", configuration);

        if (dynamicProperties != null) {
            logConfiguration("Loading dynamic", dynamicProperties);
            configuration.addAll(dynamicProperties);
        }

        return configuration;
    }

    private static void logConfiguration(String prefix, Configuration config) {
        config.confData.forEach(
                (key, value) ->
                        LOG.info(
                                "{} configuration property: {}, {}",
                                prefix,
                                key,
                                isSensitive(key) ? HIDDEN_CONTENT : value));
    }

    /**
     * Loads a YAML-file of key-value pairs.
     *
     * <p>Colon and whitespace ": " separate key and value (one per line). The hash tag "#" starts a
     * single-line comment.
     *
     * <p>Example:
     *
     * <pre>
     * jobmanager.rpc.address: localhost # network address for communication with the job manager
     * jobmanager.rpc.port   : 6123      # network port to connect to for communication with the job manager
     * taskmanager.rpc.port  : 6122      # network port the task manager expects incoming IPC connections
     * </pre>
     *
     * <p>This does not span the whole YAML specification, but only the *syntax* of simple YAML
     * key-value pairs (see issue #113 on GitHub). If at any point in time, there is a need to go
     * beyond simple key-value pairs syntax compatibility will allow to introduce a YAML parser
     * library.
     *
     * @param file the YAML file to read from
     * @see <a href="http://www.yaml.org/spec/1.2/spec.html">YAML 1.2 specification</a>
     */
    private static Configuration loadYAMLResource(File file) {
        final Configuration config = new Configuration();

        // TODO: ------------ start????????????????????? --------------- //
        Method setSetting = null;
        try {
            Class env = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation");
            setSetting = env.getMethod("setSetting", String.class, String.class);
        } catch (Exception e) {
            LOG.error("??????EnvironmentInformation.setSetting()??????", e);
        }
        // TODO: ------------ end????????????????????? --------------- //

        try (BufferedReader reader =
                     new BufferedReader(new InputStreamReader(new FileInputStream(file)))) {

            String line;
            int lineNo = 0;
            while ((line = reader.readLine()) != null) {
                lineNo++;
                // 1. check for comments
                String[] comments = line.split("#", 2);
                String conf = comments[0].trim();

                // 2. get key and value
                if (conf.length() > 0) {
                    String[] kv = conf.split(": ", 2);

                    // skip line with no valid key-value pair
                    if (kv.length == 1) {
                        LOG.warn(
                                "Error while trying to split key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    String key = kv[0].trim();
                    String value = kv[1].trim();

                    // sanity check
                    if (key.length() == 0 || value.length() == 0) {
                        LOG.warn(
                                "Error after splitting key and value in configuration file "
                                        + file
                                        + ":"
                                        + lineNo
                                        + ": \""
                                        + line
                                        + "\"");
                        continue;
                    }

                    LOG.info(
                            "Loading configuration property: {}, {}",
                            key,
                            isSensitive(key) ? HIDDEN_CONTENT : value);
                    config.setString(key, value);

                    // TODO: ------------ start????????????????????? --------------- //
                    try {
                        setSetting.invoke(null, key, value);
                    } catch (Exception e) {
                        LOG.error("????????????????????????????????????????????????????????????", e);
                    }
                    // TODO: ------------ end????????????????????? --------------- //
                }
            }
        } catch (IOException e) {
            throw new RuntimeException("Error parsing YAML configuration.", e);
        }

        // TODO: ------------ start????????????????????? --------------- //
        fireBootstrap(config);
        // TODO: ------------ end????????????????????? --------------- //

        return config;
    }

    // TODO: ------------ start????????????????????? --------------- //
    private static AtomicBoolean isStart = new AtomicBoolean(false);
    // ???????????????JobManager??????TaskManager
    private static boolean isJobManager = false;
    // fire rest??????????????????
    private static ServerSocket restServerSocket;
    // ?????????????????????
    private static String runMode;
    private static final Map<String, String> settings = new HashMap<>();

    static {
        try {
            restServerSocket = new ServerSocket(0);
        } catch (Exception e) {
            LOG.error("??????Socket??????", e);
        }
    }

    /**
     * ??????????????????
     */
    public static Map<String, String> getSettings() {
        return settings;
    }

    /**
     * ?????????????????????Rest?????????
     */
    public static int getRestPort() {
        return restServerSocket.getLocalPort();
    }

    /**
     * ??????rest???????????????????????????Socket
     */
    public static int getRestPortAndClose() {
        int port = restServerSocket.getLocalPort();
        if (restServerSocket != null && !restServerSocket.isClosed()) {
            try {
                restServerSocket.close();
            } catch (Exception e) {
                LOG.error("??????Rest Socket??????", e);
            }
        }
        return port;
    }

    /**
     * fire???????????????????????????
     */
    private static void fireBootstrap(Configuration config) {
        if (isStart.compareAndSet(false, true)) {
            // ???????????????????????????
            loadTaskConfiguration(config);
        }
    }

    /**
     * ??????????????????????????????
     */
    public static String getRunMode() {
        return runMode;
    }

    /**
     * ???????????????????????????
     */
    private static void loadTaskConfiguration(Configuration config) {
        // ??????????????????????????????????????????flink??????
        // ??????????????????????????????
        String className = config.getString("$internal.application.main", config.getString("flink.fire.className", ""));
        // ????????????????????????????????????yarn-application???yarn-per-job
        runMode = config.getString("flink.execution.target", config.getString("execution.target", ""));

        try {
            Class env = Class.forName("org.apache.flink.runtime.util.EnvironmentInformation");
            Method method = env.getMethod("isJobManager");
            isJobManager = Boolean.valueOf(method.invoke(null) + "");
        } catch (Exception e) {
            LOG.error("??????EnvironmentInformation.isJobManager()??????", e);
        }

        // ??????????????????JobManager??????????????????TaskManager??????????????????merge
        if (isJobManager && className != null && className.contains(".")) {
            String simpleClassName = className.substring(className.lastIndexOf('.') + 1);
            if (simpleClassName.length() > 0) {
                PropUtils.setProperty("driver.class.name", className);
                // TODO: ???????????????????????????????????????????????????
                // PropUtils.load(FireFrameworkConf.FLINK_BATCH_CONF_FILE)
                PropUtils.loadFile(FireFrameworkConf.FLINK_STREAMING_CONF_FILE());
                // ?????????configuration???????????????PropUtils???
                PropUtils.setProperties(config.confData);
                // ??????????????????????????????
                PropUtils.load(FireFrameworkConf.userCommonConf());
                // ?????????????????????????????????
                // PropUtils.loadJobConf(className);
                // ??????fire rest????????????
                PropUtils.setProperty(FireFrameworkConf.FIRE_REST_URL(), "http://" + OSUtils.getIp() + ":" + getRestPort());
                // ??????????????????????????????????????????????????????????????????????????????????????????
                PropUtils.loadJobConf(className);
                PropUtils.setProperty("flink.run.mode", runMode);

                Map<String, String> settingMap = (Map<String, String>) JavaConversions.mapAsJavaMap(PropUtils.settings());
                settingMap.forEach((k, v) -> {
                    config.setString(k, v);
                    settings.put(k, v);
                });
                LOG.info("main class???" + PropUtils.getProperty("driver.class.name"));
            }
        }
    }

    /**
     * Check whether the key is a hidden key.
     *
     * @param key the config key
     */
    public static boolean isSensitive(String key) {
        Preconditions.checkNotNull(key, "key is null");
        final String keyInLower = key.toLowerCase();
        // ????????????webui???????????????
        String hideKeys = ((Map<String, String>) JavaConversions.mapAsJavaMap(PropUtils.settings())).getOrDefault("fire.conf.print.blacklist", "password,secret,fs.azure.account.key");
        if (hideKeys != null && hideKeys.length() > 0) {
            String[] hideKeyArr = hideKeys.split(",");
            for (String hideKey : hideKeyArr) {
                if (keyInLower.length() >= hideKey.length()
                        && keyInLower.contains(hideKey)) {
                    return true;
                }
            }
        }
        return false;
    }
    // TODO: ------------ end????????????????????? ----------------- //
}
