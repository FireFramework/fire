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

package org.apache.flink.runtime.util;

import com.zto.fire.common.conf.FireFrameworkConf;
import com.zto.fire.common.util.ExceptionBus;
import com.zto.fire.common.util.OSUtils;
import org.apache.flink.util.OperatingSystem;
import org.slf4j.Logger;
import sun.misc.Signal;

/**
 * This signal handler / signal logger is based on Apache Hadoop's
 * org.apache.hadoop.util.SignalLogger.
 */
public class SignalHandler {

    private static boolean registered = false;

    // TODO: ------------ start：二次开发代码 ---------------- //
    /** 需要上报的退出信号编号：15 = SIGTERM。9 = SIGKILL 无法被捕获，不在此列 */
    private static final int SIGTERM_SIGNAL = 15;
    /** 防止重复上报，信号可能被多次投递 */
    private static volatile boolean termReported = false;
    // TODO: ------------ end：二次开发代码 ---------------- //

    /** Our signal handler. */
    private static class Handler implements sun.misc.SignalHandler {

        private final Logger LOG;
        private final sun.misc.SignalHandler prevHandler;

        Handler(String name, Logger LOG) {
            this.LOG = LOG;
            prevHandler = Signal.handle(new Signal(name), this);
        }

        /**
         * Handle an incoming signal.
         *
         * @param signal The incoming signal
         */
        @Override
        public void handle(Signal signal) {
            LOG.info(
                    "RECEIVED SIGNAL {}: SIG{}. Shutting down as requested.",
                    signal.getNumber(),
                    signal.getName());

            // TODO: ------------ start：二次开发代码 ---------------- //
            // 注意：此处必须使用直接调用而非反射，反射会吞掉异常导致问题无法定位
            if (signal.getNumber() == SIGTERM_SIGNAL && !termReported) {
                termReported = true;
                // 诊断：打印本进程实际读到的开关值。若为 false，说明 sendToMQ 在启动时已直接 return，
                // 退出回调与定时线程都未注册，post 进来的消息永远不会被发送。
                LOG.info("[fire-sigterm] 收到 SIGTERM(15)，exceptionTraceEnable={}", FireFrameworkConf.exceptionTraceEnable());
                try {
                    ExceptionBus.post(
                            new IllegalStateException(
                                    "[fire-sigterm]进程收到SIGTERM(15)信号即将退出：container="
                                            + System.getenv("CONTAINER_ID")
                                            + ", host="
                                            + OSUtils.getHostName()
                                            + ", pid="
                                            + OSUtils.getPid()),
                            "");
                    LOG.info("[fire-sigterm] 已将 SIGTERM 事件投递到 ExceptionBus。");
                } catch (Throwable t) {
                    LOG.warn("[fire-sigterm] 投递 SIGTERM 事件失败，不影响退出流程：", t);
                }
            }
            // TODO: ------------ end：二次开发代码 ---------------- //

            prevHandler.handle(signal);
        }
    }

    /**
     * Register some signal handlers.
     *
     * @param LOG The slf4j logger
     */
    public static void register(final Logger LOG) {
        synchronized (SignalHandler.class) {
            if (registered) {
                return;
            }
            registered = true;

            final String[] SIGNALS =
                    OperatingSystem.isWindows()
                            ? new String[] {"TERM", "INT"}
                            : new String[] {"TERM", "HUP", "INT"};

            StringBuilder bld = new StringBuilder();
            bld.append("Registered UNIX signal handlers for [");

            String separator = "";
            for (String signalName : SIGNALS) {
                try {
                    new Handler(signalName, LOG);
                    bld.append(separator);
                    bld.append(signalName);
                    separator = ", ";
                } catch (Exception e) {
                    LOG.info("Error while registering signal handler", e);
                }
            }
            bld.append("]");
            LOG.info(bld.toString());
        }
    }
}
