/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.rocketmq.flink.util;

import org.apache.rocketmq.flink.RunningChecker;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;

public class RetryUtil {
    private static final Logger log = LoggerFactory.getLogger(RetryUtil.class);
    private RunningChecker runningChecker;
    private final long INITIAL_BACKOFF = 2000;
    private final long MAX_BACKOFF = 30000;
    private int maxAttempts = 6;
    private int retries = 1;

    public RetryUtil(RunningChecker runningChecker) {
        this.runningChecker = runningChecker;
    }

    public void waitForMs(long sleepMs) {
        try {
            Thread.sleep(sleepMs);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public <T> T call(Callable<T> callable, String errorMsg) throws RuntimeException {
        long backoff = INITIAL_BACKOFF;
        do {
            try {
                return callable.call();
            } catch (Exception ex) {
                if (retries >= maxAttempts) {
                    runningChecker.setException(new RuntimeException(ex));
                    throw new RuntimeException(ex);
                }
                log.error("{}, retry {}/{}", errorMsg, retries, maxAttempts, ex);
                retries++;
            }
            waitForMs(backoff);
            backoff = Math.min(backoff * 2, MAX_BACKOFF);
        } while (true);
    }

    public void setRetries(int retries) {
        this.retries = retries;
    }

    public void setMaxAttempts(int maxAttempts) {
        this.maxAttempts = maxAttempts;
    }

}
