/*
 *  Copyright (C) 2023 ~ 2035 the original authors WL4G (James Wong).
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */

package com.wl4g.dataconnector.qos;

import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.framework.NamedDataConnectorSpi;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link AbstractQoS}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@Slf4j
public abstract class AbstractQoS extends NamedDataConnectorSpi implements IQoS {

    @Override
    public void retryIfFail(ConnectorConfig connectorConfig,
                            int retryTimes,
                            Runnable call) {
        if (canRetry(connectorConfig, retryTimes)) {
            final long intervalMs = getSleepTime(connectorConfig, retryTimes);
            if (log.isDebugEnabled()) {
                log.debug("Retrying of times: {}, intervalMs: {}", retryTimes, intervalMs);
            }
            try {
                doSleep(intervalMs);
            } catch (Exception ex) {
                throw new DataConnectorException(String.format("Failed to do retry of times: %s, intervalMs : %s",
                        retryTimes, intervalMs), ex);
            }
            call.run();
        }
    }

    protected void doSleep(final long intervalMs) throws InterruptedException {
        // see:https://github.com/openjdk/jdk/blob/jdk8-b120/hotspot/src/share/vm/runtime/thread.cpp#L1249
        Thread.sleep(intervalMs);
        // see:https://github.com/openjdk/jdk/blob/jdk8-b120/hotspot/src/share/vm/runtime/park.cpp
        //LockSupport.parkNanos(intervalMs * 1000_000);
    }

    protected long getSleepTime(ConnectorConfig connectorConfig,
                                int retryTimes) {
        return (long) Math.min(
                retryTimes * getRetryBackoffMultiplier() * getRetryBackoffMs(),
                getRetryMaxBackoffMs());
    }

    protected long getRetryBackoffMs() {
        throw new UnsupportedOperationException();
    }

    protected long getRetryMaxBackoffMs() {
        throw new UnsupportedOperationException();
    }

    protected double getRetryBackoffMultiplier() {
        throw new UnsupportedOperationException();
    }

}
