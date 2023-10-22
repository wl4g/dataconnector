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

package com.wl4g.dataconnector.stream.source.kafka;

import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap.StreamBootstrap;
import lombok.Getter;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

@Getter
public class KafkaSourceStreamBootstrap extends StreamBootstrap<KafkaSourceStream> {

    public KafkaSourceStreamBootstrap(KafkaSourceStream sourceStream,
                                      ConcurrentMessageListenerContainer<String, Object> internalTask) {
        super(sourceStream, internalTask);
    }

    @Override
    public void start() {
        if (!isRunning()) {
            getRequiredInternalTask().start();
        }
    }

    @Override
    public boolean stop(long timeoutMs, boolean force) throws Exception {
        if (!isRunning()) {
            return true;
        }
        final CountDownLatch latch = new CountDownLatch(1);
        if (force) {
            getRequiredInternalTask().stopAbnormally(latch::countDown);
        } else { // graceful shutdown
            getRequiredInternalTask().stop(latch::countDown);
        }
        if (!latch.await(timeoutMs, TimeUnit.MILLISECONDS)) {
            throw new DataConnectorException(String.format("Timeout to stop source streaming of '%s'", timeoutMs));
        }
        return !isRunning();
    }

    @Override
    public boolean scaling(int concurrency,
                           boolean restart,
                           long restartTimeout) throws Exception {
        getRequiredInternalTask().setConcurrency(concurrency);
        if (restart) {
            if (stop(restartTimeout)) {
                start();
                return true;
            } else {
                return false;
            }
        }
        // It is bound not to immediately scale the number of concurrent containers.
        return false;
    }

    @Override
    public void pause() {
        getRequiredInternalTask().pause();
    }

    @Override
    public void resume() {
        getRequiredInternalTask().resume();
    }

    @Override
    public boolean isRunning() {
        return getRequiredInternalTask().isRunning();
    }

    @Override
    public boolean isHealthy() {
        return getRequiredInternalTask().isInExpectedState();
    }

    @Override
    public int getSubTaskCount() {
        return getRequiredInternalTask().getContainers().size();
    }

    public Map<String, Map<MetricName, ? extends Metric>> metrics() {
        return getRequiredInternalTask().metrics();
    }

    @SuppressWarnings("unchecked")
    private ConcurrentMessageListenerContainer<String, Object> getRequiredInternalTask() {
        return (ConcurrentMessageListenerContainer<String, Object>)
                requireNonNull(getInternalTask(), "internalTask");
    }

}

