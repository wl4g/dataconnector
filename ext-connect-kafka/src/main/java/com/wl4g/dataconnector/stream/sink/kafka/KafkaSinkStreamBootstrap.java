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

package com.wl4g.dataconnector.stream.sink.kafka;

import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap.StreamBootstrap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;

import java.util.List;
import java.util.Map;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link KafkaSinkStreamBootstrap}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class KafkaSinkStreamBootstrap extends StreamBootstrap<KafkaSinkStream> {

    public KafkaSinkStreamBootstrap(KafkaSinkStream sinkStream,
                                    ConcurrentKafkaSinkContainer internalTask) {
        super(sinkStream, internalTask);
    }

    @Override
    public void start() {
        getStream().getPointReader().start();
    }

    @Override
    public boolean stop(long timeoutMs, boolean force) throws Exception {
        getRequiredInternalTask().close();
        getStream().close();
        return !isRunning();
    }

    @Override
    public boolean scaling(int concurrency,
                           boolean restart,
                           long restartTimeout) throws Exception {
        return getStream().getPointReader().scaling(concurrency, restart, restartTimeout);
    }

    @Override
    public void pause() {
        getStream().getPointReader().pause();
    }

    @Override
    public void resume() {
        getStream().getPointReader().resume();
    }

    @Override
    public boolean isRunning() {
        return getStream().getPointReader().isRunning();
    }

    @Override
    public boolean isHealthy() {
        try {
            if (isRunning()) {
                safeList(getRequiredInternalTask().getProducerFactories())
                        .forEach(kf -> kf.createProducer().flush());
                return true;
            }
        } catch (Exception ex) {
            if (log.isDebugEnabled()) {
                log.debug("Failed to flush detect health.", ex);
            }
        }
        return false;
    }

    @Override
    public int getSubTaskCount() {
        return safeList(getRequiredInternalTask().getProducerFactories()).size();
    }

    public List<Map<MetricName, ? extends Metric>> metrics() {
        return safeList(getRequiredInternalTask().getProducerFactories())
                .stream()
                .map(kf -> kf.createProducer().metrics())
                .collect(toList());
    }

    private ConcurrentKafkaSinkContainer getRequiredInternalTask() {
        return (ConcurrentKafkaSinkContainer) requireNonNull(getInternalTask(),
                "internalTask");
    }

}

