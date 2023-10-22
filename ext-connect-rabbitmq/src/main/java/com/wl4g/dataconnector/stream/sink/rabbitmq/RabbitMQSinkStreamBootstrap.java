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

package com.wl4g.dataconnector.stream.sink.rabbitmq;

import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap.StreamBootstrap;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rabbitmq.clients.producer.RabbitMQProducer;
import org.apache.rabbitmq.common.Metric;
import org.apache.rabbitmq.common.MetricName;

import java.time.Duration;
import java.util.List;
import java.util.Map;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link RabbitMQSinkStreamBootstrap}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class RabbitMQSinkStreamBootstrap extends StreamBootstrap<RabbitMQSinkStream> {

    public RabbitMQSinkStreamBootstrap(RabbitMQSinkStream sinkStream,
                                    ConcurrentRabbitMQSinkContainer internalTask) {
        super(sinkStream, internalTask);
    }

    @Override
    public void start() {
        getStream().getPointReader().start();
    }

    @Override
    public boolean stop(long shutdownTimeout, boolean force) throws InterruptedException {
        getRequiredInternalTaskWrapper().close(Duration.ofMillis(shutdownTimeout));
        getStream().close();
        return !isRunning();
    }

    @Override
    public boolean scaling(int concurrency,
                           boolean restart,
                           long restartTimeout) throws InterruptedException {
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
                safeList(getRequiredInternalTaskWrapper().getRabbitMQProducers())
                        .forEach(RabbitMQProducer::flush);
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
        return safeList(getRequiredInternalTaskWrapper().getRabbitMQProducers()).size();
    }

    public List<Map<MetricName, ? extends Metric>> metrics() {
        return safeList(getRequiredInternalTaskWrapper().getRabbitMQProducers())
                .stream()
                .map(RabbitMQProducer::metrics)
                .collect(toList());
    }

    private ConcurrentRabbitMQSinkContainer getRequiredInternalTaskWrapper() {
        return (ConcurrentRabbitMQSinkContainer) requireNonNull(getInternalTask(),
                "internalTask");
    }

}

