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

package com.wl4g.dataconnector.coordinator.kafka;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.BusEvent;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator.IBusPublisher;
import com.wl4g.dataconnector.coordinator.kafka.KafkaDataConnectorCoordinator.KafkaCoordinatorConfig;
import com.wl4g.dataconnector.coordinator.kafka.KafkaDataConnectorCoordinator.KafkaCoordinatorProvider;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.wl4g.infra.common.serialize.JacksonUtils.toJSONString;
import static java.util.Collections.emptyList;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

/**
 * The {@link KafkaBusPublisher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public class KafkaBusPublisher implements IBusPublisher {
    private final DataConnectorConfiguration config;
    private volatile Producer<String, String> busProducer;

    public KafkaBusPublisher(DataConnectorConfiguration config) {
        this.config = Assert2.notNullOf(config, "config");
    }

    public void publishSync(@NotNull List<BusEvent> events,
                            @NotNull Duration timeout) throws InterruptedException, TimeoutException {
        requireNonNull(timeout, "timeout must not be null");

        final List<Future<?>> futures = publishAsync(events);
        final CountDownLatch latch = new CountDownLatch(futures.size());

        final List<Object> failures = futures.stream().map(future -> {
            try {
                return future.get();
            } catch (Exception th) {
                log.error("Unable to getting publish bus event.", th);
                return null;
            } finally {
                latch.countDown();
            }
        }).filter(Objects::isNull).collect(toList());

        if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(String.format("Failed to getting publish channel " +
                    "events result within timeout %s", timeout));
        }
        if (!failures.isEmpty()) {
            throw new IllegalStateException(String.format("Failed to publish channel events" +
                    ", failures: %s", failures.size()));
        }
    }

    public List<Future<?>> publishAsync(@NotNull List<BusEvent> events) {
        requireNonNull(events, "events must not be null");
        events.forEach(BusEvent::validate);
        try {
            obtainBusProducer().beginTransaction();

            final List<Future<RecordMetadata>> futures = events
                    .stream()
                    .map(event -> obtainBusProducer().send(new ProducerRecord<>(getBusTopic(),
                            // TODO
                            "my-key", toJSONString(event))))
                    .collect(toList());

            obtainBusProducer().commitTransaction();

            return new ArrayList<>(futures);
        } catch (Exception th) {
            busProducer.abortTransaction();
            log.error(String.format("Failed to publish channel events ::: %s", events), th);
        }
        return emptyList();
    }

    private Producer<String, String> obtainBusProducer() {
        // TODO detection for producer health? and re-create?
        if (isNull(busProducer)) {
            synchronized (this) {
                if (isNull(busProducer)) {
                    final Map<String, Object> config = new HashMap<>();
                    config.putAll(KafkaDataConnectorCoordinator.DEFAULT_PRODUCER_PROPS);
                    config.putAll(getKafkaCoordinatorConfig().getBusConfig().getProducerProps());
                    config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, getKafkaCoordinatorConfig().getBootstrapServers());
                    this.busProducer = new KafkaProducer<>(config);
                    this.busProducer.initTransactions();
                }
            }
        }
        return this.busProducer;
    }

    private String getBusTopic() {
        return getKafkaCoordinatorConfig().getBusConfig().getTopic();
    }

    private KafkaCoordinatorConfig getKafkaCoordinatorConfig() {
        return ((KafkaCoordinatorProvider) config.getCoordinatorProvider())
                .getCoordinatorConfig();
    }

}
