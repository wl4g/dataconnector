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

package com.wl4g.streamconnect.coordinator;

import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonSubTypes.Type;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectProperties;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.wl4g.infra.common.serialize.JacksonUtils.toJSONString;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

/**
 * The {@link StreamConnectEventPublisher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public class StreamConnectEventPublisher {

    private final StreamConnectConfiguration config;
    private final Producer<String, String> producer;

    public StreamConnectEventPublisher(StreamConnectConfiguration config) {
        this.config = Assert2.notNullOf(config, "config");

        final Properties props = new Properties();
        props.putAll(config.getCoordinator().getBusConfig().getProducerProps());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, config.getCoordinator().getBootstrapServers());
        this.producer = new KafkaProducer<>(props);
        this.producer.initTransactions();
    }

    public void publishSync(@NotNull List<SubscribeEvent> events,
                            @NotNull Duration timeout) throws InterruptedException, TimeoutException {
        Assert2.notNullOf(timeout, "timeout");

        final List<Future<RecordMetadata>> futures = publishAsync(events);
        final CountDownLatch latch = new CountDownLatch(futures.size());

        final List<Object> failures = futures.stream().map(future -> {
            try {
                return future.get();
            } catch (Throwable th) {
                log.error("Failed to getting publish subscribe event", th);
                return null;
            } finally {
                latch.countDown();
            }
        }).filter(Objects::isNull).collect(toList());

        if (!latch.await(timeout.toMillis(), TimeUnit.MILLISECONDS)) {
            throw new TimeoutException(String.format("Failed to getting publish subscribe " +
                    "events result within timeout %s", timeout));
        }
        if (failures.size() > 0) {
            throw new IllegalStateException(String.format("Failed to publish subscribe events" +
                    ", failures: %s", failures.size()));
        }
    }

    public List<Future<RecordMetadata>> publishAsync(@NotNull List<SubscribeEvent> events) {
        Assert2.notNullOf(events, "events");
        events.forEach(SubscribeEvent::validate);
        try {
            producer.beginTransaction();

            final List<Future<RecordMetadata>> futures = events
                    .stream()
                    .map(event -> producer.send(new ProducerRecord<>(config.getCoordinator()
                            .getBusConfig().getTopic(),
                            // TODO
                            "my-key", toJSONString(event))))
                    .collect(toList());

            producer.commitTransaction();

            return futures;
        } catch (Throwable th) {
            producer.abortTransaction();
            log.error(String.format("Failed to publish subscribe events ::: %s", events), th);
        }
        return emptyList();
    }

    @Schema(oneOf = {AddSubscribeEvent.class, UpdateSubscribeEvent.class, RemoveSubscribeEvent.class},
            discriminatorProperty = "type")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = true)
    @JsonSubTypes({@Type(value = AddSubscribeEvent.class, name = "ADD"),
            @Type(value = UpdateSubscribeEvent.class, name = "UPDATE"),
            @Type(value = RemoveSubscribeEvent.class, name = "REMOVE")})
    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public abstract static class SubscribeEvent {
        private EventType type;
        private String pipelineName;

        public void validate() {
            Assert2.notNullOf(type, "type");
            Assert2.hasText(pipelineName, "pipelineName");
        }
    }

    @Getter
    @AllArgsConstructor
    public enum EventType {
        ADD(AddSubscribeEvent.class),
        UPDATE(UpdateSubscribeEvent.class),
        REMOVE(RemoveSubscribeEvent.class);
        final Class<? extends SubscribeEvent> eventClass;
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class AddSubscribeEvent extends SubscribeEvent {
        private List<SubscriberInfo> subscribers;

        @Override
        public void validate() {
            super.validate();
            Assert2.notEmptyOf(subscribers, "subscribers");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class UpdateSubscribeEvent extends SubscribeEvent {
        private List<SubscriberInfo> subscribers;

        @Override
        public void validate() {
            super.validate();
            Assert2.notEmptyOf(subscribers, "subscribers");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    public static class RemoveSubscribeEvent extends SubscribeEvent {
        private List<String> subscriberIds;

        @Override
        public void validate() {
            super.validate();
            Assert2.notEmptyOf(subscriberIds, "subscriberIds");
        }
    }

}
