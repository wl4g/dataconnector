/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.dispatch;

import com.wl4g.kafkasubscriber.config.KafkaConsumerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import lombok.extern.slf4j.Slf4j;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.lang.Assert2.notNullOf;

/**
 * The {@link KafkaSubscriberBootstrap}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class KafkaSubscriberBootstrap implements ApplicationRunner, Closeable {

    private final ApplicationContext context;
    private final KafkaSubscriberProperties config;
    private final SubscribeFacade subscribeFacade;
    private final SubscriberRegistry subscriberRegistry;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> filterSubscribers;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> sinkSubscribers;

    public KafkaSubscriberBootstrap(@NotNull ApplicationContext context,
                                    @NotNull KafkaSubscriberProperties config,
                                    @NotNull SubscribeFacade subscribeFacade,
                                    @NotNull SubscriberRegistry subscriberRegistry) {
        this.context = notNullOf(context, "context");
        this.config = notNullOf(config, "config");
        this.subscribeFacade = notNullOf(subscribeFacade, "subscribeFacade");
        this.subscriberRegistry = notNullOf(subscriberRegistry, "subscriberRegistry");
        this.filterSubscribers = new ConcurrentHashMap<>(config.getPipelines().size());
        this.sinkSubscribers = new ConcurrentHashMap<>(config.getPipelines().size());
    }

    public Map<String, ConcurrentMessageListenerContainer<String, String>> getFilterSubscribers() {
        return Collections.unmodifiableMap(filterSubscribers);
    }

    public Map<String, ConcurrentMessageListenerContainer<String, String>> getSinkSubscribers() {
        return Collections.unmodifiableMap(sinkSubscribers);
    }

    @Override
    public void close() throws IOException {
        filterSubscribers.forEach((groupId, subscriber) -> {
            try {
                log.info("Stopping filter subscriber group id: {}", groupId);
                subscriber.stop();
                log.info("Stopped filter subscriber group id: {}", groupId);
            } catch (Throwable ex) {
                log.error("Failed to stop filter subscriber group id: {}", groupId, ex);
            }
        });
        sinkSubscribers.forEach((groupId, subscriber) -> {
            try {
                log.info("Stopping sink subscriber group id: {}", groupId);
                subscriber.stop();
                log.info("Stopped sink subscriber group id: {}", groupId);
            } catch (Throwable ex) {
                log.error("Failed to stop sink subscriber group id: {}", groupId, ex);
            }
        });
    }

    @Override
    public void run(ApplicationArguments args) throws Exception {
        registerFilteringAndSenderSubscriber();
        startAllSubscriber();
    }

    private void registerFilteringAndSenderSubscriber() {
        config.getPipelines().forEach(pipeline -> {
            // Register filter dispatcher.
            final KafkaSubscriberProperties.SourceProperties source = pipeline.getSource();
            final FilterBatchMessageDispatcher filterDispatcher = new FilterBatchMessageDispatcher(context, pipeline, subscribeFacade, subscriberRegistry);
            final ConcurrentMessageListenerContainer<String, String> filterSubscriber =
                    new KafkaConsumerBuilder(source.getConsumerProps())
                            .buildSubscriber(source.getTopicPattern(),
                                    source.getParallelism(),
                                    filterDispatcher);
            if (Objects.nonNull(filterSubscribers.putIfAbsent(source.getGroupId(), filterSubscriber))) {
                throw new IllegalStateException(
                        String.format("Duplicate filter subscriber group id: %s", source.getGroupId()));
            }

            // Register sink dispatcher.
            final KafkaSubscriberProperties.SinkProperties sink = pipeline.getSink();
            if (!sink.isEnable()) {
                log.info("Pipeline sink is disabled, skip register sink dispatcher, pipeline: {}", pipeline);
                return;
            }
            final SinkBatchMessageDispatcher sinkDispatcher = new SinkBatchMessageDispatcher(context, pipeline, subscribeFacade, subscriberRegistry);
            final ConcurrentMessageListenerContainer<String, String> sinkSubscriber =
                    new KafkaConsumerBuilder(sink.getConsumerProps())
                            .buildSubscriber(Pattern.compile(pipeline.getFilter().getTopicPrefix().concat(".*")),
                                    sink.getParallelism(),
                                    sinkDispatcher);
            if (Objects.nonNull(sinkSubscribers.putIfAbsent(sink.getGroupId(), sinkSubscriber))) {
                throw new IllegalStateException(
                        String.format("Duplicate sink subscriber group id: %s", sink.getGroupId()));
            }
        });
    }

    private void startAllSubscriber() {
        log.info("Starting all pipeline filter subscribers for {}...", filterSubscribers.size());
        filterSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);
        log.info("Starting all pipeline sink subscribers for {}...", sinkSubscribers.size());
        sinkSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);
    }

}
