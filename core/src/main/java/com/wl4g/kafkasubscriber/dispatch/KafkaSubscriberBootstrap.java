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

import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.config.KafkaConsumerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaProducerBuilder;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.sink.CachingSubscriberRegistry;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.io.IOException;
import java.util.Collections;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
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
    private final CachingSubscriberRegistry subscriberRegistry;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> filterSubscribers;
    private final Map<String, ConcurrentMessageListenerContainer<String, String>> sinkSubscribers;

    public KafkaSubscriberBootstrap(@NotNull ApplicationContext context,
                                    @NotNull KafkaSubscriberProperties config,
                                    @NotNull SubscribeFacade subscribeFacade,
                                    @NotNull CachingSubscriberRegistry subscriberRegistry) {
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
            final KafkaSubscriberProperties.SourceProperties source = pipeline.getSource();

            // Create acknowledge producer.
            final Producer<String, String> acknowledgeProducer = KafkaProducerBuilder.buildDefaultAcknowledgedKafkaProducer(
                    source.getConsumerProps().get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG));

            // Register filter dispatcher.
            filterSubscribers.computeIfAbsent(source.getGroupId(), sourceGroupId -> {
                final FilterBatchMessageDispatcher filterDispatcher = new FilterBatchMessageDispatcher(context, pipeline, subscribeFacade,
                        subscriberRegistry, source.getGroupId(), acknowledgeProducer);
                return new KafkaConsumerBuilder(source.getConsumerProps())
                        .buildSubscriber(source.getTopicPattern(), sourceGroupId, source.getParallelism(), filterDispatcher);
            });

            // Register sink dispatchers If necessary. (per subscribers a sink dispatcher instance)
            final KafkaSubscriberProperties.SinkProperties sink = pipeline.getSink();
            if (!sink.isEnable()) {
                log.info("Pipeline sink is disabled, skip register sink dispatcher, pipeline: {}", pipeline);
                return;
            }
            // TODO using subscriber registry to find subscribers.
            safeList(subscribeFacade.findSubscribers(SubscriberInfo.builder().build())).forEach(subscriber -> {
                subscriber.validate();
                final String sinkFromTopic = subscribeFacade.generateFilteredTopic(pipeline.getFilter(), subscriber.getId());
                final String sinkGroupId = subscribeFacade.generateSinkGroupId(sink, subscriber.getId());
                sinkSubscribers.computeIfAbsent(sinkGroupId, _sinkGroupId -> {
                    final SinkSubscriberBatchMessageDispatcher sinkDispatcher = new SinkSubscriberBatchMessageDispatcher(context, pipeline, subscribeFacade,
                            subscriberRegistry, _sinkGroupId, acknowledgeProducer, subscriber);
                    return new KafkaConsumerBuilder(sink.getConsumerProps())
                            .buildSubscriber(Pattern.compile(sinkFromTopic), _sinkGroupId, sink.getParallelism(), sinkDispatcher);
                });
            });
        });
    }

    private void startAllSubscriber() {
        log.info("Starting all pipeline filter subscribers for {}...", filterSubscribers.size());
        filterSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);
        log.info("Starting all pipeline sink subscribers for {}...", sinkSubscribers.size());
        sinkSubscribers.values().forEach(ConcurrentMessageListenerContainer::start);
    }

}
