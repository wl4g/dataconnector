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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.config.KafkaSubscriberProperties;
import com.wl4g.kafkasubscriber.facade.SubscribeFacade;
import com.wl4g.kafkasubscriber.meter.SubscribeMeter;
import com.wl4g.kafkasubscriber.sink.SubscriberRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.context.ApplicationContext;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.support.Acknowledgment;

import java.io.Closeable;
import java.util.List;

/**
 * The {@link AbstractBatchMessageDispatcher}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class AbstractBatchMessageDispatcher
        implements BatchAcknowledgingMessageListener<String, ObjectNode>, InitializingBean, Closeable {

    protected final ApplicationContext context;
    protected final KafkaSubscriberProperties.SubscribePipelineProperties config;
    protected final SubscribeFacade facade;
    protected final SubscriberRegistry registry;

    public AbstractBatchMessageDispatcher(ApplicationContext context,
                                          KafkaSubscriberProperties.SubscribePipelineProperties config,
                                          SubscribeFacade facade, SubscriberRegistry registry) {
        this.context = Assert2.notNullOf(context, "context");
        this.config = Assert2.notNullOf(config, "config");
        this.facade = Assert2.notNullOf(facade, "facade");
        this.registry = Assert2.notNullOf(registry, "registry");
    }

    @Override
    public void onMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack) {
        try {
            SubscribeMeter.getDefault().counter(
                    SubscribeMeter.MetricsName.shared_consumed.getName(),
                    SubscribeMeter.MetricsName.shared_consumed.getHelp(),
                    SubscribeMeter.MetricsTag.TOPIC, config.getSource().getTopicPattern().toString(),
                    SubscribeMeter.MetricsTag.GROUP_ID, config.getSource().getGroupId()
            ).increment();
            final Timer timer = SubscribeMeter.getDefault().timer(
                    SubscribeMeter.MetricsName.shared_consumed_time.getName(),
                    SubscribeMeter.MetricsName.shared_consumed_time.getHelp(),
                    SubscribeMeter.DEFAULT_PERCENTILES,
                    SubscribeMeter.MetricsTag.TOPIC, config.getSource().getTopicPattern().toString(),
                    SubscribeMeter.MetricsTag.GROUP_ID, config.getSource().getGroupId()
            );
            timer.record(() -> doOnMessage(records, ack));
        } catch (Throwable ex) {
            log.error(String.format("Failed to process message. - %s", records), ex);
            // Submit directly if no quality of service is required.
            if (!config.getFilter().isBestQoS()) {
                ack.acknowledge();
            }
        }
    }

    protected abstract void doOnMessage(List<ConsumerRecord<String, ObjectNode>> records, Acknowledgment ack);

}




