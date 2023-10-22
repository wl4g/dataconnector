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

package com.wl4g.dataconnector.util;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import lombok.Getter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;
import static java.util.Objects.requireNonNull;

/**
 * The {@link KafkaConsumerBuilder}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class KafkaConsumerBuilder {
    private final Map<String, Object> consumerProps;
    private final ConcurrentKafkaListenerContainerFactory<String, Object> factory;

    public KafkaConsumerBuilder(@NotNull Map<String, Object> consumerProps) {
        this.consumerProps = requireNonNull(consumerProps, "consumerProps must not be null");
        this.factory = buildKafkaListenerContainerFactory();
    }

    public ConcurrentMessageListenerContainer<String, Object> buildContainer(
            final @NotNull Pattern topicPattern,
            final @NotBlank String groupId,
            final @NotNull BatchAcknowledgingMessageListener<String, Object> listener) {
        // force: min(concurrency, topicPartitions.length)
        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        // But it's a pity that spring doesn't get it dynamically from broker.
        // Therefore, tuning must still be set manually, generally equal to the number of partitions.
        return buildContainer(topicPattern, groupId, 1, listener);
    }

    public ConcurrentMessageListenerContainer<String, Object> buildContainer(
            final @NotNull Pattern topicPattern,
            final @NotBlank String groupId,
            final @Min(1) int concurrency,
            final @NotNull BatchAcknowledgingMessageListener<String, Object> listener) {
        notNullOf(topicPattern, "topicPattern");
        notNullOf(listener, "listener");
        Assert2.isTrueOf(concurrency >= 1, "concurrency must be greater than or equal to 1");

        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        final ConcurrentMessageListenerContainer<String, Object> container = factory.createContainer(topicPattern);
        // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#doInvokeOnMessage()
        // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#doInvokeBatchOnMessage()
        // see:org.springframework.kafka.listener.BatchMessageListener
        // see:org.springframework.kafka.listener.GenericMessageListener
        // see:org.springframework.kafka.listener.MessageListener
        container.getContainerProperties().setMessageListener(listener);
        container.getContainerProperties().setGroupId(groupId);
        // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#isAnyManualAck
        // see:org.springframework.kafka.listener.KafkaMessageListenerContainer.ListenerConsumer#doInvokeBatchOnMessage()
        container.getContainerProperties().setAckMode(ContainerProperties.AckMode.MANUAL);
        container.setBeanName(groupId.concat("_Bean"));
        container.setConcurrency(concurrency);
        return container;
    }

    private ConcurrentKafkaListenerContainerFactory<String, Object> buildKafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, Object> factory =
                new ConcurrentKafkaListenerContainerFactory<>();
        factory.setConsumerFactory(new DefaultKafkaConsumerFactory<>(consumerProps));
        factory.setBatchListener(true);
        factory.afterPropertiesSet();
        return factory;
    }

    /**
     * {@link org.apache.kafka.common.serialization.StringDeserializer}
     */
    public static class ObjectNodeDeserializer implements Deserializer<ObjectNode> {
        private String encoding = StandardCharsets.UTF_8.name();

        @Override
        public void configure(Map<String, ?> configs, boolean isKey) {
            String propertyName = isKey ? "key.deserializer.encoding" : "value.deserializer.encoding";
            Object encodingValue = configs.get(propertyName);
            if (Objects.isNull(encodingValue)) {
                encodingValue = configs.get("deserializer.encoding");
            }
            if (encodingValue instanceof String) {
                encoding = (String) encodingValue;
            }
        }

        @Override
        public ObjectNode deserialize(String topic, byte[] data) {
            try {
                if (Objects.isNull(data)) {
                    return null;
                } else {
                    return (ObjectNode) parseToNode(new String(data, encoding));
                }
            } catch (UnsupportedEncodingException e) {
                throw new SerializationException(
                        "Error when deserializing byte[] to string due to unsupported encoding " + encoding);
            }
        }
    }

}