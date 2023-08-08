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

package com.wl4g.streamconnect.config;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.Getter;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Deserializer;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.BatchAcknowledgingMessageListener;
import org.springframework.kafka.listener.ConcurrentMessageListenerContainer;
import org.springframework.kafka.listener.ContainerProperties;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The {@link KafkaConsumerBuilder}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class KafkaConsumerBuilder {
    private final Map<String, Object> consumerProps;
    private final ConcurrentKafkaListenerContainerFactory<String, String> factory;

    public KafkaConsumerBuilder(@NotNull Map<String, String> consumerProps) {
        // cleanup consumer properties.
        this.consumerProps = safeMap(consumerProps).entrySet().stream()
                .filter(e -> !isBlank(e.getKey()) && nonNull(e.getValue()))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        this.factory = buildKafkaListenerContainerFactory();
    }

    public ConcurrentMessageListenerContainer<String, String> buildSubscriber(
            final @NotNull Pattern topicPattern,
            final @NotBlank String groupId,
            final @NotNull BatchAcknowledgingMessageListener<String, String> listener) {
        // force: min(concurrency, topicPartitions.length)
        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        // But it's a pity that spring doesn't get it dynamically from broker.
        // Therefore, tuning must still be set manually, generally equal to the number of partitions.
        return buildSubscriber(topicPattern, groupId, Integer.MAX_VALUE, listener);
    }

    public ConcurrentMessageListenerContainer<String, String> buildSubscriber(
            final @NotNull Pattern topicPattern,
            final @NotBlank String groupId,
            final @NotNull Integer concurrency,
            final @NotNull Object listener) {
        notNullOf(topicPattern, "topicPattern");
        notNullOf(listener, "listener");
        // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
        final ConcurrentMessageListenerContainer<String, String> container = factory.createContainer(topicPattern);
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

    private ConcurrentKafkaListenerContainerFactory<String, String> buildKafkaListenerContainerFactory() {
        final ConcurrentKafkaListenerContainerFactory<String, String> factory =
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