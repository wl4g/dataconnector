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

package com.wl4g.kafkasubscriber.config;

import com.wl4g.infra.common.lang.Assert2;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.Properties;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;

/**
 * The {@link KafkaProducerBuilder}
 *
 * @author James Wong
 * @since v1.0
 **/
public class KafkaProducerBuilder {
    private final Map<String, Object> producerProps;
    private final ProducerFactory<String, String> factory;

    public KafkaProducerBuilder(@NotNull Properties producerProps) {
        // cleanup producer key or value is null properties.
        this.producerProps = safeMap(producerProps).entrySet().stream()
                .filter(e -> nonNull(e.getKey()) && nonNull(e.getValue()))
                .collect(toMap(e -> (String) e.getKey(), Map.Entry::getValue));
        this.factory = buildProducerFactory();
    }

    private ProducerFactory<String, String> buildProducerFactory() {
        // Props map generic type conversion.
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    public Producer<String, String> buildProducer() {
        return factory.createProducer();
    }

    public KafkaTemplate<String, String> buildKafkaTemplate() {
        return new KafkaTemplate<>(factory);
    }

    public static Producer<String, String> buildDefaultAcknowledgedKafkaProducer(String bootstrapServers) {
        // TODO support more custom configuration.
        Assert2.hasTextOf(bootstrapServers, "bootstrapServers");
        Properties configProps = new Properties();
        configProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        configProps.put(ProducerConfig.ACKS_CONFIG, "all");
        configProps.put(ProducerConfig.RETRIES_CONFIG, "3");
        configProps.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        configProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        configProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        return new KafkaProducerBuilder(configProps).buildProducer();
    }

}