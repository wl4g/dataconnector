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

package com.wl4g.streamconnect.stream.sink.kafka;

import com.wl4g.infra.common.lang.Assert2;
import lombok.Getter;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

import static java.util.Objects.requireNonNull;

/**
 * The {@link KafkaProducerBuilder}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class KafkaProducerBuilder {
    private final Map<String, Object> producerProps;
    private final ProducerFactory<String, Object> factory;

    public KafkaProducerBuilder(@NotNull Map<String, Object> producerProps) {
        this.producerProps = requireNonNull(producerProps, "producerProps must not be null");
        this.factory = buildProducerFactory();
    }

    private ProducerFactory<String, Object> buildProducerFactory() {
        return new DefaultKafkaProducerFactory<>(producerProps);
    }

    public Producer<String, Object> buildProducer() {
        return factory.createProducer();
    }

    public KafkaTemplate<String, Object> buildKafkaTemplate() {
        return new KafkaTemplate<>(factory);
    }

    public static Producer<String, Object> buildDefaultAcknowledgedKafkaProducer(String bootstrapServers) {
        Assert2.hasTextOf(bootstrapServers, "bootstrapServers");

        Map<String, Object> props = new HashMap<>(8);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        return new KafkaProducerBuilder(props).buildProducer();
    }

}
