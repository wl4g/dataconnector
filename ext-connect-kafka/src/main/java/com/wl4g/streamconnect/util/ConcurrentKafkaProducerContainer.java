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

package com.wl4g.streamconnect.util;

import com.google.common.base.Preconditions;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.exception.StreamConnectException;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.lang.String.valueOf;
import static java.util.Collections.synchronizedList;
import static java.util.Collections.synchronizedMap;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.collections.ListUtils.unmodifiableList;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.kafka.clients.producer.ProducerConfig.BOOTSTRAP_SERVERS_CONFIG;
import static org.apache.kafka.clients.producer.ProducerConfig.CLIENT_ID_CONFIG;

/**
 * The {@link ConcurrentKafkaProducerContainer}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class ConcurrentKafkaProducerContainer implements Closeable {

    /**
     * for solved problem example:
     * <code>
     * javax.management.InstanceAlreadyExistsException: kafka.producer:type=app-info,id="PLAINTEXT://192.168.64.7:59092-c1001-1-1"
     * at com.sun.jmx.mbeanserver.Repository.addMBean(Repository.java:437)
     * at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerWithRepository(DefaultMBeanServerInterceptor.java:1898)
     * at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerDynamicMBean(DefaultMBeanServerInterceptor.java:966)
     * at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerObject(DefaultMBeanServerInterceptor.java:900)
     * at com.sun.jmx.interceptor.DefaultMBeanServerInterceptor.registerMBean(DefaultMBeanServerInterceptor.java:324)
     * at com.sun.jmx.mbeanserver.JmxMBeanServer.registerMBean(JmxMBeanServer.java:522)
     * at org.apache.kafka.common.utils.AppInfoParser.registerAppInfo(AppInfoParser.java:64)
     * at org.apache.kafka.clients.producer.KafkaProducer.<init>(KafkaProducer.java:436)
     * at org.apache.kafka.clients.producer.KafkaProducer.<init>(KafkaProducer.java:292)
     * at org.springframework.kafka.core.DefaultKafkaProducerFactory.createRawProducer(DefaultKafkaProducerFactory.java:937)
     * at org.springframework.kafka.core.DefaultKafkaProducerFactory.createKafkaProducer(DefaultKafkaProducerFactory.java:788)
     * at org.springframework.kafka.core.DefaultKafkaProducerFactory.doCreateProducer(DefaultKafkaProducerFactory.java:758)
     * at org.springframework.kafka.core.DefaultKafkaProducerFactory.createProducer(DefaultKafkaProducerFactory.java:733)
     * at org.springframework.kafka.core.DefaultKafkaProducerFactory.createProducer(DefaultKafkaProducerFactory.java:727)
     * </code>
     */
    private static final AtomicInteger GLOBAL_INDEX = new AtomicInteger(0);

    private final String name;
    private final int parallelism;
    private final String transactionIdPrefix;
    private final Map<String, Object> producerProps;
    private List<DefaultKafkaProducerFactory<String, Object>> producerFactories;

    public ConcurrentKafkaProducerContainer(@NotBlank String name,
                                            @Min(1) int parallelism,
                                            @Null String transactionIdPrefix,
                                            @NotNull Map<String, Object> producerProps) {
        this.name = Assert2.hasTextOf(name, "name");
        this.transactionIdPrefix = transactionIdPrefix;
        Preconditions.checkArgument(parallelism >= 1, "required is parallelism >= 1");
        this.parallelism = parallelism;
        this.producerProps = synchronizedMap(requireNonNull(producerProps, "producerProps must not be null"));
        init();
    }

    public List<DefaultKafkaProducerFactory<String, Object>> getProducerFactories() {
        return unmodifiableList(safeList(producerFactories));
    }

    public int size() {
        return getProducerFactories().size();
    }

    public synchronized void init() {
        // Initializing kafka producer factories if necessary.
        if (isNull(producerFactories)) {
            if (log.isInfoEnabled()) {
                log.info("{} :: Initializing kafka producer factories with parallelism: {}, producerPropsSupplier: {}",
                        name, parallelism, producerProps);
            }
            this.producerFactories = synchronizedList(Stream.of(parallelism)
                    .map(i -> buildProducerFactory(producerProps, i))
                    .collect(toList()));
            if (log.isInfoEnabled()) {
                log.info("{} :: Initialized kafka producer factories with parallelism: {}, producerPropsSupplier: {}",
                        name, parallelism, producerProps);
            }
        }
    }

    /**
     * Build kafka producer factory.
     *
     * @param producerProps
     * @param index
     * @return
     */
    private DefaultKafkaProducerFactory<String, Object> buildProducerFactory(@NotNull Map<String, Object> producerProps,
                                                                             int index) {
        requireNonNull(producerProps, "producerProps must not be null");
        final Map<String, Object> mergedProps = new HashMap<>(producerProps);
        final String clientId = valueOf(mergedProps.getOrDefault(CLIENT_ID_CONFIG, mergedProps
                .get(BOOTSTRAP_SERVERS_CONFIG)))
                .concat("-")
                .concat(valueOf(name))
                .concat("-")
                .concat(valueOf(index))
                .concat("-")
                .concat(valueOf(GLOBAL_INDEX.getAndIncrement())); //
        mergedProps.put(CLIENT_ID_CONFIG, clientId);
        final DefaultKafkaProducerFactory<String, Object> factory = new DefaultKafkaProducerFactory<>(mergedProps);
        if (isNotBlank(transactionIdPrefix)) {
            factory.setTransactionIdPrefix(transactionIdPrefix);
        }
        return factory;
    }

    // TODO should lock in scaling
    public synchronized void scaling(@Min(1) int parallelism) {
        Preconditions.checkArgument(parallelism >= 1, "required is parallelism >= 1");

        // Scaling producer factories instance if necessary.
        if (parallelism > producerFactories.size()) {
            final int beforeParallelism = producerFactories.size();
            if (parallelism > beforeParallelism) {
                final int delta = parallelism - beforeParallelism;
                if (log.isInfoEnabled()) {
                    log.info("{} :: Scaling kafka producer factories with parallelism: {}, delta: {}",
                            name, parallelism, delta);
                }
                for (int i = 0; i < delta; i++) {
                    producerFactories.add(buildProducerFactory(producerProps, beforeParallelism + i));
                }
                if (log.isInfoEnabled()) {
                    log.info("{} :: Scaled kafka producer factories with parallelism: {}",
                            name, parallelism);
                }
            }
        } else if (parallelism < producerFactories.size()) {
            // TODO Scaling down producer instances has not yet been implemented!
            log.warn("{} :: Scaling down producer instances has not yet been implemented!", name);
        }
    }

    // TODO should lock in resetting
    public synchronized void reset(@NotNull Map<String, Object> newProducerProps) {
        requireNonNull(newProducerProps, "newProducerProps must not be null");

        boolean isChanged = false;
        // Check for bootstrap servers.
        final Map<String, Object> oldProducerProps = producerProps;
        final String oldBootstrapServers = (String) oldProducerProps.get(BOOTSTRAP_SERVERS_CONFIG);
        final String newBootstrapServers = (String) newProducerProps.get(BOOTSTRAP_SERVERS_CONFIG);
        if (!StringUtils.equals(oldBootstrapServers, newBootstrapServers)) {
            isChanged = true;
        }
        // Check for other properties.
        final Map<String, Object> changedProps = oldProducerProps
                .entrySet()
                .stream()
                .filter(e -> !StringUtils.equals(valueOf(oldProducerProps.get(e.getKey())),
                        valueOf(e.getValue())))
                .collect(toMap(Map.Entry::getKey, Map.Entry::getValue));
        if (!changedProps.isEmpty()) {
            isChanged = true;
        }
        // Resetting producer if changed.
        if (isChanged) {
            this.producerProps.putAll(changedProps);
            for (DefaultKafkaProducerFactory<String, Object> producerFactory : producerFactories) {
                if (log.isInfoEnabled()) {
                    log.info("{} :: Resetting kafka producer factory with changed properties: {}", name, changedProps);
                }
                producerFactory.updateConfigs(changedProps);
                producerFactory.reset();
                if (log.isInfoEnabled()) {
                    log.info("{} :: Resetted kafka producer factory with changed properties: {}", name, changedProps);
                }
            }
        }
    }

    public Producer<String, Object> obtainFirstProducer() {
        if (producerFactories.isEmpty()) {
            throw new StreamConnectException("The producer factory instances is empty " +
                    "and has been validated during initialization. This error should not occur.");
        }
        return producerFactories.get(0).createProducer(); // Get or create producer.
    }

    public Producer<String, Object> obtainDetermineProducer(@NotNull String key,
                                                            boolean isSequence) {
        requireNonNull(key, "key must not be null");

        DefaultKafkaProducerFactory<String, Object> producerFactory = producerFactories
                .get(nextInt(0, producerFactories.size()));
        if (isSequence) {
            //final String key = valueOf(channel.getId());
            final int index = Assignments.assign(key, producerFactories.size());
            producerFactory = producerFactories.get(index);
            if (log.isDebugEnabled()) {
                log.debug("{} :: Determined to producer factory of index: {}", name, index);
            }
        }
        if (isNull(producerFactory)) {
            throw new StreamConnectException(String.format("%s :: Could not get producer factory for key: %s", name, key));
        }
        if (log.isDebugEnabled()) {
            log.debug("{} :: Using producer factory for key: {}", name, key);
        }

        return producerFactory.createProducer(); // Get or create producer.
    }

    @Override
    public void close() throws IOException {
        close(Duration.ofSeconds(120));
    }

    public synchronized void close(@NotNull Duration timeout) throws IOException {
        final Iterator<DefaultKafkaProducerFactory<String, Object>> it =
                safeList(producerFactories).iterator();
        while (it.hasNext()) {
            try {
                final DefaultKafkaProducerFactory<String, Object> factory = it.next();
                factory.setPhysicalCloseTimeout((int) timeout.getSeconds());
                if (log.isInfoEnabled()) {
                    log.info("{} :: Closing kafka producer factory : {}", name, factory);
                }
                factory.destroy();
                if (log.isInfoEnabled()) {
                    log.info("{} :: Closed kafka producer factories.", name);
                }
                it.remove();
            } catch (Throwable ex) {
                log.error("{} :: Failed to close kafka producer factory.", name, ex);
            }
        }
    }

    public static ConcurrentKafkaProducerContainer buildDefaultAcknowledgedProducerContainer(
            @NotBlank String name,
            @NotBlank String bootstrapServers) {
        Assert2.hasTextOf(bootstrapServers, "bootstrapServers");

        Map<String, Object> props = new HashMap<>(8);
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        final String transactionIdPrefix = "txid-".concat(name);
        return new ConcurrentKafkaProducerContainer(name, 1, transactionIdPrefix, props);
    }

}
