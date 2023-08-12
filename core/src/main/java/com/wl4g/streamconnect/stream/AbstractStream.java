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

package com.wl4g.streamconnect.stream;

import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.framework.NamedStreamConnectSpi;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;

import javax.validation.constraints.Max;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static java.lang.System.getenv;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

/**
 * The {@link AbstractStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class AbstractStream implements Closeable {
    private final StreamContext context;

    public AbstractStream(@NotNull final StreamContext context) {
        this.context = requireNonNull(context, "context must not be null");
        this.context.validate();
    }

    public String getDescription() {
        return getClass().getSimpleName();
    }

    public List<String> getBasedMeterTags() {
        return emptyList();
    }

    protected IStreamConnectConfigurator getConfigurator() {
        return getContext().getConfig().getConfigurator();
    }

    protected ConnectorConfig getConnectorConfig() {
        return getContext().getConnectorConfig();
    }

    protected CachingChannelRegistry getRegistry() {
        return getContext().getRegistry();
    }

    protected ApplicationEventPublisher getEventPublisher() {
        return getContext().getConfig().getEventPublisher();
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static abstract class BaseStreamConfig extends NamedStreamConnectSpi {
        @Builder.Default
        private @Min(0) @Max(64) int parallelism = 1;

        @Override
        public void validate() {
            super.validate();

            // for example: kafka stream limit.
            // By force: min(concurrency, topicPartitions.length)
            // see:org.springframework.kafka.listener.ConcurrentMessageListenerContainer#doStart()
            // But it's a pity that spring doesn't get it dynamically from broker.
            // Therefore, tuning must still be set manually, generally equal to the number of partitions.
            Assert2.isTrueOf(parallelism > 0 && parallelism < 64, "parallelism > 0 && parallelism < 64");
        }

        public static String getStreamProviderTypeName(@NotBlank String streamConfigTypeName) {
            Assert2.hasTextOf(streamConfigTypeName, "streamConfigTypeName");
            return streamConfigTypeName + "_STREAM";
        }
    }

    @Getter
    @Setter
    public static class StreamContext {
        private final @NotNull Environment environment;
        private final @NotNull StreamConnectConfiguration config;
        private final @NotNull ConnectorConfig connectorConfig;
        private final @NotNull CachingChannelRegistry registry;
        private final @NotNull StreamConnectEngineBootstrap bootstrap;
        private final @NotNull Map<String, Object> extraAttributes = new ConcurrentHashMap<>();

        public StreamContext(Environment environment,
                             StreamConnectConfiguration config,
                             ConnectorConfig connectorConfig,
                             CachingChannelRegistry registry,
                             StreamConnectEngineBootstrap bootstrap) {
            this.environment = requireNonNull(environment, "environment must not be null");
            this.config = requireNonNull(config, "config must not be null");
            this.connectorConfig = requireNonNull(connectorConfig, "connectorConfig must not be null");
            this.registry = requireNonNull(registry, "registry must not be null");
            this.bootstrap = requireNonNull(bootstrap, "bootstrap must not be null");
        }

        public void validate() {
            requireNonNull(config, "config must not be null");
            requireNonNull(connectorConfig, "connectorConfig must not be null");
            requireNonNull(registry, "registry must not be null");
            requireNonNull(bootstrap, "bootstrap must not be null");
        }
    }

    public interface MessageRecord<K, V> {

        default Map<String, V> getMetadata() {
            return null;
        }

        K getKey();

        V getValue();

        long getTimestamp();
    }

    public interface DelegateMessageRecord<K, V> extends MessageRecord<K, V> {
        MessageRecord<K, V> getOriginal();
    }

    public static final String KEY_TENANT = getenv().getOrDefault("STREAM_TENANT_ID", "$$tenant");
    public static final String KEY_CHANNEL = getenv().getOrDefault("STREAM_CHANNEL_ID", "$$channel");
    public static final String KEY_SEQUENCE = getenv().getOrDefault("STREAM_IS_SEQUENCE", "$$sequence");
}
