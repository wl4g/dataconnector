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
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.framework.NamedStreamConnectSpi;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.springframework.core.env.Environment;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.time.Duration;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;

import static java.util.Objects.requireNonNull;

/**
 * The {@link IStreamConnectCoordinator}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface IStreamConnectCoordinator extends Runnable, Closeable {

    CoordinatorConfig getCoordinatorConfig();

    void start();

    void stop();

    /**
     * IMPORTANT: used to ensure that the coordinator is ready when engine bootstrap starts source/sinks streams.
     * <p>
     * Otherwise, if the coordinator/service discoverer is not ready, it has not yet assigned of the sharding channels.
     *
     * @throws TimeoutException
     * @throws InterruptedException
     */
    void waitForReady() throws TimeoutException, InterruptedException;

    @Override
    default void close() {
        stop();
    }

    void onDiscovery(List<ServerInstance> instances);

    void onBusEvent(BusEvent event);

    IBusPublisher getBusPublisher();

    // ----- Coordinator sharding. -----

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    @EqualsAndHashCode(of = "instanceId")
    class ServerInstance implements Comparable<ServerInstance> {
        private String instanceId;
        private Boolean selfInstance;
        private String host;
        private Properties metadata;

        @Override
        public int compareTo(ServerInstance other) {
            return getInstanceId().compareTo(other.getInstanceId());
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    @AllArgsConstructor
    @ToString
    class ShardingInfo {
        private int total;
        private List<Integer> items;
    }

    // ----- Coordinator Bus event. -----

    @Schema(oneOf = {AddChannelEvent.class, UpdateChannelEvent.class, RemoveChannelEvent.class},
            discriminatorProperty = "type")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type", visible = true)
    @JsonSubTypes({@Type(value = AddChannelEvent.class, name = "ADD"),
            @Type(value = UpdateChannelEvent.class, name = "UPDATE"),
            @Type(value = RemoveChannelEvent.class, name = "REMOVE")})
    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    abstract class BusEvent {
        private EventType type;
        private String connectorName;

        public void validate() {
            requireNonNull(type, "type must not be null");
            Assert2.hasText(connectorName, "connectorName");
        }
    }

    @Getter
    @AllArgsConstructor
    enum EventType {
        ADD(AddChannelEvent.class),
        UPDATE(UpdateChannelEvent.class),
        REMOVE(RemoveChannelEvent.class);
        final Class<? extends BusEvent> eventClass;
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    class AddChannelEvent extends BusEvent {
        private List<ChannelInfo> channels;

        @Override
        public void validate() {
            super.validate();
            Assert2.notEmptyOf(channels, "channels");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    class UpdateChannelEvent extends BusEvent {
        private List<ChannelInfo> channels;

        @Override
        public void validate() {
            super.validate();
            Assert2.notEmptyOf(channels, "channels");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    @AllArgsConstructor
    class RemoveChannelEvent extends BusEvent {
        private List<String> channelIds;

        @Override
        public void validate() {
            super.validate();
            Assert2.notEmptyOf(channelIds, "channelIds");
        }
    }

    // ----- Coordinator Configuration. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    abstract class CoordinatorConfig {
        private @NotBlank String shardingStrategy;
        @Min(0)
        private @Builder.Default long waitReadyTimeoutMs = 120_000L;

        public void validate() {
            Assert2.hasTextOf(shardingStrategy, "shardingStrategy");
            Assert2.isTrueOf(waitReadyTimeoutMs > 0, "waitReadyTimeoutMs > 0");
        }
    }

    // ----- Coordinator Provider. -----

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    abstract class CoordinatorProvider extends NamedStreamConnectSpi {
        public abstract IStreamConnectCoordinator obtain(Environment environment,
                                                         StreamConnectConfiguration config,
                                                         IStreamConnectConfigurator customizer,
                                                         CachingChannelRegistry registry,
                                                         StreamConnectMeter meter);
    }

    // ----- Coordinator Bus Publisher. -----

    interface IBusPublisher {
        void publishSync(@NotNull List<BusEvent> events,
                         @NotNull Duration timeout) throws InterruptedException, TimeoutException;

        List<Future<?>> publishAsync(@NotNull List<BusEvent> events);
    }

}
