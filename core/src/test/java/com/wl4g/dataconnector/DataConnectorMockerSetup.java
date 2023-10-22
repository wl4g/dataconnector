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

package com.wl4g.dataconnector;

import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.ChannelInfo.*;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.config.DataConnectorProperties;
import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator;
import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.coordinator.IDataConnectorCoordinator;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.AbstractStream.StreamContext;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap;
import com.wl4g.dataconnector.stream.sink.SinkStream;
import com.wl4g.dataconnector.stream.source.SourceStream.SourceStreamConfig;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;

import java.util.List;
import java.util.Map;

import static java.util.Collections.singletonList;

/**
 * The {@link DataConnectorMockerSetup}
 *
 * @author James Wong
 * @since v1.0
 **/
public abstract class DataConnectorMockerSetup {

    public static DataConnectorMeter buildDefaultMockDataConnectorMeter() {
        PrometheusMeterRegistry mockMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        return new DataConnectorMeter(mockMeterRegistry, "unitTestApp", 65432);
    }

    public static DataConnectorProperties buildDefaultMockDataConnectorProperties() {
        return new DataConnectorProperties() {
            @Override
            public void afterPropertiesSet() {
            }

            @Override
            public IDataConnectorConfigurator.ConfiguratorProvider getConfigurator() {
                return new IDataConnectorConfigurator.ConfiguratorProvider() {
                    @Override
                    public String getType() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public IDataConnectorConfigurator obtain(Environment environment,
                                                             DataConnectorConfiguration config,
                                                             DataConnectorMeter meter) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public static DataConnectorConfiguration buildDefaultDataConnectorConfiguration() {
        DataConnectorProperties mockProperties = buildDefaultMockDataConnectorProperties();
        PrometheusMeterRegistry mockMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        DataConnectorMeter mockMeter = new DataConnectorMeter(mockMeterRegistry, "testApp", 12345);

        Environment mockEnvironment = new StandardEnvironment();
        ApplicationEventPublisher mockEventPublisher = event -> {
            throw new UnsupportedOperationException();
        };
        return new DataConnectorConfiguration(mockEnvironment, mockProperties, mockEventPublisher, mockMeter);
    }

    public static IDataConnectorConfigurator buildDefaultMockDataConnectorConfigurator() {
        return new IDataConnectorConfigurator() {
            @Override
            public List<? extends SourceStreamConfig> loadSourceConfigs(String connectorName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<ChannelInfo> loadChannels(String connectorName, IDataConnectorCoordinator.ShardingInfo sharding) {
                throw new UnsupportedOperationException();
            }
        };
    }

    public static StreamContext buildDefaultMockStreamContext() {
        ConnectorConfig mockConnectorConfig = new ConnectorConfig();
        mockConnectorConfig.setName("connector_1");

        DataConnectorConfiguration mockConfiguration = buildDefaultDataConnectorConfiguration();
        CachingChannelRegistry mockRegistry = new CachingChannelRegistry(mockConfiguration);
        DataConnectorEngineBootstrap mockBootstrap = buildDefaultMockDataConnectorEngineBootstrap(mockConfiguration.getEnvironment(), mockConfiguration, mockRegistry);

        return new StreamContext(mockConfiguration.getEnvironment(), mockConfiguration, mockConnectorConfig, mockRegistry, mockBootstrap);
    }

    public static DataConnectorEngineBootstrap buildDefaultMockDataConnectorEngineBootstrap(Environment environment,
                                                                                            DataConnectorConfiguration config,
                                                                                            CachingChannelRegistry registry) {
        return new DataConnectorEngineBootstrap(environment, config, registry);
    }

    public static ChannelInfo buildDefaultMockChannelInfo(String channelId, String connectorName, String firstRuleItemValue) {
        final ChannelInfo mockChannel = ChannelInfo.builder()
                .id(channelId)
                .name(channelId)
                .labels(singletonList(connectorName))
                .settingsSpec(SettingsSpec.builder()
                        .policySpec(PolicySpec.builder()
                                .sequence(true)
                                .rules(singletonList(RuleSpec.builder()
                                        .chain(singletonList(RuleItem.builder()
                                                .name("rule_1")
                                                .value(firstRuleItemValue)
                                                .build()))
                                        .build()))
                                .build())
                        .checkpointSpec(CheckpointSpec.builder()
                                .servers("localhost:9092")
                                .retentionTime(60 * 60 * 1000L)
                                .retentionBytes(1024 * 1024 * 1024L)
                                .build())
                        .sinkSpec(new SinkStream.SinkStreamConfig() {
                            @Override
                            public String getType() {
                                return "KAFKA_SINK";
                            }

                            @Override
                            public String getQos() {
                                return "qos_1";
                            }
                        })
                        .build())
                .build();
        mockChannel.validate();
        return mockChannel;
    }

    public static MessageRecord<String, Object> buildDefaultMockMessageRecord(String key, Map<String, Object> metadata) {
        return new MessageRecord<String, Object>() {
            @Override
            public Map<String, Object> getMetadata() {
                return metadata;
            }

            @Override
            public String getKey() {
                return key;
            }

            @Override
            public Object getValue() {
                throw new UnsupportedOperationException();
            }

            @Override
            public long getTimestamp() {
                throw new UnsupportedOperationException();
            }
        };
    }

}
