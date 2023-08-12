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

package com.wl4g.streamconnect.stream.process;

import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectProperties;
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.AbstractStream;
import com.wl4g.streamconnect.stream.StreamConnectEngineBootstrap;
import com.wl4g.streamconnect.stream.source.SourceStream;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.core.env.Environment;
import org.springframework.core.env.StandardEnvironment;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static java.util.Collections.singletonMap;
import static java.util.stream.Collectors.toList;

/**
 * The {@link ProcessStreamTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ProcessStreamTests {

    static AbstractStream.MessageRecord<String, Object> buildMockMessageRecord(String key, String tenantId) {
        return new AbstractStream.MessageRecord<String, Object>() {
            @Override
            public Map<String, Object> getMetadata() {
                return singletonMap(AbstractStream.KEY_TENANT, tenantId);
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

    static ChannelInfo buildMockChannelInfo(String channelId, String selfTenantId, List<String> policyTenantIds) {
        final List<ChannelInfo.RuleSpec> rules = safeList(policyTenantIds)
                .stream()
                .map(policyTid -> ChannelInfo.RuleSpec
                        .builder()
                        .tenantId(policyTid)
                        .recordFilter(null)
                        .build())
                .collect(toList());

        return ChannelInfo.builder()
                .id(channelId)
                .name(channelId)
                .tenantId(selfTenantId)
                .settingsSpec(ChannelInfo.SettingsSpec
                        .builder()
                        .policySpec(ChannelInfo.PolicySpec
                                .builder()
                                .rules(rules)
                                .build())
                        .checkpointSpec(null)
                        .sinkSpec(null)
                        .build())
                .build();
    }

    static StreamConnectProperties buildMockStreamConnectProperties() {
        return new StreamConnectProperties() {
            @Override
            public void afterPropertiesSet() {
            }

            @Override
            public IStreamConnectConfigurator.ConfiguratorProvider getConfigurator() {
                return new IStreamConnectConfigurator.ConfiguratorProvider() {
                    @Override
                    public String getType() {
                        throw new UnsupportedOperationException();
                    }

                    @Override
                    public IStreamConnectConfigurator obtain(Environment environment,
                                                             StreamConnectConfiguration config,
                                                             StreamConnectMeter meter) {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    static IStreamConnectConfigurator buildMockStreamConnectConfigurator() {
        return new IStreamConnectConfigurator() {
            @Override
            public List<? extends SourceStream.SourceStreamConfig> loadSourceConfigs(String connectorName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public List<ChannelInfo> loadChannels(String connectorName, IStreamConnectCoordinator.ShardingInfo sharding) {
                throw new UnsupportedOperationException();
            }
        };
    }

    //@Test
    public void testSourceAndProccessStreaming() {
        List<AbstractStream.MessageRecord<String, Object>> mockRecords = new ArrayList<>();
        mockRecords.add(buildMockMessageRecord("10001", "t1001"));
        mockRecords.add(buildMockMessageRecord("10002", "t1001"));
        mockRecords.add(buildMockMessageRecord("10003", "t1002"));
        mockRecords.add(buildMockMessageRecord("10004", "t1003"));

        List<ChannelInfo> mockAssignedChannels = new ArrayList<>();
        mockAssignedChannels.add(buildMockChannelInfo("c1001", "t1001", singletonList("t1001")));
        mockAssignedChannels.add(buildMockChannelInfo("c1002", "t1002", asList("t1001", "t1002")));
        mockAssignedChannels.add(buildMockChannelInfo("c1003", "t1003", singletonList("t1003")));

        IStreamConnectConfigurator mockConfigurator = buildMockStreamConnectConfigurator();
        StreamConnectProperties mockProperties = buildMockStreamConnectProperties();
        PrometheusMeterRegistry mockMeterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT);
        StreamConnectMeter mockMeter = new StreamConnectMeter(mockMeterRegistry, "testApp", 12345);

        Environment mockEnvironment = new StandardEnvironment();
        ApplicationEventPublisher mockEventPublisher = event -> {
            throw new UnsupportedOperationException();
        };
        StreamConnectConfiguration mockConfiguration = new StreamConnectConfiguration(mockEnvironment, mockProperties, mockEventPublisher, mockMeter);
        StreamConnectConfiguration.ConnectorConfig mockConnectorConfig = new StreamConnectConfiguration.ConnectorConfig();
        mockConnectorConfig.setName("connector_1");

        CachingChannelRegistry mockRegistry = new CachingChannelRegistry(mockConfiguration);
        StreamConnectEngineBootstrap mockBootstrap = new StreamConnectEngineBootstrap(mockEnvironment, mockConfiguration, mockRegistry);

        AbstractStream.StreamContext mockContext = new AbstractStream.StreamContext(mockEnvironment, mockConfiguration, mockConnectorConfig, mockRegistry, mockBootstrap);

        SourceStream mockSourceStream = new SourceStream(mockContext) {
            @Override
            public SourceStreamConfig getSourceStreamConfig() {
                throw new UnsupportedOperationException();
            }

            @Override
            protected Object getInternalTask() {
                throw new UnsupportedOperationException();
            }
        };

        //List<ChannelRecord> mockChannelRecords = new ProcessStream(mockContext, mockSourceStream)
        //        .doMatchToChannelRecords(mockConfigurator, mockAssignedChannels, mockRecords);

        List<ProcessStream.ChannelRecord> mockChannelRecords = ProcessStream.doMatchToChannelRecords(
                mockConfigurator, mockConnectorConfig, mockAssignedChannels, mockRecords);

        Assertions.assertEquals(6, mockChannelRecords.size());
    }

    @Test
    public void testProcessStreamDoMatchToChannelRecords() {
        List<AbstractStream.MessageRecord<String, Object>> mockRecords = new ArrayList<>();
        mockRecords.add(buildMockMessageRecord("10001", "t1001"));
        mockRecords.add(buildMockMessageRecord("10002", "t1001"));
        mockRecords.add(buildMockMessageRecord("10003", "t1002"));
        mockRecords.add(buildMockMessageRecord("10004", "t1003"));

        List<ChannelInfo> mockAssignedChannels = new ArrayList<>();
        mockAssignedChannels.add(buildMockChannelInfo("c1001", "t1001", singletonList("t1001")));
        mockAssignedChannels.add(buildMockChannelInfo("c1002", "t1002", asList("t1001", "t1002")));
        mockAssignedChannels.add(buildMockChannelInfo("c1003", "t1003", singletonList("t1003")));

        IStreamConnectConfigurator mockConfigurator = buildMockStreamConnectConfigurator();
        StreamConnectConfiguration.ConnectorConfig mockConnectorConfig = new StreamConnectConfiguration.ConnectorConfig();
        mockConnectorConfig.setName("connector_1");

        List<ProcessStream.ChannelRecord> mockChannelRecords = ProcessStream.doMatchToChannelRecords(
                mockConfigurator, mockConnectorConfig, mockAssignedChannels, mockRecords);

        Assertions.assertEquals(6, mockChannelRecords.size());
    }

}
