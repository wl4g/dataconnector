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

package com.wl4g.streamconnect.config.configurator;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.ChannelInfo.RuleSpec;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.framework.IStreamConnectSpi;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.AbstractStream;
import com.wl4g.streamconnect.stream.source.SourceStream.SourceStreamConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.springframework.core.env.Environment;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.List;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.streamconnect.stream.AbstractStream.KEY_TENANT;
import static com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The {@link IStreamConnectConfigurator}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface IStreamConnectConfigurator {

    List<? extends SourceStreamConfig> loadSourceConfigs(@NotBlank String connectorName);

    List<ChannelInfo> loadChannels(@NotBlank String connectorName,
                                   @Null IStreamConnectCoordinator.ShardingInfo sharding);

    default boolean matchChannelRecord(@NotBlank String connectorName,
                                       @NotNull ChannelInfo channel,
                                       @NotNull MessageRecord<String, Object> record) {
        Assert2.hasTextOf(connectorName, "connectorName");
        requireNonNull(channel, "channel must not be null");
        requireNonNull(record, "record must not be null");

        final String tenantId = getTenantIdForRecord(channel, record);
        if (isNotBlank(tenantId)) {
            return safeList(channel.getSettingsSpec().getPolicySpec().getRules())
                    .stream()
                    .map(RuleSpec::getTenantId)
                    .anyMatch(tid -> StringUtils.equals(tenantId, tid));
        }
        return false;
    }

    // ----- Configurator provider. -----

    @Getter
    @Setter
    @NoArgsConstructor
    abstract class ConfiguratorProvider implements IStreamConnectSpi {
        public abstract IStreamConnectConfigurator obtain(Environment environment,
                                                          StreamConnectConfiguration config,
                                                          StreamConnectMeter meter);
    }

    static String getTenantIdForRecord(@NotNull ChannelInfo channel,
                                       @NotNull MessageRecord<String, Object> record) {
        requireNonNull(channel, "channel must not be null");
        requireNonNull(record, "record must not be null");

        // Notice: By default, the $$tenant field of the source message match, which should be customized
        // to match the channel relationship corresponding to each record in the source consume topic.
        if (nonNull(record.getMetadata())) {
            // Priority match to record header.
            final String tenantId = (String) record.getMetadata().get(AbstractStream.KEY_TENANT);
            if (isNotBlank(tenantId)) {
                return tenantId;
            }
        }
        if (nonNull(record.getValue())) {
            // Fallback match to record value.
            if (record.getValue() instanceof ObjectNode) {
                final JsonNode tNode = ((ObjectNode) record.getValue()).get(KEY_TENANT);
                final String tenantId = nonNull(tNode) ? tNode.textValue() : null;
                if (isNotBlank(tenantId)) {
                    return tenantId;
                }
            }
        }
        return null;
    }

}
