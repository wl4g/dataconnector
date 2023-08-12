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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.stream.sink.SinkStream.SinkStreamConfig;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.List;
import java.util.Objects;

/**
 * The channel information processed by the data flow connection,
 * which can be a logical channel, similar to the channel information.
 * <p>
 * The overall architecture diagram is as follows:
 *
 * <pre>
 *  *  --------------------------------------------------------------------
 *  *  |           Upstream Servers (kafka/postgresql/emq/redis/...)      |
 *  *  --------------------------------------------------------------------
 *  *                             ↓
 *  *                     consuming/query (all channels)
 *  *                             ↓
 *  *  --------------------------------------------------------------------
 *  *  |                SourceStream (kafka/jdbc/mqtt/redis/...)          |
 *  *  --------------------------------------------------------------------
 *  *                             ↓
 *  *                     invoking (all channels)
 *  *                             ↓
 *  *  --------------------------------------------------------------------
 *  *  |             ProcessStream (executors filters+mappers)            |
 *  *  --------------------------------------------------------------------
 *  *                             ↓↓
 *  *                     write to DLQ (checkpoint) (one per channel)
 *  *                             ↓↓
 *  *  --------------------------------------------------------------------
 *  *  |            DLQ (checkpoint) Store (kafka/minio/...)              |
 *  *  --------------------------------------------------------------------
 *  *                             ↓↓
 *  *                     read from DLQ (one per channel)
 *  *                             ↓↓
 *  *  --------------------------------------------------------------------
 *  *  |      SinkStreams (kafka/rocketmq/rabbitmq/websocket/mqtt/...)    |
 *  *  --------------------------------------------------------------------
 *  *                             ↓↓
 *  *                     producing/send (one per channel)
 *  *                             ↓↓
 *  *  --------------------------------------------------------------------
 *  *  |  Downstream Servers (kafka/rocketmq/rabbitmq/httpserver/emq/...) |
 *  *  --------------------------------------------------------------------
 *  * <pre>
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@SuperBuilder
@ToString(callSuper = true)
@NoArgsConstructor
public class ChannelInfo {
    private @NotBlank String id;
    private @NotBlank String name;
    private @Default boolean enable = true;
    private @NotBlank String tenantId;
    private @NotNull
    @Builder.Default SettingsSpec settingsSpec = new SettingsSpec();

    public ChannelInfo validate() {
        Assert2.notNullOf(id, "id");
        Assert2.hasTextOf(name, "name");
        Assert2.notNullOf(enable, "enable");
        Assert2.hasTextOf(tenantId, "tenantId");
        Assert2.notNullOf(settingsSpec, "settings");
        this.settingsSpec.validate();
        return this;
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class SettingsSpec {
        private @Builder.Default PolicySpec policySpec = new PolicySpec();
        private @NotNull CheckpointSpec checkpointSpec;
        private @NotNull SinkStreamConfig sinkSpec;

        public void validate() {
            Objects.requireNonNull(policySpec, "policySpec must not be null");
            Objects.requireNonNull(checkpointSpec, "checkpointSpec must not be null");
            Objects.requireNonNull(sinkSpec, "sinkSpec must not be null");
            this.policySpec.validate();
            this.checkpointSpec.validate();
            this.sinkSpec.validate();
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class PolicySpec {
        private @Default boolean sequence = false;
        private @NotEmpty List<RuleSpec> rules;

        public void validate() {
            Assert2.notEmptyOf(rules, "rules");
            rules.forEach(RuleSpec::validate);
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class RuleSpec {
        private @NotBlank String tenantId; // TODO modify to support cross tenant subscribe grants rule expr filter
        private @NotBlank String recordFilter;
        private @Null String fieldFilter;

        public void validate() {
            Assert2.hasTextOf(tenantId, "tenantId");
            Assert2.hasTextOf(recordFilter, "recordFilter");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class CheckpointSpec {
        private @NotBlank String servers;
        private @Default long retentionTime = 60 * 60 * 1000L; // Dead-letter-queue-expire-time
        private @Default long retentionBytes = 1024 * 1024 * 1024L; // Dead-letter-queue-capacity

        public void validate() {
            Assert2.hasTextOf(servers, "checkpointServers");
            Assert2.isTrueOf(retentionTime > 0, "retentionTime > 0");
            Assert2.isTrueOf(retentionBytes > 0, "retentionBytes > 0");
        }
    }

}
