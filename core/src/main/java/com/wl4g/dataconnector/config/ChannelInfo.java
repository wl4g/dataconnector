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

package com.wl4g.dataconnector.config;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.stream.sink.SinkStream;
import lombok.*;
import lombok.Builder.Default;
import lombok.experimental.SuperBuilder;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.List;

import static com.wl4g.infra.common.lang.Assert2.hasTextOf;
import static com.wl4g.infra.common.lang.Assert2.notEmptyOf;
import static java.util.Objects.requireNonNull;

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
 *  *  |             DispatchStream (executors filters+mappers)            |
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
    private @Nullable  List<String> labels;
    private @NotNull
    @Builder.Default SettingsSpec settingsSpec = new SettingsSpec();

    public ChannelInfo validate() {
        Assert2.notNullOf(id, "id");
        hasTextOf(name, "name");
        Assert2.notNullOf(enable, "enable");
        Assert2.notNullOf(settingsSpec, "settingsSpec");
        this.settingsSpec.validate();
        return this;
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class SettingsSpec {
        private @Default PolicySpec policySpec = new PolicySpec();
        private @NotNull CheckpointSpec checkpointSpec;
        private @NotNull SinkStream.SinkStreamConfig sinkSpec;

        public void validate() {
            requireNonNull(policySpec, "policySpec must not be null");
            requireNonNull(checkpointSpec, "checkpointSpec must not be null");
            requireNonNull(sinkSpec, "sinkSpec must not be null");
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
            notEmptyOf(rules, "rules");
            rules.forEach(RuleSpec::validate);
            // Check if there is a duplicate rule name.
            Assert2.isTrueOf(rules.stream().map(RuleSpec::getName).distinct().count()
                    == rules.size(), "Duplicate rule names");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class RuleSpec {
        private @NotBlank String name;
        private @NotNull List<RuleItem> chain;

        public void validate() {
            hasTextOf(name, "name");
            requireNonNull(chain, "chain");
            chain.forEach(RuleItem::validate);
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class RuleItem {
        private @NotBlank String name;
        private @Nullable  String value;

        public void validate() {
            hasTextOf(name, "name");
            //hasTextOf(value, "value");
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
            hasTextOf(servers, "checkpointServers");
            Assert2.isTrueOf(retentionTime > 0, "retentionTime > 0");
            Assert2.isTrueOf(retentionBytes > 0, "retentionBytes > 0");
        }
    }

}
