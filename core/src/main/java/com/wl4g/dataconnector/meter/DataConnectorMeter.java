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

package com.wl4g.dataconnector.meter;

import com.wl4g.infra.common.metrics.PrometheusMeterFacade;
import com.wl4g.infra.common.net.InetUtils;
import com.wl4g.infra.common.net.InetUtils.InetUtilsProperties;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * {@link DataConnectorMeter}
 *
 * @author James Wong
 * @since v1.0
 */
public class DataConnectorMeter extends PrometheusMeterFacade {

    public DataConnectorMeter(PrometheusMeterRegistry meterRegistry,
                              String appName,
                              int port) {
        super(meterRegistry, appName, false, new InetUtils(new InetUtilsProperties()), port);
    }

    @Getter
    @AllArgsConstructor
    public enum MetricsName {

        source_records("source_records", "The stats of source records total count"),

        source_records_time("source_records_time", "The stats of source records latency"),

        process_records_success("process_records_success", "The stats of processed records success"),

        process_records_failure("process_records_failure", "The stats of processed records failure"),

        process_records_time("process_time", "The stats of processed time latency"),

        checkpoint_write_success("checkpoint_write_success", "The stats of written to checkpoint store success"),

        checkpoint_write_failure("checkpoint_write_failure", "The stats of written to checkpoint store failure"),

        checkpoint_write_time("checkpoint_write_time", "The stats of written to checkpoint store time latency"),

        checkpoint_read_success("checkpoint_read_success", "The stats of read from checkpoint store success total count"),

        checkpoint_read_failure("checkpoint_read_failure", "The stats of read from checkpoint store failure total count"),

        checkpoint_read_time("checkpoint_read_time", "The stats of read from checkpoint store time latency"),

        acknowledge_success("acknowledge_success", "The stats of acknowledge process success"),

        acknowledge_failure("acknowledge_failure", "The stats of acknowledge process failure"),

        acknowledge_time("acknowledge_time", "The stats of acknowledge offset time latency"),

        sink_records_success("sink_records_success", "The stats of sink records success"),

        sink_records_failure("sink_records_failure", "The stats of sink records failure"),

        sink_time("sink_time", "The stats of sink time latency"),

        connector_total("connector_total", "The stats of connectors total count"),

        coordinator_sharding_channels_total("coordinator_sharding_channels_total", "The stats of sharding channels total count"),

        coordinator_eventbus_total("coordinator_eventbus_total", "The stats of coordinator config event changed total count"),

        coordinator_discovery_total("coordinator_discovery_total", "The stats of coordinator discovery changed total count");

        private final String name;
        private final String help;
    }

    public static abstract class MetricsTag {
        public static final String CONNECTOR = "connector";
        public static final String TOPIC = "topic";
        public static final String GROUP_ID = "groupId";
        public static final String QOS = "qos";
        public static final String CHECKPOINT = "checkpoint";
        public static final String CHANNEL = "channel";
        public static final String ACK_KIND = "ackKind";
        public static final String ACK_KIND_VALUE_COMMIT = "commit";
        public static final String ACK_KIND_VALUE_SEND = "send";
    }

    public static final double[] DEFAULT_PERCENTILES = new double[] {0.5, 0.9, 0.99};

}