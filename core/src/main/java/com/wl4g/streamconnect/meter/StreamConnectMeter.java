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

package com.wl4g.streamconnect.meter;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.infra.common.metrics.PrometheusMeterFacade;
import com.wl4g.infra.common.net.InetUtils;
import com.wl4g.infra.common.net.InetUtils.InetUtilsProperties;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.context.ApplicationContext;

import static java.util.Objects.isNull;

/**
 * {@link StreamConnectMeter}
 *
 * @author James Wong
 * @since v1.0
 */
public class StreamConnectMeter extends PrometheusMeterFacade {
    private static ApplicationContext context;
    private static StreamConnectMeter DEFAULT;

    public static StreamConnectMeter getDefault() {
        if (isNull(DEFAULT)) {
            synchronized (StreamConnectMeter.class) {
                if (isNull(DEFAULT)) {
                    DEFAULT = context.getBean(StreamConnectMeter.class);
                }
            }
        }
        return DEFAULT;
    }

    public StreamConnectMeter(ApplicationContext context,
                              PrometheusMeterRegistry meterRegistry,
                              String appName,
                              int port) {
        super(meterRegistry, appName, false, new InetUtils(new InetUtilsProperties()), port);
        StreamConnectMeter.context = Assert2.notNullOf(context, "context");
    }

    @Getter
    @AllArgsConstructor
    public enum MetricsName {

        shared_consumed("shared_consumed", "The stats of shared consumed count"),

        shared_consumed_time("shared_consumed_time", "The stats of shared consumed latency"),

        filter_records_success("filter_records_success", "The stats of filtering records success"),

        filter_records_failure("filter_records_failure", "The stats of filtering records failure"),

        filter_records_time("filter_time", "The stats of filter latency"),

        checkpoint_sent_success("checkpoint_sent", "The stats of filtered sent to checkpoint topic success"),

        checkpoint_sent_failure("checkpoint_sent", "The stats of filtered sent to checkpoint topic failure"),

        checkpoint_sent_time("checkpoint_time", "The stats of filtered sent to checkpoint topic latency"),

        acknowledge_success("acknowledge_success", "The stats of acknowledge offset success"),

        acknowledge_failure("acknowledge_failure", "The stats of acknowledge offset failure"),

        acknowledge_time("acknowledge_time", "The stats of acknowledge offset latency"),

        sink_records_success("sink_records_success", "The stats of sink records success"),

        sink_records_failure("sink_records_failure", "The stats of sink records failure"),

        sink_time("sink_time", "The stats of sink latency");

        private final String name;
        private final String help;
    }

    public static abstract class MetricsTag {
        public static final String TOPIC = "topic";
        public static final String PARTITION = "partition";
        public static final String GROUP_ID = "groupId";
        public static final String SUBSCRIBE = "subscribe";
        public static final String ACK_KIND = "ackKind";
        public static final String ACK_KIND_VALUE_COMMIT = "commit";
        public static final String ACK_KIND_VALUE_SEND = "send";
    }

    public static final double[] DEFAULT_PERCENTILES = new double[]{0.5, 0.9, 0.95};

}