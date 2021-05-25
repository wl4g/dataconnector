/*
 * Copyright 2017 ~ 2025 the original author or authors. James Wong <wanglsir@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.meter;

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
 * {@link SubscribeMeter}
 *
 * @author James Wong
 * @since v1.0
 */
public class SubscribeMeter extends PrometheusMeterFacade {
    private static ApplicationContext context;
    private static SubscribeMeter DEFAULT;

    public static SubscribeMeter getDefault() {
        if (isNull(DEFAULT)) {
            synchronized (SubscribeMeter.class) {
                if (isNull(DEFAULT)) {
                    DEFAULT = context.getBean(SubscribeMeter.class);
                }
            }
        }
        return DEFAULT;
    }

    public SubscribeMeter(ApplicationContext context,
                          PrometheusMeterRegistry meterRegistry,
                          String appName,
                          int port) {
        super(meterRegistry, appName, false, new InetUtils(new InetUtilsProperties()), port);
        SubscribeMeter.context = Assert2.notNullOf(context, "context");
    }

    @Getter
    @AllArgsConstructor
    public enum MetricsName {

        shared_consumed("shared_consumed", "The stats of shared consumed count"),

        shared_consumed_time("shared_consumed_time", "The stats of shared consumed latency"),

        filter_records_success("filter_records_success", "The stats of filter records success"),

        filter_records_failure("filter_records_failure", "The stats of filter records failure"),

        filter_records_time("filter_time", "The stats of filter latency"),

        filter_records_sent_success("filter_records_sent", "The stats of filter records sent success"),

        filter_records_sent_failure("filter_records_sent", "The stats of filter records sent failure"),

        filter_records_sent_time("filter_time", "The stats of filter sent latency"),

        acknowledge_time("acknowledge_time", "The stats of commit acknowledge latency"),

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