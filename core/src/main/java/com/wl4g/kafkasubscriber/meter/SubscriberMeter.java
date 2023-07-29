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
 * {@link SubscriberMeter}
 *
 * @author James Wong
 * @since v1.0
 */
public class SubscriberMeter extends PrometheusMeterFacade {
    private static ApplicationContext context;
    private static SubscriberMeter DEFAULT;

    public static SubscriberMeter getDefault() {
        if (isNull(DEFAULT)) {
            synchronized (SubscriberMeter.class) {
                if (isNull(DEFAULT)) {
                    DEFAULT = context.getBean(SubscriberMeter.class);
                }
            }
        }
        return DEFAULT;
    }

    public SubscriberMeter(ApplicationContext context,
                           PrometheusMeterRegistry meterRegistry,
                           String appName,
                           int port) {
        super(meterRegistry, appName, false, new InetUtils(new InetUtilsProperties()), port);
        SubscriberMeter.context = Assert2.notNullOf(context, "context");
    }

    @Getter
    @AllArgsConstructor
    public static enum MetricsName {

        shared_consumed("shared_consumed", "The stats of shared consumed count"),

        shared_consumed_time("shared_consumed_time", "The stats of shared consumed latency"),

        filtered_records("filtered_records", "The stats of filtered records"),

        filtered_time("filtered_time", "The stats of filtered consumed latency"),

        sink_records_success("sink_records_success", "The stats of sink records success"),

        sink_records_failure("sink_records_failure", "The stats of sink records failure"),

        sink_time("sink_time", "The stats of sdk client sink latency");

        private final String name;
        private final String help;
    }

    public static abstract class MetricsTag {
        public static final String TOPIC = "topic";
        public static final String GROUP_ID = "groupId";
        public static final String SUBSCRIBE = "subscribe";
    }

    public static final double[] DEFAULT_PERCENTILES = new double[]{0.5, 0.9, 0.95};

}