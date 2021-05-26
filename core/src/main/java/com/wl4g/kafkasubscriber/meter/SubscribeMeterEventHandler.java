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

package com.wl4g.kafkasubscriber.meter;

import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.context.event.EventListener;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.time.Duration;
import java.util.List;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeToList;

/**
 * The {@link SubscribeMeterEventHandler}
 *
 * @author James Wong
 * @since v1.0
 **/
@AllArgsConstructor
public class SubscribeMeterEventHandler {

    private final SubscribeMeter meter;

    @EventListener(CountMeterEvent.class)
    public void onCountMeterEvent(CountMeterEvent event) {
        meter.counter(event.getMetrics().getName(),
                event.getMetrics().getHelp(),
                safeList(event.getTags()).toArray(new String[0])).increment();
    }

    @EventListener(TimingMeterEvent.class)
    public void onTimerMeterEvent(TimingMeterEvent event) {
        meter.timer(event.getMetrics().getName(),
                        event.getMetrics().getHelp(),
                        SubscribeMeter.DEFAULT_PERCENTILES,
                        safeList(event.getTags()).toArray(new String[0]))
                .record(event.getDuration());
    }

    @EventListener(GaugeMeterEvent.class)
    public void onGaugeMeterEvent(GaugeMeterEvent event) {
        meter.gauge(event.getMetrics().getName(),
                event.getMetrics().getHelp(),
                event.getValue(),
                safeList(event.getTags()).toArray(new String[0]));
    }

    @Getter
    @AllArgsConstructor
    public static abstract class AbstractMeterEvent {
        private @NotNull SubscribeMeter.MetricsName metrics;
        private @NotBlank String topic;
        private @Null Integer partition;
        private @NotBlank String groupId;
        private @Null String subscriberId;
        private @Null List<String> tags;
    }

    @Getter
    public static class CountMeterEvent extends AbstractMeterEvent {
        public CountMeterEvent(@NotNull SubscribeMeter.MetricsName metrics,
                               @NotBlank String topic,
                               @Null Integer partition,
                               @NotBlank String groupId,
                               @Null String subscriberId,
                               @Null List<String> tags) {
            super(metrics, topic, partition, groupId, subscriberId, tags);
        }
    }

    @Getter
    public static class TimingMeterEvent extends AbstractMeterEvent {
        private final double[] percentiles;
        private final Duration duration;


        public TimingMeterEvent(@NotNull SubscribeMeter.MetricsName metrics,
                                @NotBlank String topic,
                                @Null Integer partition,
                                @NotBlank String groupId,
                                @Null String subscriberId,
                                @NotNull Duration duration,
                                @Null String... tags) {
            this(metrics, topic, partition, groupId, subscriberId,
                    SubscribeMeter.DEFAULT_PERCENTILES, duration, tags);
        }

        public TimingMeterEvent(@NotNull SubscribeMeter.MetricsName metrics,
                                @NotBlank String topic,
                                @Null Integer partition,
                                @NotBlank String groupId,
                                @Null String subscriberId,
                                @NotNull double[] percentiles,
                                @NotNull Duration duration,
                                @Null String... tags) {
            super(metrics, topic, partition, groupId, subscriberId, safeToList(String.class, tags));
            this.percentiles = percentiles;
            this.duration = duration;
        }
    }

    @Getter
    public static class GaugeMeterEvent extends AbstractMeterEvent {
        private final double value;

        public GaugeMeterEvent(@NotNull SubscribeMeter.MetricsName metrics,
                               @NotBlank String topic,
                               @Null Integer partition,
                               @NotBlank String groupId,
                               @Null String subscriberId,
                               @Null List<String> tags,
                               double value) {
            super(metrics, topic, partition, groupId, subscriberId, tags);
            this.value = value;
        }
    }

}
