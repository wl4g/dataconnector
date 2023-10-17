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
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.springframework.context.event.EventListener;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.lang.ref.WeakReference;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeToList;
import static com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import static java.util.Collections.emptyList;
import static java.util.Objects.nonNull;

/**
 * The {@link MeterEventHandler}
 *
 * @author James Wong
 * @since v1.0
 **/
@AllArgsConstructor
public class MeterEventHandler {

    private final StreamConnectMeter meter;

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
                        event.getPercentiles(),
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

    @EventListener(WeakReference.class)
    public void onWeakReferenceMeterEvent(WeakReference<Object> event) {
        final Object eventObj = event.get();
        if (nonNull(eventObj) && eventObj instanceof AbstractMeterEvent) {
            if (eventObj instanceof CountMeterEvent) {
                onCountMeterEvent((CountMeterEvent) eventObj);
            } else if (eventObj instanceof TimingMeterEvent) {
                onTimerMeterEvent((TimingMeterEvent) eventObj);
            } else if (eventObj instanceof GaugeMeterEvent) {
                onGaugeMeterEvent((GaugeMeterEvent) eventObj);
            }
        }
    }

    @Getter
    public static abstract class AbstractMeterEvent {
        private final @NotNull StreamConnectMeter.MetricsName metrics;
        private final @Null List<String> tags;

        public AbstractMeterEvent(@NotNull StreamConnectMeter.MetricsName metrics,
                                  @Null List<String> tags,
                                  @Null String... additionalTags) {
            this.metrics = Assert2.notNullOf(metrics, "metrics");
            // Merge tags and additionalTags.
            tags = new ArrayList<>(safeList(tags));
            tags.addAll(safeToList(String.class, additionalTags));
            this.tags = null;
        }
    }

    @Getter
    public static class CountMeterEvent extends AbstractMeterEvent {
        public CountMeterEvent(@NotNull MetricsName metrics,
                               @Null String... tags) {
            this(metrics, emptyList(), tags);
        }

        public CountMeterEvent(@NotNull MetricsName metrics,
                               @Null List<String> tags,
                               @Null String... additionalTags) {
            super(metrics, tags, additionalTags);
        }

        public static WeakReference<CountMeterEvent> of(@NotNull MetricsName metrics,
                                                        @Null List<String> tags,
                                                        @Null String... additionalTags) {
            return new WeakReference<>(new CountMeterEvent(metrics, tags, additionalTags));
        }
    }

    @Getter
    public static class TimingMeterEvent extends AbstractMeterEvent {
        private final double[] percentiles;
        private final Duration duration;

        public TimingMeterEvent(@NotNull MetricsName metrics,
                                @NotNull double[] percentiles,
                                @NotNull Duration duration,
                                @Null String... tags) {
            this(metrics, percentiles, duration, emptyList(), tags);
        }

        public TimingMeterEvent(@NotNull MetricsName metrics,
                                @NotNull double[] percentiles,
                                @NotNull Duration duration,
                                @Null List<String> tags,
                                @Null String... additionalTags) {
            super(metrics, tags, additionalTags);
            this.percentiles = percentiles;
            this.duration = duration;
        }

        public static WeakReference<TimingMeterEvent> of(@NotNull MetricsName metrics,
                                                         @NotNull double[] percentiles,
                                                         @NotNull Duration duration,
                                                         @Null List<String> tags,
                                                         @Null String... additionalTags) {
            return new WeakReference<>(new TimingMeterEvent(metrics, percentiles, duration, tags, additionalTags));
        }
    }

    @Getter
    public static class GaugeMeterEvent extends AbstractMeterEvent {
        private final double value;

        public GaugeMeterEvent(@NotNull MetricsName metrics,
                               @Null List<String> tags,
                               double value,
                               @Null String... additionalTags) {
            super(metrics, tags, additionalTags);
            this.value = value;
        }

        public static WeakReference<GaugeMeterEvent> of(@NotNull MetricsName metrics,
                                                        @Null List<String> tags,
                                                        double value,
                                                        @Null String... additionalTags) {
            return new WeakReference<>(new GaugeMeterEvent(metrics, tags, value, additionalTags));
        }
    }

}
