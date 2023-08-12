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

package com.wl4g.streamconnect.stream.sink;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.ICheckpoint;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.framework.NamedStreamConnectSpi;
import com.wl4g.streamconnect.meter.MeterEventHandler.CountMeterEvent;
import com.wl4g.streamconnect.meter.MeterEventHandler.TimingMeterEvent;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsTag;
import com.wl4g.streamconnect.stream.AbstractStream;
import com.wl4g.streamconnect.stream.StreamConnectEngineBootstrap.StreamBootstrap;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.HashSet;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.streamconnect.meter.StreamConnectMeter.DEFAULT_PERCENTILES;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toCollection;

/**
 * The {@link SinkStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class SinkStream extends AbstractStream {
    private final ChannelInfo channel;
    private final ICheckpoint.PointReader pointReader;

    public SinkStream(@NotNull final StreamContext context,
                      @NotNull final ChannelInfo channel) {
        super(context);
        this.channel = requireNonNull(channel, "channel must not be null");
        this.pointReader = getConnectorConfig().getCheckpoint()
                .createReader(getConnectorConfig(), channel, new AcknowledgeSinkListener());
    }

    protected abstract Object getInternalTask();

    public abstract SinkStreamConfig getSinkStreamConfig();

    @Override
    public String getDescription() {
        return String.format("%s-%s", super.getDescription(),
                getChannel().getId());
    }

    @Override
    public void close() {
        this.pointReader.close();
    }

    protected abstract SinkResult doSink(MessageRecord<String, Object> processedRecord,
                                         int retryTimes);

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static abstract class SinkStreamConfig extends BaseStreamConfig {
        private @NotBlank String qos; // Only valid for sink write to remote.

        @Override
        public void validate() {
            super.validate();
            Assert2.hasTextOf(qos, "qos");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @AllArgsConstructor
    @ToString(callSuper = true)
    public static class SinkResult {
        private MessageRecord<String, Object> record;
        private Future<?> future;
        private int retryTimes;
    }

    class AcknowledgeSinkListener extends ICheckpoint.ReadPointListener {
        @Override
        public void onMessage(List<? extends MessageRecord<String, Object>> records,
                              Runnable ack) {
            if (log.isDebugEnabled()) {
                log.debug("Read to checkpoint records : {}, channel : {}", records, channel);
            }
            final Queue<SinkResult> sinkResults = safeList(records)
                    .stream()
                    .map(record -> doSink(record, 1))
                    .collect(toCollection(ConcurrentLinkedQueue::new));

            if (getConnectorConfig().getQos().supportRetry(getConnectorConfig())) {
                final Set<SinkResult> sentResults = new HashSet<>(sinkResults.size());
                while (!sinkResults.isEmpty()) {
                    final SinkResult sr = sinkResults.poll();
                    // Notice: is done is not necessarily successful, both exception and
                    // cancellation will be done.
                    if (sr.getFuture().isDone()) {
                        Object rm;
                        try {
                            rm = sr.getFuture().get();
                            sentResults.add(sr);
                            if (log.isDebugEnabled()) {
                                log.debug("{} :: {} :: Sink record metadata : {}", getConnectorConfig().getName(),
                                        channel.getId(), rm);
                            }
                            getEventPublisher().publishEvent(new CountMeterEvent(
                                    MetricsName.sink_records_success,
                                    getBasedMeterTags()));
                        } catch (InterruptedException | CancellationException | ExecutionException ex) {
                            log.error("{} :: {} :: Unable not to getting sink result.",
                                    getConnectorConfig().getName(), channel.getId(), ex);

                            getEventPublisher().publishEvent(new CountMeterEvent(
                                    MetricsName.sink_records_failure,
                                    getBasedMeterTags()));

                            if (ex instanceof ExecutionException) {
                                getConnectorConfig().getQos().retryIfFail(getConnectorConfig(),
                                        sr.getRetryTimes(), () -> {
                                            if (log.isDebugEnabled()) {
                                                log.debug("{} :: Retrying to sink : {}", getConnectorConfig().getName(), sr);
                                            }
                                            sinkResults.offer(doSink(sr.getRecord(), sr.getRetryTimes() + 1));
                                        });
                            }
                        }
                    }
                    Thread.yield(); // May give up the CPU
                }

                // e.g: According to the records of each partition, only submit the part of
                // this batch that has been successively successful from the earliest.
                if (getConnectorConfig().getQos().supportPreferAcknowledge(getConnectorConfig())) {
                    getPointReader().preferAutoAcknowledge(sentResults);
                } else {
                    // After the maximum retries, there may still be records of processing failures.
                    // At this time, the ack commit is forced and the failures are ignored.
                    final long acknowledgeTimingBegin = System.nanoTime();
                    try {
                        if (log.isDebugEnabled()) {
                            log.debug("{} :: {} :: Batch sent acknowledging ...",
                                    getConnectorConfig().getName(),
                                    channel.getId());
                        }
                        ack.run();
                        if (log.isInfoEnabled()) {
                            log.info("{} :: {} :: Sent acknowledged.", getConnectorConfig().getName(),
                                    channel.getId());
                        }
                        getPointReader().addAcknowledgeCountMeter(MetricsName.acknowledge_success,
                                sentResults);
                    } catch (Throwable ex) {
                        log.error(String.format("%s :: Failed to sent success acknowledge.",
                                channel.getId()), ex);
                        getPointReader().addAcknowledgeCountMeter(MetricsName.acknowledge_failure,
                                sentResults);
                    } finally {
                        getEventPublisher().publishEvent(new TimingMeterEvent(
                                MetricsName.acknowledge_time,
                                DEFAULT_PERCENTILES,
                                Duration.ofNanos(System.nanoTime() - acknowledgeTimingBegin),
                                getBasedMeterTags(),
                                MetricsTag.ACK_KIND,
                                MetricsTag.ACK_KIND_VALUE_COMMIT));
                    }
                }
            }
            // If retry is not supported, it means that the SLA requirements
            // are low and data losses are allowed and commit it directly.
            else {
                final long acknowledgeTimingBegin = System.nanoTime();
                try {
                    if (log.isDebugEnabled()) {
                        log.debug("{} :: {} :: Batch regardless of success or failure force acknowledging ...",
                                getConnectorConfig().getName(), channel.getId());
                    }
                    ack.run();
                    if (log.isInfoEnabled()) {
                        log.info("{} :: {} :: Force sent to acknowledged.", getConnectorConfig().getName(),
                                channel.getId());
                    }
                    getPointReader().addAcknowledgeCountMeter(MetricsName.acknowledge_success,
                            sinkResults);
                } catch (Throwable ex) {
                    log.error(String.format("%s :: %s :: Failed to sent force acknowledge for %s",
                            getConnectorConfig().getName(), channel.getId(), ack), ex);
                    getPointReader().addAcknowledgeCountMeter(MetricsName.acknowledge_failure,
                            sinkResults);
                } finally {
                    getEventPublisher().publishEvent(new TimingMeterEvent(
                            MetricsName.acknowledge_time,
                            DEFAULT_PERCENTILES,
                            Duration.ofNanos(System.nanoTime() - acknowledgeTimingBegin),
                            getBasedMeterTags(),
                            MetricsTag.ACK_KIND,
                            MetricsTag.ACK_KIND_VALUE_COMMIT));
                }
            }
        }
    }

    public static abstract class SinkStreamProvider extends NamedStreamConnectSpi {
        public abstract StreamBootstrap<? extends SinkStream> create(
                @NotNull final StreamContext context,
                @NotNull final SinkStreamConfig sinkStreamConfig,
                @NotNull final ChannelInfo channel);
    }

}
