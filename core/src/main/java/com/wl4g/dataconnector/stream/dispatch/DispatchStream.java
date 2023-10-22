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

package com.wl4g.dataconnector.stream.dispatch;

import com.wl4g.dataconnector.checkpoint.ICheckpoint;
import com.wl4g.dataconnector.checkpoint.ICheckpoint.PointWriter;
import com.wl4g.dataconnector.checkpoint.ICheckpoint.WritePointResult;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.coordinator.CachingChannelRegistry.ChannelInfoWrapper;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.exception.GiveUpRetryProcessException;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.meter.DataConnectorMeter.MetricsName;
import com.wl4g.dataconnector.meter.MeterEventHandler.CountMeterEvent;
import com.wl4g.dataconnector.meter.MeterEventHandler.TimingMeterEvent;
import com.wl4g.dataconnector.stream.AbstractStream;
import com.wl4g.dataconnector.stream.dispatch.ComplexProcessChain.ComplexProcessResult;
import com.wl4g.dataconnector.stream.source.SourceStream;
import com.wl4g.dataconnector.util.assign.Assignments;
import com.wl4g.dataconnector.util.concurrent.BlockAbortPolicy;
import com.wl4g.dataconnector.util.concurrent.NamedThreadFactory;
import com.wl4g.infra.common.lang.Assert2;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.lang.System.nanoTime;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static org.apache.commons.lang3.exception.ExceptionUtils.getRootCause;

/**
 * The {@link DispatchStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class DispatchStream extends AbstractStream {
    private final SourceStream sourceStream;
    private final ThreadPoolExecutor sharedNonSequenceExecutor;
    private final List<ThreadPoolExecutor> isolationSequenceExecutors;
    private final Map<String, ICheckpoint.PointWriter> channelPointWriters = new ConcurrentHashMap<>();

    public DispatchStream(@NotNull final StreamContext context,
                          @NotNull final SourceStream sourceStream) {
        super(context);
        this.sourceStream = requireNonNull(sourceStream, "sourceStream is null");

        // Create the shared filter/mapper single executor.
        final DispatchStreamConfig dispatchConfig = getConnectorConfig().getDispatchConfig();
        this.sharedNonSequenceExecutor = new ThreadPoolExecutor(dispatchConfig.getSharedExecutorThreadPoolSize(),
                dispatchConfig.getSharedExecutorThreadPoolSize(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(dispatchConfig.getSharedExecutorQueueSize()),
                new NamedThreadFactory("processStream-sharedNonSeq-"),
                new BlockAbortPolicy()); // TODO custom reject handler
        if (dispatchConfig.isExecutorWarmUp()) {
            this.sharedNonSequenceExecutor.prestartAllCoreThreads();
        }

        // Create the sequence filter/mapper executors.
        this.isolationSequenceExecutors = synchronizedList(new ArrayList<>(dispatchConfig
                .getSequenceExecutorsMaxCountLimit()));
        for (int i = 0; i < dispatchConfig.getSequenceExecutorsMaxCountLimit(); i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(dispatchConfig.getSequenceExecutorsPerQueueSize()),
                    new NamedThreadFactory("processStream-isolationSeq-"),
                    new BlockAbortPolicy()); // TODO custom reject handler
            if (dispatchConfig.isExecutorWarmUp()) {
                executor.prestartAllCoreThreads();
            }
            this.isolationSequenceExecutors.add(executor);
        }
    }

    @Override
    public String getDescription() {
        return String.format("%s(%s)", super.getDescription(),
                sourceStream.getDescription());
    }

    @Override
    public List<String> getBasedMeterTags() {
        return sourceStream.getBasedMeterTags();
    }

    @Override
    public void close() {
        log.info("Checkpoint stopping ...");
        if (!sharedNonSequenceExecutor.isShutdown()) {
            try {
                log.info("{} :: Closing shared non filter executor...", getConnectorConfig().getName());
                this.sharedNonSequenceExecutor.shutdown();
                log.info("{} :: Closed shared non filter executor.", getConnectorConfig().getName());
            } catch (Exception ex) {
                log.error(String.format("%s :: Failed to close shared filter executor.", getConnectorConfig().getName()), ex);
            }
        }
        this.isolationSequenceExecutors.forEach(executor -> {
            if (!executor.isShutdown()) {
                try {
                    log.info("{} :: Closing filter executor {}...", getConnectorConfig().getName(), executor);
                    executor.shutdown();
                    log.info("{} :: Closed filter executor {}.", getConnectorConfig().getName(), executor);
                } catch (Exception ex) {
                    log.error(String.format("%s :: Failed to close filter executor %s.", getConnectorConfig().getName(), executor), ex);
                }
            }
        });
    }

    public Queue<WritePointResult> dispatch(List<? extends MessageRecord<String, Object>> records) {
        // Execute custom filters in parallel them to different send executor queues.
        final Collection<ChannelInfoWrapper> assignedChannels = getRegistry()
                .getAssignedChannels(getConnectorConfig().getName());

        final Queue<PendingResult> pendingResults = safeList(assignedChannels)
                .stream()
                .flatMap(c -> safeList(records)
                        .stream()
                        .map(r -> {
                            final ThreadPoolExecutor executor = determineDispatchExecutor(c, r);
                            final Future<PendingMetadata> f;
                            try {
                                f = executor.submit(buildDispatchTask(c, r));
                            } catch (RejectedExecutionException ex) {
                                // TODO custom reject handling ???
                                throw new DataConnectorException(ex);
                            }
                            return new PendingResult(c, r, f, 1);
                        }))
                .collect(toCollection(ConcurrentLinkedQueue::new));

        return waitForWritten(pendingResults);
    }

    private Queue<WritePointResult> waitForWritten(Queue<PendingResult> pendingResults) {
        // Add timing process metrics.
        // The benefit of not using lamda records is better use of arthas for troubleshooting during operation.
        final long begin = nanoTime();

        // Wait for all parallel processed results to be completed.
        final Queue<WritePointResult> writeResults = new ConcurrentLinkedQueue<>();
        while (!pendingResults.isEmpty()) {
            final PendingResult pr = pendingResults.poll();
            // Notice: is done is not necessarily successful, both exception and cancellation will be done.
            if (pr.getFuture().isDone()) {
                PendingMetadata pm = null;
                try {
                    pm = pr.getFuture().get();

                    getEventPublisher().publishEvent(CountMeterEvent.of(
                            MetricsName.process_records_success,
                            getBasedMeterTags()));
                } catch (InterruptedException | CancellationException | ExecutionException ex) {
                    log.error("{} :: Unable to get process result.", getConnectorConfig().getName(), ex);

                    getEventPublisher().publishEvent(CountMeterEvent.of(
                            MetricsName.process_records_failure,
                            getBasedMeterTags()));

                    if (ex instanceof ExecutionException) {
                        final Throwable reason = getRootCause(ex);
                        if (reason instanceof GiveUpRetryProcessException) { // User need giveUp retry
                            log.warn("{} :: User ask to give up re-trying again process. fr : {}, reason : {}",
                                    getConnectorConfig().getName(), pr, reason.getMessage());
                        } else {
                            getConnectorConfig().getQos().retryIfFail(getConnectorConfig(),
                                    pr.getRetryTimes(), () -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug("{} :: Retry to process. pr : {}", getConnectorConfig().getName(), pr);
                                        }
                                        enqueueDispatchExecutor(pr, pendingResults);
                                    });
                        }
                    }
                }
                if (nonNull(pm) && pm.isMatched()) {
                    // Send to processed topic and add sent future If necessary.
                    // Replace to mapped record(eg: data permission processing).
                    @SuppressWarnings("all") final PointWriter pointWriter = obtainChannelPointWriter(pr.getChannel());
                    writeResults.offer(pointWriter.writeAsync(getConnectorConfig(), pr.getChannel(), pr.getRecord(), 1));
                }
            }
            Thread.yield(); // May give up the CPU
        }

        getEventPublisher().publishEvent(TimingMeterEvent.of(
                MetricsName.process_records_time,
                DataConnectorMeter.DEFAULT_PERCENTILES,
                Duration.ofNanos(nanoTime() - begin),
                getBasedMeterTags()));

        // Flush to all records in this batch are committed.
        flushWritten(writeResults);

        return writeResults;
    }

    private void enqueueDispatchExecutor(PendingResult pr, Queue<PendingResult> prsQueue) {
        if (log.isInfoEnabled()) {
            log.info("{} :: Re-enqueue and retry processing. pr : {}", getConnectorConfig().getName(), pr);
        }
        final ChannelInfoWrapper channel = pr.getChannel();
        final MessageRecord<String, Object> record = pr.getRecord();
        final PendingResult rePendingResult = new PendingResult(channel, record,
                determineDispatchExecutor(channel, record)
                        .submit(buildDispatchTask(pr.getChannel(), pr.getRecord())),
                pr.getRetryTimes() + 1);
        prsQueue.offer(rePendingResult);
    }

    private Callable<PendingMetadata> buildDispatchTask(ChannelInfoWrapper channel,
                                                        MessageRecord<String, Object> record) {
        return () -> {
            //final List<ComplexProcessResult> result = safeList(channel.getChains())
            //        //.stream()
            //        .parallelStream()
            //        .map(ch -> ch.process(record))
            //        .collect(toList());
            //final boolean hasMatched = result.stream().anyMatch(ComplexProcessResult::isMatched);
            //return new PendingMetadata(hasMatched, result.getRecord());

            boolean hasMatched = false;
            MessageRecord<String, Object> r = record;
            for (ComplexProcessChain chain : safeList(channel.getChains())) {
                final ComplexProcessResult result = chain.process(r);
                if (result.isMatched()) {
                    hasMatched = true;
                }
                r = result.getRecord();
            }
            return new PendingMetadata(hasMatched, r);
        };
    }

    private ThreadPoolExecutor determineDispatchExecutor(ChannelInfo channel, MessageRecord<String, Object> record) {
        final boolean sequence = channel.getSettingsSpec().getPolicySpec().isSequence();
        //final String key = String.valueOf(channel.getId());
        final String key = record.getKey();

        ThreadPoolExecutor executor = this.sharedNonSequenceExecutor;
        if (sequence) {
            final int index = Assignments.getInstance().assign(key, isolationSequenceExecutors.size());
            executor = isolationSequenceExecutors.get(index);
            if (log.isDebugEnabled()) {
                log.debug("{} :: {} :: Determined isolation sequence executor index : {}",
                        getConnectorConfig().getName(), channel.getId(), index);
            }
        }
        return executor;
    }

    @SuppressWarnings("all")
    public void flushWritten(@NotNull Collection<WritePointResult> writerResults) {
        requireNonNull(writerResults, "writerResults");
        writerResults.stream().collect(groupingBy(WritePointResult::getChannel))
                .forEach((channel, results) -> obtainChannelPointWriter(channel).flush(results));
    }

    public PointWriter obtainChannelPointWriter(@NotNull ChannelInfo channel) {
        requireNonNull(channel, "channel");
        return channelPointWriters.computeIfAbsent(channel.getId(), channelId -> {
            final ConnectorConfig connectorConfig = getContext().getConnectorConfig();
            return connectorConfig.getCheckpoint().createWriter(connectorConfig, channel, getRegistry());
        });
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class DispatchStreamConfig {
        private @Builder.Default int sharedExecutorThreadPoolSize = 50;
        private @Builder.Default int sharedExecutorQueueSize = 500;
        private @Builder.Default int sequenceExecutorsMaxCountLimit = 100;
        private @Builder.Default int sequenceExecutorsPerQueueSize = 100;
        private @Builder.Default boolean executorWarmUp = true;

        public void validate() {
            Assert2.isTrueOf(sharedExecutorThreadPoolSize > 0, "sharedExecutorThreadPoolSize > 0");
            Assert2.isTrueOf(sharedExecutorQueueSize > 0, "sharedExecutorQueueSize > 0");
            Assert2.isTrueOf(sequenceExecutorsMaxCountLimit > 0, "sequenceExecutorsMaxCountLimit > 0");
            Assert2.isTrueOf(sequenceExecutorsPerQueueSize > 0, "sequenceExecutorsPerQueueSize > 0");
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class PendingResult {
        private ChannelInfoWrapper channel;
        private MessageRecord<String, Object> record;
        private Future<PendingMetadata> future;
        private int retryTimes;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class PendingMetadata {
        private boolean matched;
        private MessageRecord<String, Object> record;
    }

}
