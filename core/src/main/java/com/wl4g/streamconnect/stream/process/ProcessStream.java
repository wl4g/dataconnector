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

package com.wl4g.streamconnect.stream.process;

import com.google.common.annotations.VisibleForTesting;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.ICheckpoint.PointWriter;
import com.wl4g.streamconnect.checkpoint.ICheckpoint.WritePointResult;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.exception.GiveUpRetryProcessException;
import com.wl4g.streamconnect.meter.MeterEventHandler.CountMeterEvent;
import com.wl4g.streamconnect.meter.MeterEventHandler.TimingMeterEvent;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.stream.AbstractStream;
import com.wl4g.streamconnect.stream.process.ComplexProcessChain.ComplexProcessResult;
import com.wl4g.streamconnect.stream.source.SourceStream;
import com.wl4g.streamconnect.util.Assignments;
import com.wl4g.streamconnect.util.concurrent.BlockAbortPolicy;
import com.wl4g.streamconnect.util.concurrent.NamedThreadFactory;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.exception.ExceptionUtils;

import javax.validation.constraints.NotNull;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Collections.synchronizedList;
import static java.util.Objects.nonNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.groupingBy;
import static java.util.stream.Collectors.toCollection;
import static java.util.stream.Collectors.toList;

/**
 * The {@link ProcessStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class ProcessStream extends AbstractStream {
    private final SourceStream sourceStream;
    private final ThreadPoolExecutor sharedNonSequenceExecutor;
    private final List<ThreadPoolExecutor> isolationSequenceExecutors;
    private final Map<String, PointWriter> channelPointWriters = new ConcurrentHashMap<>();

    public ProcessStream(@NotNull final StreamContext context,
                         @NotNull final SourceStream sourceStream) {
        super(context);
        this.sourceStream = requireNonNull(sourceStream, "sourceStream is null");

        // Create the shared filterProvider single executor.
        final ProcessStreamConfig processConfig = getConnectorConfig().getProcessConfig();
        this.sharedNonSequenceExecutor = new ThreadPoolExecutor(processConfig.getSharedExecutorThreadPoolSize(),
                processConfig.getSharedExecutorThreadPoolSize(),
                0L, TimeUnit.MILLISECONDS,
                new LinkedBlockingQueue<>(processConfig.getSharedExecutorQueueSize()),
                new NamedThreadFactory("processStream-sharedNonSeq-"),
                new BlockAbortPolicy());
        if (processConfig.isExecutorWarmUp()) {
            this.sharedNonSequenceExecutor.prestartAllCoreThreads();
        }

        // Create the sequence filterProvider executors.
        this.isolationSequenceExecutors = synchronizedList(new ArrayList<>(processConfig
                .getSequenceExecutorsMaxCountLimit()));
        for (int i = 0; i < processConfig.getSequenceExecutorsMaxCountLimit(); i++) {
            final ThreadPoolExecutor executor = new ThreadPoolExecutor(1, 1,
                    0L, TimeUnit.MILLISECONDS,
                    new LinkedBlockingQueue<>(processConfig.getSequenceExecutorsPerQueueSize()),
                    new NamedThreadFactory("processStream-isolationSeq-"),
                    new BlockAbortPolicy());
            if (processConfig.isExecutorWarmUp()) {
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
            } catch (Throwable ex) {
                log.error(String.format("%s :: Failed to close shared filter executor.", getConnectorConfig().getName()), ex);
            }
        }
        this.isolationSequenceExecutors.forEach(executor -> {
            if (!executor.isShutdown()) {
                try {
                    log.info("{} :: Closing filter executor {}...", getConnectorConfig().getName(), executor);
                    executor.shutdown();
                    log.info("{} :: Closed filter executor {}.", getConnectorConfig().getName(), executor);
                } catch (Throwable ex) {
                    log.error(String.format("%s :: Failed to close filter executor %s.", getConnectorConfig().getName(), executor), ex);
                }
            }
        });
    }

    public Queue<WritePointResult> process(List<? extends MessageRecord<String, Object>> records) {
        return doParallelProcessAndSavepoint(records);
    }

    private Queue<WritePointResult> doParallelProcessAndSavepoint(
            List<? extends MessageRecord<String, Object>> records) {

        // Obtain the connector filters,mappers process chain.
        final ComplexProcessChain chain = getConnectorConfig().getProcessChain();

        // Match wrap to channel records.
        final List<ChannelRecord> channelRecords = matchToChannelRecords(chain, records);

        // Add timing process metrics.
        // The benefit of not using lamda records is better use of arthas for troubleshooting during operation.
        final long processTimingBegin = System.nanoTime();

        // Execute custom filters in parallel them to different send executor queues.
        final Queue<ProcessResult> processResults = safeList(channelRecords).stream()
                .map(cr -> new ProcessResult(cr, determineProcessExecutor(cr)
                        .submit(buildProcessTask(cr, chain)), 1))
                .collect(toCollection(ConcurrentLinkedQueue::new));

        // Wait for all parallel processed results to be completed.
        final Queue<WritePointResult> writePointResults = new ConcurrentLinkedQueue<>();
        while (!processResults.isEmpty()) {
            final ProcessResult pr = processResults.poll();
            // Notice: is done is not necessarily successful, both exception and cancellation will be done.
            if (pr.getFuture().isDone()) {
                ProcessMetadata pm = null;
                try {
                    pm = pr.getFuture().get();

                    getEventPublisher().publishEvent(new CountMeterEvent(
                            MetricsName.process_records_success,
                            getBasedMeterTags()));
                } catch (InterruptedException | CancellationException | ExecutionException ex) {
                    log.error("{} :: Unable to get process result.", getConnectorConfig().getName(), ex);

                    getEventPublisher().publishEvent(new CountMeterEvent(
                            MetricsName.process_records_failure,
                            getBasedMeterTags()));

                    if (ex instanceof ExecutionException) {
                        final Throwable reason = ExceptionUtils.getRootCause(ex);
                        if (reason instanceof GiveUpRetryProcessException) { // User need giveUp retry
                            log.warn("{} :: User ask to give up re-trying again process. fr : {}, reason : {}",
                                    getConnectorConfig().getName(), pr, reason.getMessage());
                        } else {
                            getConnectorConfig().getQos().retryIfFail(getConnectorConfig(),
                                    pr.getRetryTimes(), () -> {
                                        if (log.isDebugEnabled()) {
                                            log.debug("{} :: Retry to process. pr : {}", getConnectorConfig().getName(), pr);
                                        }
                                        enqueueProcessExecutor(chain, pr, processResults);
                                    });
                        }
                    }
                }
                if (nonNull(pm) && pm.isMatched()) {
                    // Send to processed topic and add sent future If necessary.
                    // Replace to mapped record(eg: data permission processing).
                    pr.getRecord().setRecord(pm.getRecord());
                    final PointWriter pointWriter = obtainChannelPointWriter(pr.getRecord().getChannel());
                    writePointResults.offer(pointWriter.writeAsync(getConnectorConfig(), pr.getRecord(), 1));
                }
            }
            Thread.yield(); // May give up the CPU
        }

        getEventPublisher().publishEvent(new TimingMeterEvent(
                MetricsName.process_records_time,
                StreamConnectMeter.DEFAULT_PERCENTILES,
                Duration.ofNanos(System.nanoTime() - processTimingBegin),
                getBasedMeterTags()));

        // Flush to all records in this batch are committed.
        flushWritePoints(writePointResults);

        // Release the memory objects.
        channelRecords.clear();

        return writePointResults;
    }

    private List<ChannelRecord> matchToChannelRecords(ComplexProcessChain chain,
                                                      List<? extends MessageRecord<String, Object>> records) {
        final Collection<ChannelInfo> assignedChannels = getRegistry().getAssignedChannels(getConnectorConfig().getName());

        // Merge subscription server configurations and update to filters.
        // Notice: According to the consumption processing model design, it is necessary to share getConnectorConfig().getName()
        // consumption for unified processing, So here, all channel processing rules should be merged.
        chain.updateMergeConditions(assignedChannels);

        return doMatchToChannelRecords(getConfigurator(), getConnectorConfig(), assignedChannels, records);
    }

    @VisibleForTesting
    static List<ChannelRecord> doMatchToChannelRecords(@NotNull IStreamConnectConfigurator configurator,
                                                       @NotNull ConnectorConfig connectorConfig,
                                                       @NotNull Collection<ChannelInfo> shardingChannels,
                                                       @NotNull List<? extends MessageRecord<String, Object>> records) {
        requireNonNull(configurator, "configurator must not be null");
        requireNonNull(shardingChannels, "shardingChannels must not be null");
        requireNonNull(records, "records must not be null");

        return safeList(records)
                .parallelStream()
                .flatMap(r -> safeList(shardingChannels)
                        .stream()
                        .filter(c -> configurator.matchChannelRecord(connectorConfig.getName(), c, r))
                        .map(c -> new ChannelRecord(c, r)))
                .collect(toList());
    }

    private void enqueueProcessExecutor(ComplexProcessChain chain,
                                        ProcessResult pr,
                                        Queue<ProcessResult> prsQueue) {
        if (log.isInfoEnabled()) {
            log.info("{} :: Re-enqueue and retry processing. pr : {}", getConnectorConfig().getName(), pr);
        }
        final ChannelRecord cr = pr.getRecord();
        prsQueue.offer(new ProcessResult(pr.getRecord(),
                determineProcessExecutor(pr.getRecord()).submit(buildProcessTask(cr, chain)),
                pr.getRetryTimes() + 1));
    }

    private Callable<ProcessMetadata> buildProcessTask(ChannelRecord cr,
                                                       ComplexProcessChain chain) {
        return () -> {
            final ComplexProcessResult result = chain.process(cr.getChannel(), cr.getRecord());
            return new ProcessMetadata(result.isMatched(), result.getRecord());
        };
    }

    private ThreadPoolExecutor determineProcessExecutor(ChannelRecord record) {
        final ChannelInfo channel = record.getChannel();
        final String key = record.getRecord().getKey();
        return determineTaskExecutor(channel.getId(), channel.getSettingsSpec().getPolicySpec().isSequence(), key);
    }

    private ThreadPoolExecutor determineTaskExecutor(String channelId,
                                                     boolean isSequence,
                                                     String key) {
        ThreadPoolExecutor executor = this.sharedNonSequenceExecutor;
        if (isSequence) {
            //final String key = String.valueOf(channel.getId());
            final int index = Assignments.assign(key, isolationSequenceExecutors.size());
            executor = isolationSequenceExecutors.get((int) index);
            if (log.isDebugEnabled()) {
                log.debug("{} :: {} :: determined isolation sequence executor index : {}",
                        getConnectorConfig().getName(), channelId, index);
            }
        }
        return executor;
    }

    public PointWriter obtainChannelPointWriter(@NotNull ChannelInfo channel) {
        requireNonNull(channel, "channel");
        return channelPointWriters.computeIfAbsent(channel.getId(), channelId -> {
            final ConnectorConfig connectorConfig = getContext().getConnectorConfig();
            return connectorConfig.getCheckpoint().createWriter(connectorConfig, channel, getRegistry());
        });
    }

    public void flushWritePoints(@NotNull Collection<WritePointResult> writePointResults) {
        requireNonNull(writePointResults, "writePointResults");
        writePointResults.stream().collect(groupingBy(pr -> pr.getRecord().getChannel()))
                .forEach((channel, results) -> obtainChannelPointWriter(channel).flush(results));
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class ProcessStreamConfig {
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
    public static class ChannelRecord {
        private ChannelInfo channel;
        private MessageRecord<String, Object> record;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            ChannelRecord that = (ChannelRecord) o;
            return Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(record);
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class ProcessResult {
        private ChannelRecord record;
        private Future<ProcessMetadata> future;
        private int retryTimes;
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    static class ProcessMetadata {
        private boolean matched;
        private MessageRecord<String, Object> record;
    }

}
