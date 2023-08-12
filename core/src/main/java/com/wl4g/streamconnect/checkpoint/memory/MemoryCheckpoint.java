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

package com.wl4g.streamconnect.checkpoint.memory;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.checkpoint.AbstractCheckpoint;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.stream.process.ProcessStream.ChannelRecord;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

/**
 * The {@link MemoryCheckpoint}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class MemoryCheckpoint extends AbstractCheckpoint {
    public static final String TYPE_NAME = "MEMORY_CHECKPOINT";

    private MemoryCheckpointConfig checkpointConfig;
    private Queue<MessageRecord<String, Object>> bufferQueue;

    @Override
    public void init() {
        // TODO support other queue?
        this.bufferQueue = new LinkedBlockingQueue<>(getCheckpointConfig().getBufferQueueCapacity());
    }

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public PointWriter createWriter(@NotNull ConnectorConfig connectorConfig,
                                    @NotNull ChannelInfo channel,
                                    @NotNull CachingChannelRegistry registry) {
        return new PointWriter() {
            @Override
            public void stop(long timeoutMs, boolean force) {
                if (force) {
                    bufferQueue.clear();
                }
            }

            @Override
            public WritePointResult writeAsync(ConnectorConfig connectorConfig,
                                               ChannelRecord record,
                                               int retryTimes) {
                Future<?> future = FUTURE_FAILURE;
                if (bufferQueue.offer(record.getRecord())) {
                    future = FUTURE_SUCCESS;
                }
                return new WritePointResult(record, bufferQueue, future, retryTimes);
            }

            @Override
            public void flush(Collection<WritePointResult> results) {
            }
        };
    }

    @Override
    public PointReader createReader(@NotNull ConnectorConfig connectorConfig,
                                    @NotNull ChannelInfo channel,
                                    @NotNull ReadPointListener listener) {
        return new PointReader() {
            private Thread worker;

            @SuppressWarnings("unchecked")
            @Override
            public synchronized void start() {
                if (Objects.isNull(worker)) {
                    this.worker = new Thread(() -> {
                        while (!worker.isInterrupted()) {
                            final MessageRecord<String, Object> record = bufferQueue.poll();
                            listener.onMessage((List<? extends MessageRecord<String, Object>>) record, DEFAULT_ACKNOWLEDGE);
                        }
                    }, getClass().getSimpleName().concat("-reader-").concat(channel.getId()));
                    this.worker.start();
                }
            }

            @Override
            public boolean stop(long timeout, boolean force) {
                if (worker.isInterrupted()) {
                    worker.interrupt();
                }
                return worker.isInterrupted();
            }

            @Override
            public void preferAutoAcknowledge(Collection<SinkStream.SinkResult> sentResults) {
                // ignore
            }

            @Override
            public void addAcknowledgeCountMeter(StreamConnectMeter.MetricsName metrics,
                                                 Collection<SinkStream.SinkResult> sinkResults) {
                // ignore
            }
        };
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class MemoryCheckpointConfig extends CheckpointConfig {
        @Builder.Default
        private @Min(0) int bufferQueueCapacity = 1024;

        @Override
        public void validate() {
            Assert2.isTrueOf(bufferQueueCapacity > 0, "bufferQueueCapacity > 0");
        }
    }

    public static final Future<?> FUTURE_SUCCESS = new Future<Object>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            return null;
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    };

    public static final Future<?> FUTURE_FAILURE = new Future<Object>() {
        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return false;
        }

        @Override
        public boolean isCancelled() {
            return false;
        }

        @Override
        public boolean isDone() {
            return true;
        }

        @Override
        public Object get() {
            throw new StreamConnectException("Failed to offer memory queue.");
        }

        @Override
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    };

    public static final Runnable DEFAULT_ACKNOWLEDGE = () -> {
    };

}
