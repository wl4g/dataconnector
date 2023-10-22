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

package com.wl4g.dataconnector.checkpoint.memory;

import com.wl4g.dataconnector.checkpoint.AbstractCheckpoint;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.sink.SinkStream;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.validation.constraints.NotNull;
import java.util.Collection;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import static java.util.Objects.requireNonNull;

/**
 * The {@link DummyCheckpoint}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class DummyCheckpoint extends AbstractCheckpoint {
    public static final String TYPE_NAME = "DUMMY_CHECKPOINT";

    private DummyCheckpointConfig checkpointConfig;
    private ReadPointListener readPointListener;

    @Override
    public void init() {
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
            }

            @Override
            public WritePointResult writeAsync(ConnectorConfig connectorConfig,
                                               ChannelInfo channel,
                                               MessageRecord<String, Object> record,
                                               int retryTimes) {
                try {
                    requireNonNull(readPointListener, "Should not be here. readPointListener has not been initialized yet!");
                    readPointListener.onMessage(castMessageRecord(record), DEFAULT_ACKNOWLEDGE);
                } catch (Exception ex) {
                    throw new DataConnectorException(ex);
                }
                return new WritePointResult(channel, record, null, FUTURE_SUCCESS, retryTimes);
            }

            @Override
            public void flush(Collection<WritePointResult> results) {
            }
        };
    }

    @SuppressWarnings("unchecked")
    private <T extends MessageRecord<String, Object>> T castMessageRecord(MessageRecord<String, Object> record) {
        return (T) record;
    }

    @Override
    public PointReader createReader(@NotNull ConnectorConfig connectorConfig,
                                    @NotNull ChannelInfo channel,
                                    @NotNull ReadPointListener listener) {
        return new PointReader() {
            @Override
            public void start() {
                readPointListener = listener;
                readPointListener.setReader(this);
            }

            @Override
            public boolean stop(long timeout, boolean force) {
                return true;
            }

            @Override
            public void preferAutoAcknowledge(Collection<SinkStream.SinkResult> sentResults) {
                // ignore
            }

            @Override
            public void addAcknowledgeCountMeter(DataConnectorMeter.MetricsName metrics,
                                                 Collection<SinkStream.SinkResult> sinkResults) {
                // ignore
            }
        };
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class DummyCheckpointConfig extends CheckpointConfig {
        @SuppressWarnings("all")
        @Override
        public void validate() {
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

        @SuppressWarnings("all")
        @Override
        public Object get(long timeout, TimeUnit unit) {
            return null;
        }
    };

    public static final Runnable DEFAULT_ACKNOWLEDGE = () -> {
    };

}
