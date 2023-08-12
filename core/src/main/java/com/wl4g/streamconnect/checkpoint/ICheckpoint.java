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

package com.wl4g.streamconnect.checkpoint;

import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.framework.IStreamConnectSpi;
import com.wl4g.streamconnect.meter.StreamConnectMeter.MetricsName;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.stream.process.ProcessStream.ChannelRecord;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.Closeable;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Future;

/**
 * The {@link ICheckpoint}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface ICheckpoint extends IStreamConnectSpi {

    CheckpointConfig getCheckpointConfig();

    PointWriter createWriter(@NotNull ConnectorConfig connectorConfig,
                             @NotNull ChannelInfo channel,
                             @NotNull CachingChannelRegistry registry);

    PointReader createReader(@NotNull ConnectorConfig connectorConfig,
                             @NotNull ChannelInfo channel,
                             @NotNull ReadPointListener listener);

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    abstract class CheckpointConfig {
        public void validate() {
        }
    }

    @Getter
    @Setter
    @AllArgsConstructor
    @ToString
    class WritePointResult {
        private ChannelRecord record;
        private Object internalOperator;
        private Future<?> future;
        private int retryTimes;

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            WritePointResult that = (WritePointResult) o;
            return Objects.equals(record, that.record);
        }

        @Override
        public int hashCode() {
            return Objects.hash(record);
        }
    }

    interface PointWriter extends Closeable {
        void stop(long timeoutMs, boolean force) throws Exception;

        @Override
        default void close() {
            try {
                stop(Long.MAX_VALUE, true);
            } catch (Throwable ex) {
                throw new IllegalStateException(ex);
            }
        }

        WritePointResult writeAsync(ConnectorConfig connectorConfig,
                                    ChannelRecord record,
                                    int retryTimes);

        void flush(Collection<WritePointResult> results);
    }

    interface PointReader extends Closeable {
        void start();

        boolean stop(long timeout, boolean force) throws Exception;

        @Override
        default void close() {
            try {
                stop(Long.MAX_VALUE, true);
            } catch (Throwable ex) {
                throw new IllegalStateException(ex);
            }
        }

        default void pause() {
            throw new UnsupportedOperationException();
        }

        default void resume() {
            throw new UnsupportedOperationException();
        }

        default boolean scaling(int concurrency,
                                boolean restart,
                                long restartTimeout) throws Exception {
            throw new UnsupportedOperationException();
        }

        default boolean isRunning() {
            throw new UnsupportedOperationException();
        }

        default int getSubTaskCount() {
            throw new UnsupportedOperationException();
        }

        default void preferAutoAcknowledge(Collection<SinkStream.SinkResult> sentResults) {
            throw new UnsupportedOperationException();
        }

        default void addAcknowledgeCountMeter(MetricsName metrics,
                                              Collection<SinkStream.SinkResult> sinkResults) {
            throw new UnsupportedOperationException();
        }
    }

    @Getter
    @Setter
    abstract class ReadPointListener {
        private PointReader reader;

        public abstract void onMessage(List<? extends MessageRecord<String, Object>> records,
                                       Runnable ack);
    }

    // TODO add support custom avro/protobuf serialization
    // --- Checkpoint serialization. ---

    interface PointSerializer<T> {
        /**
         * Convert {@code data} into a byte array.
         *
         * @param topic   topic associated with data
         * @param headers headers associated with the record
         * @param data    typed data
         * @return serialized bytes
         */
        byte[] serialize(String topic, Map<String, T> headers, T data);
    }

    interface PointDeserializer<T> {
        /**
         * Deserialize a record value from a byte array into a value or object.
         *
         * @param topic   topic associated with the data
         * @param headers headers associated with the record; may be empty.
         * @param data    serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
         * @return deserialized typed data; may be null
         */
        T deserialize(String topic, Map<String, T> headers, byte[] data);
    }

}
