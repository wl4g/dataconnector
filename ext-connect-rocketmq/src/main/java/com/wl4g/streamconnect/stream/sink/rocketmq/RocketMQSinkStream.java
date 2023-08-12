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

package com.wl4g.streamconnect.stream.sink.rocketmq;

import com.google.common.util.concurrent.SettableFuture;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.meter.StreamConnectMeter;
import com.wl4g.streamconnect.stream.sink.SinkStream;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.producer.SendCallback;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.util.Arrays;
import java.util.List;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.streamconnect.stream.AbstractStream.BaseStreamConfig.getStreamProviderTypeName;
import static java.lang.String.valueOf;
import static java.util.Objects.requireNonNull;

/**
 * The {@link RocketMQSinkStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class RocketMQSinkStream extends SinkStream {

    private final RocketMQSinkStreamConfig sinkStreamConfig;
    private final List<String> basedMetricsTags;
    private final ConcurrentRocketMQSinkContainer internalTask;

    public RocketMQSinkStream(@NotNull final StreamContext context,
                              @NotNull final RocketMQSinkStreamConfig sinkStreamConfig,
                              @NotNull final ChannelInfo channel) {
        super(context, channel);
        this.sinkStreamConfig = requireNonNull(sinkStreamConfig,
                "sinkStreamConfig must not be null");

        this.basedMetricsTags = Arrays.asList(
                StreamConnectMeter.MetricsTag.CONNECTOR, getConnectorConfig().getName(),
                StreamConnectMeter.MetricsTag.TOPIC, sinkStreamConfig.getTopic(),
                StreamConnectMeter.MetricsTag.QOS, sinkStreamConfig.getQos(),
                StreamConnectMeter.MetricsTag.CHANNEL, getChannel().getId());

        // Create to RocketMQ producers.
        this.internalTask = new ConcurrentRocketMQSinkContainer(sinkStreamConfig, channel);
    }

    @Override
    public String getDescription() {
        return String.format("%s-%s-%s", super.getDescription(), sinkStreamConfig.getTopic(),
                sinkStreamConfig.getQos());
    }

    @Override
    public List<String> getBasedMeterTags() {
        return basedMetricsTags;
    }

    @Override
    public SinkResult doSink(MessageRecord<String, Object> processedRecord,
                             int retryTimes) {
        final String key = processedRecord.getKey();
        final Object value = processedRecord.getValue();
        final String channelId = (String) safeMap(processedRecord.getMetadata()).get(KEY_CHANNEL);
        final boolean isSequence = Boolean.parseBoolean((String) safeMap(processedRecord.getMetadata()).get(KEY_SEQUENCE));
        try {
            final Message msg = new Message(sinkStreamConfig.getTopic(), sinkStreamConfig.getTag(),
                    key, valueOf(value).getBytes(RemotingHelper.DEFAULT_CHARSET));
            final SettableFuture<Object> future = SettableFuture.create();
            internalTask.determineRocketMQProducer(isSequence, key, channelId)
                    .send(msg, new SendCallback() {
                        @Override
                        public void onSuccess(SendResult sendResult) {
                            future.set(sendResult);
                        }

                        @Override
                        public void onException(Throwable ex) {
                            future.setException(ex);
                        }
                    });
            return new SinkResult(processedRecord, future, retryTimes);
        } catch (Exception e) {
            throw new RuntimeException("Failed to send message to RocketMQ", e);
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class RocketMQSinkStreamConfig extends SinkStreamConfig {
        public static final String TYPE_NAME = "ROCKETMQ_SINK";

        private @NotBlank String namesrvAddr;
        private @NotBlank String topic;
        private @NotBlank String producerGroup;
        private @Null String tag;
        private @Null String accessKey;
        private @Null String secretKey;
        private @Null String securityToken;
        private @Null String signature;
        private @Builder.Default boolean enableTls = false;
        private @Builder.Default int sendMsgTimeoutMs = 3_000;
        private @Builder.Default int retryTimesWhenSendFailed = 2;
        private @Builder.Default int retryTimesWhenSendAsyncFailed = 2;
        private @Builder.Default boolean retryAnotherBrokerWhenNotStoreOK = false;
        private @Builder.Default int maxMessageSize = 1024 * 1024 * 4; // 4M

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public void validate() {
            super.validate();
            Assert2.hasTextOf(namesrvAddr, "namesrvAddr");
            Assert2.hasTextOf(topic, "topic");
            Assert2.hasTextOf(producerGroup, "producerGroup");
            Assert2.isTrueOf(sendMsgTimeoutMs > 0, "sendMsgTimeoutMs > 0");
            Assert2.isTrueOf(retryTimesWhenSendFailed > 0, "retryTimesWhenSendFailed > 0");
            Assert2.isTrueOf(retryTimesWhenSendAsyncFailed > 0, "retryTimesWhenSendAsyncFailed > 0");
            Assert2.isTrueOf(maxMessageSize > 0, "maxMessageSize > 0");
        }
    }

    public static class RocketMQSinkStreamProvider extends SinkStreamProvider {
        public static final String TYPE_NAME = getStreamProviderTypeName(RocketMQSinkStreamConfig.TYPE_NAME);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public RocketMQSinkStreamBootstrap create(@NotNull final StreamContext context,
                                                  @NotNull final SinkStreamConfig sinkStreamConfig,
                                                  @NotNull final ChannelInfo channel) {
            // Create RocketMQ sink stream.
            final RocketMQSinkStream sinkStream = new RocketMQSinkStream(context,
                    (RocketMQSinkStreamConfig) sinkStreamConfig, channel);

            // Create RocketMQ sink stream wrapper(bootstrap).
            return new RocketMQSinkStreamBootstrap(sinkStream, sinkStream.getInternalTask());
        }
    }

}
