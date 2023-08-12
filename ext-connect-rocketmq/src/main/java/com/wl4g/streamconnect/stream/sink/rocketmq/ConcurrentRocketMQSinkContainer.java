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

import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.stream.sink.rocketmq.RocketMQSinkStream.RocketMQSinkStreamConfig;
import com.wl4g.streamconnect.util.Assignments;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.acl.common.AclClientRPCHook;
import org.apache.rocketmq.acl.common.SessionCredentials;
import org.apache.rocketmq.client.producer.DefaultMQProducer;

import java.io.Closeable;
import java.util.List;
import java.util.UUID;
import java.util.stream.Stream;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.RandomUtils.nextInt;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The {@link ConcurrentRocketMQSinkContainer}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public class ConcurrentRocketMQSinkContainer implements Closeable {
    private final RocketMQSinkStreamConfig sinkStreamConfig;
    private final ChannelInfo channel;
    private final List<DefaultMQProducer> producers;

    public ConcurrentRocketMQSinkContainer(RocketMQSinkStreamConfig sinkStreamConfig,
                                           ChannelInfo channel) {
        this.sinkStreamConfig = requireNonNull(sinkStreamConfig, "sinkStreamConfig must not be null");
        this.channel = requireNonNull(channel, "channel must not be null");

        // Create to RocketMQ producers.
        this.producers = Stream.of(sinkStreamConfig.getParallelism())
                .map(this::buildRocketMQProducer)
                .collect(toList());
    }

    private DefaultMQProducer buildRocketMQProducer(int index) {
        final String producerGroup = sinkStreamConfig.getProducerGroup()
                .concat("-").concat(channel.getId());
        AclClientRPCHook aclHook = null;
        if (isNotBlank(sinkStreamConfig.getAccessKey())) {
            final SessionCredentials credentials = new SessionCredentials(sinkStreamConfig.getAccessKey(),
                    sinkStreamConfig.getSecretKey());
            if (isNotBlank(sinkStreamConfig.getSecurityToken())) {
                credentials.setSecurityToken(sinkStreamConfig.getSecurityToken());
            }
            if (isNotBlank(sinkStreamConfig.getSignature())) {
                credentials.setSignature(sinkStreamConfig.getSignature());
            }
            aclHook = new AclClientRPCHook(credentials);
        }
        final DefaultMQProducer producer = new DefaultMQProducer(producerGroup, aclHook);
        producer.setNamesrvAddr(sinkStreamConfig.getNamesrvAddr());
        producer.setSendMsgTimeout(sinkStreamConfig.getSendMsgTimeoutMs());
        producer.setRetryTimesWhenSendFailed(sinkStreamConfig.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(sinkStreamConfig.getRetryTimesWhenSendAsyncFailed());
        producer.setRetryAnotherBrokerWhenNotStoreOK(sinkStreamConfig.isRetryAnotherBrokerWhenNotStoreOK());
        producer.setMaxMessageSize(sinkStreamConfig.getMaxMessageSize());
        // TODO standalone configure prefix(or connector name)??
        producer.setUnitName(producerGroup.concat(String.valueOf(index)));
        producer.setInstanceName(UUID.randomUUID().toString());
        producer.setUseTLS(sinkStreamConfig.isEnableTls());
        return producer;
    }

    public DefaultMQProducer determineRocketMQProducer(boolean isSequence,
                                                       String key,
                                                       String channelId) {
        final int producerSize = producers.size();
        DefaultMQProducer producer = producers.get(nextInt(0, producerSize));
        if (isSequence) {
            //final String key = String.valueOf(channel.getId());
            final int index = Assignments.assign(key, producerSize);
            producer = producers.get(index);
            if (log.isDebugEnabled()) {
                log.debug("determined send isolation sequence producer index : {}, channel : {}",
                        index, channelId);
            }
        }
        if (isNull(producer)) {
            throw new StreamConnectException(String.format("Could not get producer by channel: %s, key: %s",
                    channelId, key));
        }
        if (log.isDebugEnabled()) {
            log.debug("Using RocketMQ producer by channel: {}, key: {}", channelId, key);
        }
        return producer;
    }

    public void start() {
        safeList(producers).forEach(producer -> {
            try {
                producer.start();
            } catch (Throwable ex) {
                throw new StreamConnectException(String.format("Failed to start RocketMQ producer of %s",
                        producer.getInstanceName()), ex);
            }
        });
    }

    @Override
    public void close() {
        if (log.isInfoEnabled()) {
            log.info("Closing RocketMQ producer for channel: {}, sink stream config: {}",
                    channel.getId(), sinkStreamConfig);
        }
        safeList(producers).forEach(DefaultMQProducer::shutdown);
        if (log.isInfoEnabled()) {
            log.info("Closed RocketMQ producer for channel: {}, sink stream config: {}",
                    channel.getId(), sinkStreamConfig);
        }
    }

}
