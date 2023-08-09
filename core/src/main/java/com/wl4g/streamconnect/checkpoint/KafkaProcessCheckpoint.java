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

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.StreamConnectProperties.CheckpointQoS;
import com.wl4g.streamconnect.config.StreamConnectProperties.ProcessProperties;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Properties;

/**
 * The {@link KafkaProcessCheckpoint}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class KafkaProcessCheckpoint extends AbstractProcessCheckpoint {

    public static final String TYPE_NAME = "KAFKA_CHECKPOINT";

    private String topicPrefix = "subscribe_topic_ckp_";
    private int topicPartitions = 10;
    private short replicationFactor = 1;
    // TODO custom yaml with tag not support ???
    //private CheckpointQoS qos = CheckpointQoS.RETRIES_AT_MOST;
    private String qosType = CheckpointQoS.RETRIES_AT_MOST.name();
    private int qoSMaxRetries = 5;
    private long qoSMaxRetriesTimeout = 1800_000; // 30min
    private int producerMaxCountLimit = 10;
    private Properties producerProps = new Properties() {
        {
            setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
            setProperty(ProducerConfig.ACKS_CONFIG, "0");
            setProperty(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, "30000");
            setProperty(ProducerConfig.MAX_REQUEST_SIZE_CONFIG, "1048576");
            setProperty(ProducerConfig.SEND_BUFFER_CONFIG, "131072");
            setProperty(ProducerConfig.RETRIES_CONFIG, "5");
            setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "6000");
            setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip"); // snappy|gzip|lz4|zstd|none
            setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        }
    };
    private Properties defaultTopicProps = new Properties() {
        {
            setProperty(TopicConfig.CLEANUP_POLICY_CONFIG, "delete");
            setProperty(TopicConfig.RETENTION_MS_CONFIG, String.valueOf(Duration.ofDays(1)));
            setProperty(TopicConfig.RETENTION_BYTES_CONFIG, String.valueOf(DataSize.ofGigabytes(1).toBytes()));
            setProperty(TopicConfig.DELETE_RETENTION_MS_CONFIG, "86400000");
            //setProperty(TopicConfig.SEGMENT_MS_CONFIG, "86400000");
            //setProperty(TopicConfig.SEGMENT_BYTES_DOC, "1073741824");
            //setProperty(TopicConfig.SEGMENT_INDEX_BYTES_DOC, "10485760");
            //setProperty(TopicConfig.MIN_CLEANABLE_DIRTY_RATIO_CONFIG, "0.1");
            //setProperty(TopicConfig.MIN_COMPACTION_LAG_MS_CONFIG, "86400000");
            //setProperty(TopicConfig.MAX_COMPACTION_LAG_MS_CONFIG, "86400000");
            //setProperty(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "1");
            //setProperty(TopicConfig.FLUSH_MS_CONFIG, "1000");
            //setProperty(TopicConfig.FLUSH_MESSAGES_INTERVAL_CONFIG, "10000");
            //setProperty(TopicConfig.UNCLEAN_LEADER_ELECTION_ENABLE_CONFIG, "false");
        }
    };

    private ProcessProperties processProps = new ProcessProperties();

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public void validate() {
        super.validate();

        Assert2.hasTextOf(topicPrefix, "topicPrefix");
        Assert2.isTrueOf(topicPartitions > 0, "topicPartitions > 0");
        Assert2.isTrueOf(replicationFactor > 0, "replicationFactor > 0");

        Assert2.notNullOf(qosType, "qos");
        Assert2.isTrueOf(qoSMaxRetries > 0, "qosMaxRetries > 0");
        Assert2.isTrueOf(producerMaxCountLimit > 0, "checkpointProducerMaxCountLimit > 0");
        Assert2.isTrueOf(qoSMaxRetriesTimeout > 0, "checkpointTimeoutMs > 0");

        // check for producer props.
        // TODO should to be priority choose sources bootstrap servers?
        // 但这里也应该统一使用, com.wl4g.streamconnect.dispatch.CheckpointTopicManager.lambda$addTopicAllIfNecessary$0(CheckpointTopicManager.java:88)
        //Assert2.notNullOf(producerProps.get(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG), "bootstrap.servers");
        Assert2.notNullOf(producerProps.get(ProducerConfig.ACKS_CONFIG), "acks");
        Assert2.notNullOf(producerProps.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG), "request.timeout.ms");
        Assert2.notNullOf(producerProps.get(ProducerConfig.MAX_REQUEST_SIZE_CONFIG), "max.request.size");
        Assert2.notNullOf(producerProps.get(ProducerConfig.SEND_BUFFER_CONFIG), "send.buffer.bytes");
        Assert2.notNullOf(producerProps.get(ProducerConfig.RETRIES_CONFIG), "retries");
        Assert2.notNullOf(producerProps.get(ProducerConfig.RETRY_BACKOFF_MS_CONFIG), "retry.backoff.ms");
        Assert2.notNullOf(producerProps.get(ProducerConfig.COMPRESSION_TYPE_CONFIG), "compression.type");

        // check for topic props.
        Assert2.notNullOf(defaultTopicProps.get(TopicConfig.CLEANUP_POLICY_CONFIG), "cleanup.policy");
        Assert2.notNullOf(defaultTopicProps.get(TopicConfig.RETENTION_MS_CONFIG), "retention.ms");
        Assert2.notNullOf(defaultTopicProps.get(TopicConfig.RETENTION_BYTES_CONFIG), "retention.bytes");
        //Assert2.notNullOf(defaultTopicProps.get(TopicConfig.DELETE_RETENTION_MS_CONFIG), "delete.retention.ms");

        processProps.validate();
    }

    @Override
    public CheckpointQoS getQoS() {
        if (StringUtils.isNumeric(qosType)) {
            return CheckpointQoS.values()[Integer.parseInt(qosType)];
        }
        return CheckpointQoS.valueOf(qosType);
    }

}
