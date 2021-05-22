/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.sink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.kafkasubscriber.dispatch.SinkBatchMessageDispatcher;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

/**
 * The {@link DefaultPrintSubscribeSink}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class DefaultPrintSubscribeSink implements ISubscribeSink {

    public static final String BEAN_NAME = "defaultPrintSubscribeSink";

    @Override
    public SinkBatchMessageDispatcher.SinkCompleted doSink(long subscriberId,
                                                           boolean sequence,
                                                           ConsumerRecord<String, ObjectNode> record) {
        log.info("----- This is a default printer sink, and you should customize the implementation of sink logic ! -----\n{}", record);
        return SinkBatchMessageDispatcher.SinkCompleted.EMPTY;
    }

}
