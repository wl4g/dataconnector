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

package com.wl4g.streamconnect.process.sink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.concurrent.Future;

/**
 * The {@link NoOpProcessSink}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class NoOpProcessSink extends AbstractProcessSink {

    public static final String TYPE_NAME = "NOOP_SINK";

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public Future<? extends Serializable> doSink(String subscriberId,
                                                 boolean sequence,
                                                 ConsumerRecord<String, ObjectNode> record) {
        log.info("----- This is a default printer sink, and you should customize " +
                "the implementation of sink logic ! -----\nsubscriberId: {}\nrecord: {}", subscriberId, record);
        return super.doSink(subscriberId, sequence, record);
    }

}
