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
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.concurrent.Future;

/**
 * The {@link DefaultSubscribeSink}
 *
 * @author James Wong
 * @since v1.0
 **/
public class DefaultSubscribeSink implements ISubscribeSink {

    public static final String BEAN_NAME = "defaultSubscribeSink";

    @Override
    public Future<?> apply(ConsumerRecord<String, ObjectNode> filteredRecord) {
        // TODO Auto-generated method stub
        return null;
    }

}
