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

package com.wl4g.streamconnect.sink;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.config.StreamConnectProperties.SubscribeSinkProperties;
import com.wl4g.streamconnect.framework.IStreamConnectSpi;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.io.Serializable;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

/**
 * The {@link IProcessSink}
 *
 * @author James Wong
 * @since v1.0
 **/
public interface IProcessSink extends IStreamConnectSpi {

    SubscribeSinkProperties getSinkConfig();

    default Future<? extends Serializable> doSink(String subscriberId,
                                                  boolean sequence,
                                                  ConsumerRecord<String, ObjectNode> record) {
        return DONE;
    }

    Future<Serializable> DONE = new Future<Serializable>() {
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
        public Serializable get() {
            return null;
        }

        @Override
        public Serializable get(long timeout, TimeUnit unit) {
            return null;
        }
    };

}
