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

package com.wl4g.streamconnect.map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.concurrent.NotThreadSafe;
import java.util.Objects;

/**
 * The {@link ProcessMapperChain}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@NotThreadSafe
public class ProcessMapperChain {

    private final IProcessMapper[] mappers;
    private int index = 0;

    public ProcessMapperChain(IProcessMapper[] mappers) {
        this.mappers = mappers;
    }

    public ConsumerRecord<String, ObjectNode> doMap(SubscriberInfo subscriber,
                                                    ConsumerRecord<String, ObjectNode> record) {
        ConsumerRecord<String, ObjectNode> mapped = record;
        if (Objects.nonNull(mappers) && index < mappers.length) {
            mapped = mappers[index++].doMap(this, subscriber, record);
        }
        return mapped;
    }

}
