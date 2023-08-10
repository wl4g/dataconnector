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
 */

package com.wl4g.streamconnect.process;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.process.filter.IProcessFilter;
import com.wl4g.streamconnect.process.map.IProcessMapper;
import lombok.AllArgsConstructor;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;

/**
 * The {@link ComplexProcessChain}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class ComplexProcessChain {

    private final ComplexProcessHandler[] handlers;

    public ComplexProcessChain(ComplexProcessHandler[] handlers) {
        this.handlers = Assert2.notNullOf(handlers, "handlers");
    }

    public void initFilterMergeSubscribeConditions(Collection<SubscriberInfo> subscribers) {
        for (ComplexProcessHandler handler : handlers) {
            if (handler instanceof IProcessFilter) {
                ((IProcessFilter) handler).updateMergeSubscribeConditions(subscribers);
            }
        }
    }

    public ComplexProcessedResult doProcess(SubscriberInfo subscriber,
                                            ConsumerRecord<String, ObjectNode> record) {
        boolean lastMatched = false;
        for (ComplexProcessHandler handler : handlers) {
            if (handler instanceof IProcessFilter) {
                lastMatched = ((IProcessFilter) handler).doFilter(subscriber, record);
                if (!lastMatched) {
                    break;
                }
            } else if (handler instanceof IProcessMapper) {
                record = ((IProcessMapper) handler).doMap(subscriber, record);
            }
        }
        return new ComplexProcessedResult(lastMatched, record);
    }

    @Getter
    @AllArgsConstructor
    public static class ComplexProcessedResult {
        private boolean matched;
        private ConsumerRecord<String, ObjectNode> record;
    }

}
