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

package com.wl4g.streamconnect.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import lombok.Getter;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import javax.annotation.concurrent.ThreadSafe;
import java.util.Collection;
import java.util.Objects;

/**
 * The {@link ProcessFilterChain}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@ThreadSafe
public class ProcessFilterChain {

    private final IProcessFilter[] filters;

    public ProcessFilterChain(IProcessFilter[] filters) {
        this.filters = filters;
    }

    public void updateMergeSubscribeConditions(Collection<SubscriberInfo> subscribers) {
        if (Objects.nonNull(filters)) {
            for (IProcessFilter filter : filters) {
                filter.updateMergeSubscribeConditions(subscribers);
            }
        }
    }

    public boolean doFilter(SubscriberInfo subscriber,
                            ConsumerRecord<String, ObjectNode> record) {
        if (Objects.nonNull(filters)) {
            for (IProcessFilter filter : filters) {
                if (filter.doFilter(subscriber, record)) {
                    return false; // end filtering if there is no match.
                }
            }
        }
        return true;
    }

}