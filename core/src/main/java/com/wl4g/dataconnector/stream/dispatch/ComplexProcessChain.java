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

package com.wl4g.dataconnector.stream.dispatch;

import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.filter.IProcessFilter;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper.MapperContext;
import lombok.AllArgsConstructor;
import lombok.Getter;

import javax.annotation.concurrent.ThreadSafe;

import static java.util.Objects.isNull;

/**
 * The {@link ComplexProcessChain}
 *
 * @author James Wong
 * @since v1.0
 **/
@ThreadSafe
public class ComplexProcessChain {

    private final ComplexProcessHandler[] processes;

    public ComplexProcessChain(ComplexProcessHandler[] processes) {
        this.processes = isNull(processes) ? new ComplexProcessHandler[0] : processes;
    }

    public ComplexProcessResult process(boolean hasNext, MessageRecord<String, Object> record) {
        final MapperContext context = new MapperContext(hasNext, record);
        boolean lastMatched = false;
        for (ComplexProcessHandler handler : processes) {
            if (handler instanceof IProcessFilter) {
                lastMatched = ((IProcessFilter) handler).doFilter(record);
                if (!lastMatched) {
                    break;
                }
            } else if (handler instanceof IProcessMapper) {
                record = ((IProcessMapper) handler).doMap(context);
            }
        }
        return new ComplexProcessResult(lastMatched, record);
    }

    @Getter
    @AllArgsConstructor
    public static class ComplexProcessResult {
        private boolean matched;
        private MessageRecord<String, Object> record;
    }

}
