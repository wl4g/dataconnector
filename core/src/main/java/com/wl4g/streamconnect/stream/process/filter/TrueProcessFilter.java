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

package com.wl4g.streamconnect.stream.process.filter;

import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.stream.AbstractStream.StreamContext;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;

/**
 * The always true filter.
 * expression record.
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class TrueProcessFilter extends AbstractProcessFilter {

    public TrueProcessFilter(StreamContext context) {
        super(context);
    }

    @Override
    public boolean doFilter(MessageRecord<String, Object> record) {
        return true;
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class TrueProcessFilterConfig extends ProcessFilterConfig {
    }

    public static class TrueProcessFilterProvider extends ProcessFilterProvider {
        public static final String TYPE_NAME = "TRUE_FILTER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessFilter> T create(@NotNull StreamContext context,
                                                   @Null ProcessFilterConfig processFilterConfig,
                                                   @Null ChannelInfo channel) {
            return (T) new TrueProcessFilter(context);
        }
    }


}
