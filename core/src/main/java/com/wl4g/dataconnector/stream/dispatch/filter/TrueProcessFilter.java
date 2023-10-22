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

package com.wl4g.dataconnector.stream.dispatch.filter;

import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.filter.TrueProcessFilter.TrueProcessFilterConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

/**
 * The always true filter.
 * expression record.
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class TrueProcessFilter extends AbstractProcessFilter<TrueProcessFilterConfig> {

    public TrueProcessFilter(@NotNull DataConnectorConfiguration config,
                             @NotNull TrueProcessFilterConfig filterConfig,
                             @NotNull String channelId,
                             @NotNull ChannelInfo.RuleItem rule) {
        super(config, filterConfig, channelId, rule);
    }

    @Override
    public boolean doFilter(MessageRecord<String, Object> record) {
        return true;
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class TrueProcessFilterConfig extends IProcessFilter.ProcessFilterConfig {
        public static final TrueProcessFilterConfig DEFAULT = new TrueProcessFilterConfig();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class TrueProcessFilterProvider extends ProcessFilterProvider {
        public static final String TYPE_NAME = "TRUE_FILTER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessFilter> T create(@NotNull DataConnectorConfiguration config,
                                                   @NotNull ChannelInfo channel,
                                                   @NotNull ChannelInfo.RuleItem rule) {
            return (T) new TrueProcessFilter(config, TrueProcessFilterConfig.DEFAULT, channel.getId(), rule);
        }
    }


}
