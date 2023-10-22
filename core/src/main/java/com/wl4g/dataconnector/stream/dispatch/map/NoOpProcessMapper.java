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

package com.wl4g.dataconnector.stream.dispatch.map;

import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.map.NoOpProcessMapper.NoOpProcessMapperConfig;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

/**
 * No-operation mapper.
 *
 * @author James Wong
 * @since v1.0
 **/
public class NoOpProcessMapper extends AbstractProcessMapper<NoOpProcessMapperConfig> {

    public NoOpProcessMapper(@NotNull DataConnectorConfiguration config,
                             @NotNull NoOpProcessMapperConfig mapperConfig,
                             @NotNull String channelId,
                             @NotNull ChannelInfo.RuleItem rule) {
        super(config, mapperConfig, channelId, rule);
    }

    @Override
    public MessageRecord<String, Object> doMap(MessageRecord<String, Object> record) {
        return record;
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class NoOpProcessMapperConfig extends IProcessMapper.ProcessMapperConfig {
        public static final NoOpProcessMapperConfig DEFAULT = new NoOpProcessMapperConfig();
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class NoOpProcessMapperProvider extends ProcessMapperProvider {
        public static final String TYPE_NAME = "NOOP_MAPPER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessMapper> T create(@NotNull DataConnectorConfiguration config,
                                                   @NotNull ChannelInfo channel,
                                                   @NotNull ChannelInfo.RuleItem rule) {
            return (T) new NoOpProcessMapper(config, NoOpProcessMapperConfig.DEFAULT, channel.getId(), rule);
        }
    }

}
