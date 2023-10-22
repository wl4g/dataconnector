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

import com.wl4g.dataconnector.config.ChannelInfo.RuleItem;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.stream.dispatch.map.IProcessMapper.ProcessMapperConfig;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * The {@link AbstractProcessMapper}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class AbstractProcessMapper<C extends ProcessMapperConfig> implements IProcessMapper {
    private final @NotNull DataConnectorConfiguration config;
    private final @NotNull C mapperConfig;
    private final @NotNull String channelId;
    private final @NotNull RuleItem rule;

    public AbstractProcessMapper(@NotNull DataConnectorConfiguration config,
                                 @NotNull C mapperConfig,
                                 @NotNull String channelId,
                                 @NotNull RuleItem rule) {
        this.config = requireNonNull(config, "config must not be null");
        this.mapperConfig = requireNonNull(mapperConfig, "mapperConfig must not be null");
        this.channelId = requireNonNull(channelId, "channelId must not be null");
        this.rule = requireNonNull(rule, "rule must not be null");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().concat("-").concat(channelId).concat("-").concat(rule.getName());
    }

}
