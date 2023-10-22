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

import com.wl4g.dataconnector.config.ChannelInfo.RuleItem;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * The {@link AbstractProcessFilter}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@Slf4j
public abstract class AbstractProcessFilter<C> implements IProcessFilter {
    private final @NotNull DataConnectorConfiguration config;
    private final @NotNull C filterConfig;
    private final @NotNull String channelId;
    private final @NotNull RuleItem rule;

    public AbstractProcessFilter(@NotNull DataConnectorConfiguration config,
                                 @NotNull C filterConfig,
                                 @NotNull String channelId,
                                 @NotNull RuleItem rule) {
        this.config = requireNonNull(config, "config must not be null");
        this.filterConfig = requireNonNull(filterConfig, "filterConfig must not be null");
        this.channelId = requireNonNull(channelId, "channelId must not be null");
        this.rule = requireNonNull(rule, "rule must not be null");
    }

    @Override
    public String toString() {
        return getClass().getSimpleName().concat("-").concat(channelId).concat("-").concat(rule.getName());
    }

}
