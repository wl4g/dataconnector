/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g.kafkasubscriber.filter;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeFilterConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * The {@link AbstractSubscribeFilter}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public abstract class AbstractSubscribeFilter implements ISubscribeFilter {

    private String name;
    private SubscribeFilterConfig filterConfig = new SubscribeFilterConfig();

    @Override
    public void validate() {
        Assert2.hasTextOf(name, "name");
        filterConfig.validate();
    }

}