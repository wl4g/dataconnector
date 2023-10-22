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
import com.wl4g.dataconnector.config.ChannelInfo.RuleItem;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.framework.NamedDataConnectorSpi;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.ComplexProcessHandler;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;

/**
 * The custom record processing(filtering).
 *
 * @author James Wong
 * @since v1.0
 **/
public interface IProcessFilter extends ComplexProcessHandler {

    boolean doFilter(MessageRecord<String, Object> record);

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    abstract class ProcessFilterConfig {
    }

    @Getter
    @Setter
    @NoArgsConstructor
    abstract class ProcessFilterProvider extends NamedDataConnectorSpi {
        private @NotNull ProcessFilterConfig filterConfig;

        @Override
        public void validate() {
            super.validate();
            requireNonNull(filterConfig, "filterConfig is null");
        }

        public abstract <T extends IProcessFilter> T create(@NotNull DataConnectorConfiguration config,
                                                            @NotNull ChannelInfo channel,
                                                            @NotNull RuleItem rule);
    }
}
