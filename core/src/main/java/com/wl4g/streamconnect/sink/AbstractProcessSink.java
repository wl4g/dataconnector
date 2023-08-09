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

package com.wl4g.streamconnect.sink;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.config.StreamConnectProperties.SubscribeSinkProperties;
import lombok.Getter;
import lombok.Setter;

/**
 * The {@link AbstractProcessSink}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public abstract class AbstractProcessSink implements IProcessSink {

    private String name;
    private SubscribeSinkProperties sinkConfig = new SubscribeSinkProperties();

    @Override
    public void validate() {
        Assert2.hasTextOf(name, "name");
        sinkConfig.validate();
    }

}