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

package com.wl4g.dataconnector.config.configurator;

import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.meter.DataConnectorMeter;
import lombok.AllArgsConstructor;
import org.springframework.core.env.Environment;

/**
 * The {@link AbstractDataConnectorConfigurator}
 *
 * @author James Wong
 * @since v1.0
 **/
@AllArgsConstructor
public abstract class AbstractDataConnectorConfigurator implements IDataConnectorConfigurator {
    protected final Environment environment;
    protected final DataConnectorConfiguration config;
    protected final DataConnectorMeter meter;
}