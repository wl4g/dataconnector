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

package com.wl4g.dataconnector.checkpoint;

import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.framework.NamedDataConnectorSpi;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.context.ApplicationEventPublisher;

import static java.util.Objects.requireNonNull;

/**
 * The {@link AbstractCheckpoint}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@Slf4j
public abstract class AbstractCheckpoint
        extends NamedDataConnectorSpi implements ICheckpoint {

    private DataConnectorConfiguration config;

    @Override
    public void validate() {
        requireNonNull(config, "config must not be null");
    }

    public abstract void init();

    protected ApplicationEventPublisher getEventPublisher() {
        return getConfig().getEventPublisher();
    }

}

