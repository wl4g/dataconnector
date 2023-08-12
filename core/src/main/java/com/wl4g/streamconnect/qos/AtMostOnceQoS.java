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

package com.wl4g.streamconnect.qos;

import com.wl4g.streamconnect.config.StreamConnectConfiguration.ConnectorConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * Execute only once and give up if it fails.
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class AtMostOnceQoS extends AbstractQoS {

    public static final String TYPE_NAME = "AT_MOST_ONCE_QOS";

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public boolean supportRetry(ConnectorConfig connectorConfig) {
        return false;
    }

    @Override
    public boolean canRetry(ConnectorConfig connectorConfig,
                            int retryTimes) {
        return false; // no need retry
    }

    @Override
    public void acknowledgeIfFail(ConnectorConfig connectorConfig,
                                  Throwable ex,
                                  Runnable call) {
        call.run();
    }

}
