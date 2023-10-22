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

package com.wl4g.dataconnector.qos;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * Max retries then give up if it fails.
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class AtMostAttemptsQoS extends AbstractQoS {

    public static final String TYPE_NAME = "AT_MOST_ATTEMPTS_QOS";

    private int retries = 1024;
    private long retryBackoffMs = 200;
    private long retryMaxBackoffMs = 60 * 1000; // default by 30s
    private double retryBackoffMultiplier = 1.5d;

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public void validate() {
        super.validate();
        Assert2.isTrueOf(retries > 0, "retries > 0");
        Assert2.isTrueOf(retryBackoffMs > 0, "retryBackoffMs > 0");
        Assert2.isTrueOf(retryMaxBackoffMs > 0, "retryMaxBackoffMs > 0");
    }

    @Override
    public boolean supportRetry(ConnectorConfig connectorConfig) {
        return true;
    }

    @Override
    public boolean canRetry(ConnectorConfig connectorConfig,
                            int retryTimes) {
        return retryTimes <= getRetries();
    }

}
