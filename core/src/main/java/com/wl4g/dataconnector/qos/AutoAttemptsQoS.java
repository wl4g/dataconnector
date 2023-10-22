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

import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import lombok.Getter;
import lombok.Setter;

/**
 * If the failure still occurs after the maximum retries, the first consecutive
 * successful part in the offset order of this batch will be submitted, and the other
 * failed offsets will be abandoned and submitted until the next re-consumption.
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class AutoAttemptsQoS extends AtMostAttemptsQoS {

    public static final String TYPE_NAME = "AUTO_ATTEMPTS_QOS";

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public boolean supportPreferAcknowledge(ConnectorConfig connectorConfig) {
        return true;
    }

}
