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

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.dataconnector.DataConnectorMockerSetup;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.filter.TrueProcessFilter.TrueProcessFilterConfig;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;

/**
 * The {@link TrueProcessFilterTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class TrueProcessFilterTests {

    @Test
    public void testSimpleDoFilter() {
        final ChannelInfo mockChannel = DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1001", "connector_1",
                "t > 1690345000000 && props.online == true");

        final TrueProcessFilter mockFilter = buildDefaultMockTrueProcessFilter(mockChannel);

        final MessageRecord<String, Object> mockRecord = new MessageRecord<String, Object>() {
            @Override
            public String getKey() {
                return "myKey";
            }

            @Override
            public ObjectNode getValue() {
                return (ObjectNode) parseToNode("{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}");
            }

            @Override
            public long getTimestamp() {
                return 0;
            }
        };

        Assertions.assertTrue(mockFilter.doFilter(mockRecord));
    }

    public static TrueProcessFilter buildDefaultMockTrueProcessFilter(ChannelInfo channel) {
        final DataConnectorConfiguration mockConfig = DataConnectorMockerSetup.buildDefaultDataConnectorConfiguration();
        final TrueProcessFilterConfig filterConfig = new TrueProcessFilterConfig();
        return new TrueProcessFilter(mockConfig, filterConfig, channel.getId(),
                channel.getSettingsSpec().getPolicySpec().getRules().get(0).getChain().get(0));
    }

}
