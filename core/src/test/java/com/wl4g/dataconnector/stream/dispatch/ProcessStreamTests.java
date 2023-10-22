///*
// *  Copyright (C) 2023 ~ 2035 the original authors WL4G (James Wong).
// *
// *  Licensed under the Apache License, Version 2.0 (the "License");
// *  you may not use this file except in compliance with the License.
// *  You may obtain a copy of the License at
// *
// *        http://www.apache.org/licenses/LICENSE-2.0
// *
// *  Unless required by applicable law or agreed to in writing, software
// *  distributed under the License is distributed on an "AS IS" BASIS,
// *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// *  See the License for the specific language governing permissions and
// *  limitations under the License.
// *
// */
//
//package com.wl4g.dataconnector.stream.process;
//
//import com.wl4g.dataconnector.DataConnectorMockerSetup;
//import com.wl4g.dataconnector.config.ChannelInfo;
//import com.wl4g.dataconnector.config.DataConnectorConfiguration;
//import com.wl4g.dataconnector.config.configurator.IDataConnectorConfigurator;
//import com.wl4g.dataconnector.stream.AbstractStream;
//import com.wl4g.dataconnector.stream.AbstractStream.StreamContext;
//import com.wl4g.dataconnector.stream.source.SourceStream;
//import org.junit.jupiter.api.Assertions;
//import org.junit.jupiter.api.Test;
//
//import java.util.ArrayList;
//import java.util.List;
//
//import static java.util.Arrays.asList;
//import static java.util.Collections.singletonList;
//
///**
// * The {@link ProcessStreamTests}
// *
// * @author James Wong
// * @since v1.0
// **/
//public class ProcessStreamTests {
//
//    //@Test
//    public void testSourceAndProccessStreaming() {
//        List<AbstractStream.MessageRecord<String, Object>> mockRecords = new ArrayList<>();
//        mockRecords.add(DataConnectorMockerSetup.buildDefaultMockMessageRecord("10001", "t1001"));
//        mockRecords.add(DataConnectorMockerSetup.buildDefaultMockMessageRecord("10002", "t1001"));
//        mockRecords.add(DataConnectorMockerSetup.buildDefaultMockMessageRecord("10003", "t1002"));
//        mockRecords.add(DataConnectorMockerSetup.buildDefaultMockMessageRecord("10004", "t1003"));
//
//        List<ChannelInfo> mockAssignedChannels = new ArrayList<>();
//        mockAssignedChannels.add(DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1001", "t1001", singletonList("t1001")));
//        mockAssignedChannels.add(DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1002", "t1002", asList("t1001", "t1002")));
//        mockAssignedChannels.add(DataConnectorMockerSetup.buildDefaultMockChannelInfo("c1003", "t1003", singletonList("t1003")));
//
//        StreamContext mockContext = DataConnectorMockerSetup.buildDefaultMockStreamContext();
//
//        SourceStream mockSourceStream = new SourceStream(mockContext) {
//            @Override
//            public SourceStreamConfig getSourceStreamConfig() {
//                throw new UnsupportedOperationException();
//            }
//
//            @Override
//            protected Object getInternalTask() {
//                throw new UnsupportedOperationException();
//            }
//        };
//
//        //List<ChannelRecord> mockChannelRecords = new ProcessStream(mockContext, mockSourceStream)
//        //        .doMatchToChannelRecords(mockConfigurator, mockAssignedChannels, mockRecords);
//
//        List<ChannelRecord> mockChannelRecords = ProcessStream.doMatchToChannelRecords(
//                mockConfigurator, mockConnectorConfig, mockAssignedChannels, mockRecords);
//
//        Assertions.assertEquals(6, mockChannelRecords.size());
//    }
//
//    @Test
//    public void testProcessStreamDoMatchToChannelRecords() {
//        List<AbstractStream.MessageRecord<String, Object>> mockRecords = new ArrayList<>();
//        mockRecords.add(buildMockMessageRecord("10001", "t1001"));
//        mockRecords.add(buildMockMessageRecord("10002", "t1001"));
//        mockRecords.add(buildMockMessageRecord("10003", "t1002"));
//        mockRecords.add(buildMockMessageRecord("10004", "t1003"));
//
//        List<ChannelInfo> mockAssignedChannels = new ArrayList<>();
//        mockAssignedChannels.add(buildMockChannelInfo("c1001", "t1001", singletonList("t1001")));
//        mockAssignedChannels.add(buildMockChannelInfo("c1002", "t1002", asList("t1001", "t1002")));
//        mockAssignedChannels.add(buildMockChannelInfo("c1003", "t1003", singletonList("t1003")));
//
//        IDataConnectorConfigurator mockConfigurator = buildMockDataConnectorConfigurator();
//        DataConnectorConfiguration.ConnectorConfig mockConnectorConfig = new DataConnectorConfiguration.ConnectorConfig();
//        mockConnectorConfig.setName("connector_1");
//
//        List<ChannelRecord> mockChannelRecords = ProcessStream.doMatchToChannelRecords(
//                mockConfigurator, mockConnectorConfig, mockAssignedChannels, mockRecords);
//
//        Assertions.assertEquals(6, mockChannelRecords.size());
//    }
//
//}
