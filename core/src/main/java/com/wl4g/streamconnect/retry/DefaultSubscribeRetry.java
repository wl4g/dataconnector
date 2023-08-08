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
//package com.wl4g.streamconnect.retry;
//
//import com.wl4g.infra.common.lang.Assert2;
//import com.wl4g.streamconnect.config.SubscribeConfiguration.CheckpointQoS;
//import lombok.Getter;
//import lombok.Setter;
//import org.apache.commons.lang3.StringUtils;
//
///**
// * The {@link DefaultSubscribeRetry}
// *
// * @author James Wong
// * @since v1.0
// **/
//@Getter
//@Setter
//public class DefaultSubscribeRetry extends AbstractSubscribeRetry {
//
//    public static final String TYPE_NAME = "DEFAULT";
//
//    // TODO custom yaml with tag not support ???
//    //private CheckpointQoS qos = CheckpointQoS.RETRIES_AT_MOST;
//    private String qosType = CheckpointQoS.RETRIES_AT_MOST.name();
//    private int qoSMaxRetries = 5;
//    private long qoSMaxRetriesTimeout = 1800_000; // 30min
//
//    @Override
//    public String getType() {
//        return TYPE_NAME;
//    }
//
//    @Override
//    public void validate() {
//        super.validate();
//        Assert2.notNullOf(qosType, "qos");
//        Assert2.isTrueOf(qoSMaxRetries > 0, "qosMaxRetries > 0");
//        Assert2.isTrueOf(qoSMaxRetriesTimeout > 0, "checkpointTimeoutMs > 0");
//    }
//
//    public CheckpointQoS getQos() {
//        if (StringUtils.isNumeric(qosType)) {
//            return CheckpointQoS.values()[Integer.parseInt(qosType)];
//        }
//        return CheckpointQoS.valueOf(qosType);
//    }
//
//}
