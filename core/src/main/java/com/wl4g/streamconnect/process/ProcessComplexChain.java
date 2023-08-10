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
//package com.wl4g.streamconnect.process;
//
//import com.fasterxml.jackson.databind.node.ObjectNode;
//import com.wl4g.infra.common.lang.Assert2;
//import com.wl4g.streamconnect.bean.SubscriberInfo;
//import com.wl4g.streamconnect.process.filter.ProcessFilterChain;
//import com.wl4g.streamconnect.process.map.ProcessMapperChain;
//import lombok.Getter;
//import org.apache.kafka.clients.consumer.ConsumerRecord;
//
///**
// * The {@link ProcessComplexChain}
// *
// * @author James Wong
// * @since v1.0
// **/
//@Getter
//public class ProcessComplexChain {
//
//    private final ProcessFilterChain filterChain;
//    private final ProcessMapperChain mapperChain;
//
//    public ProcessComplexChain(ProcessFilterChain filterChain,
//                               ProcessMapperChain mapperChain) {
//        this.filterChain = Assert2.notNullOf(filterChain, "filterChain");
//        this.mapperChain = Assert2.notNullOf(mapperChain, "mapperChain");
//    }
//
//    public  ConsumerRecord<String, ObjectNode>  doProcess(SubscriberInfo subscriber,
//                             ConsumerRecord<String, ObjectNode> record) {
//
//    }
//
//}
