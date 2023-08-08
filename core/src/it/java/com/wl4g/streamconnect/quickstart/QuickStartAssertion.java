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

package com.wl4g.streamconnect.quickstart;

import com.wl4g.streamconnect.base.BasedMockAssertion;
import lombok.AllArgsConstructor;
import org.springframework.stereotype.Component;

import java.util.List;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;

/**
 * The {@link QuickStartAssertion}
 *
 * @author James Wong
 * @since v1.0
 **/
@Component
@AllArgsConstructor
public class QuickStartAssertion extends BasedMockAssertion {

    List<QuickStartMockInitializer> initializers;

    @Override
    public void run() {
        doAssertion();
    }

    public void doAssertion() {
        //safeList(initializers).parallelStream().forEach(initializer -> {
        //    final Properties props = new Properties();
        //    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, initializer.getBrokerServers());
        //    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //    try (Consumer<String, String> consumer = new KafkaConsumer<>(props)) {
        //        boolean running = true;
        //        while (running) {
        //            final ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(2));
        //            // TODO
        //        }
        //    }
        //});
    }

}
