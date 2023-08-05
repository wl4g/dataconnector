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

package com.wl4g.kafkasubscriber.quickstart;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.List;

/**
 * The {@link QuickStartApplicationIT}
 *
 * @author James Wong
 * @since v1.0
 **/
@Configuration
@SpringBootApplication(scanBasePackages = {"com.wl4g.kafkasubscriber"})
public class QuickStartApplicationIT {

    public static void main(String[] args) {
        System.out.println("Integration test application starting ...");
        SpringApplication.run(QuickStartApplicationIT.class, args);
    }

    @Bean
    public QuickStartMockInitializer quickStartTopicInitializer_with_broker01() {
        return new QuickStartMockInitializer(
                Boolean.parseBoolean(System.getenv().getOrDefault("IT_KAFKA_EMBEDDED_ENABLE", "true")),
                System.getenv().getOrDefault("IT_KAFKA_SERVERS_01", "localhost:9092"),
                System.getenv().getOrDefault("IT_KAFKA_INPUT_TOPIC", "shared_input"),
                1, 3
        );
    }

    @Bean
    public QuickStartMockInitializer quickStartTopicInitializer_with_broker02() {
        return new QuickStartMockInitializer(
                Boolean.parseBoolean(System.getenv().getOrDefault("IT_KAFKA_EMBEDDED_ENABLE", "true")),
                System.getenv().getOrDefault("IT_KAFKA_SERVERS_02", "localhost:9092"),
                System.getenv().getOrDefault("IT_KAFKA_INPUT_TOPIC", "t1001_input"),
                3, 4
        );
    }

    @Bean
    public QuickStartAssertion quickStartAssertionRunner(List<QuickStartMockInitializer> initializers) {
        return new QuickStartAssertion(initializers);
    }

}
