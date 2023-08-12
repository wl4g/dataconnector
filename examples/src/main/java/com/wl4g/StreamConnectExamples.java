/*
 * Copyright 2017 ~ 2025 the original authors James Wong.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ALL_OR KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.wl4g;

import org.springframework.boot.ApplicationRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * The {@link StreamConnectExamples}
 *
 * @author James Wong
 * @since v1.0
 **/
@SpringBootApplication(scanBasePackages = {"com.wl4g.streamconnect"})
public class StreamConnectExamples {

    private static ConfigurableApplicationContext context;
    private static Runnable startedListener;

    public static void main(String[] args) {
        System.out.println("Starting for Integration Tests examples ...");
        SpringApplication.run(StreamConnectExamples.class, args);
    }

    public static void exit() {
        SpringApplication.exit(context);
    }

    public static ConfigurableApplicationContext getApplicationContext() {
        return context;
    }

    public static void setStartedListener(Runnable startedListener) {
        StreamConnectExamples.startedListener = startedListener;
    }

    @Configuration
    static class StreamConnectExamplesConfiguration {
        @Bean
        public ApplicationRunner applicationRunner(ConfigurableApplicationContext context) {
            StreamConnectExamples.context = context;
            return args -> {
                if (startedListener != null) {
                    startedListener.run();
                }
            };
        }
    }

}
