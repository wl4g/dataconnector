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
import org.junit.Test;
import org.junit.jupiter.api.Assertions;
//import org.springframework.retry.RetryCallback;
//import org.springframework.retry.RetryPolicy;
//import org.springframework.retry.backoff.BackOffPolicyBuilder;
//import org.springframework.retry.policy.MaxAttemptsRetryPolicy;
//import org.springframework.retry.support.RetryTemplate;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * The {@link AtMostAttemptsQoSTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class AtMostAttemptsQoSTests {

    @Test
    public void testRetryBackoff() {
        final AtMostAttemptsQoS qos = new AtMostAttemptsQoS();
        qos.setRetries(1024);
        qos.setRetryBackoffMs(100);
        qos.setRetryMaxBackoffMs(10_000);
        qos.setRetryBackoffMultiplier(2.0d);

        final ConnectorConfig connectorConfig = ConnectorConfig
                .builder()
                .enable(true)
                .name("connector_1")
                .processChain(null)
                .processConfig(null)
                .qos(null)
                .checkpoint(null)
                .build();

        final List<Long> history = new ArrayList<>();

        final AtomicInteger retryTimes = new AtomicInteger(1);
        for (int i = 0; i < 5; i++) {
            qos.retryIfFail(connectorConfig, retryTimes.getAndIncrement(),
                    () -> history.add(System.currentTimeMillis()));
        }

        Assertions.assertEquals(6, retryTimes.get());
        Assertions.assertEquals(5, history.size());

        boolean isValid = true;
        for (int i = 1; i < history.size(); i++) {
            long previous = history.get(i - 1);
            long current = history.get(i);
            long increment = (long) ((i + 1) * qos.getRetryBackoffMs() * qos.getRetryBackoffMultiplier());
            long diff = current - previous;
            // allowed difference is 50ms
            if (diff > (increment + 50) || diff < (increment - 50)) {
                isValid = false;
                break;
            }
        }
        Assertions.assertTrue(isValid);
    }

    // private static RetryTemplate createRetryTemplate() throws Throwable {
    //     final RetryPolicy policy = new MaxAttemptsRetryPolicy(3);
    //     RetryTemplate retryTemplate = new RetryTemplate();
    //     retryTemplate.setBackOffPolicy(BackOffPolicyBuilder
    //             .newBuilder()
    //             .delay(1000)
    //             .maxDelay(5000)
    //             .multiplier(2.0)
    //             .build());
    //     retryTemplate.setRetryPolicy(policy);
    //     //retryTemplate.execute((RetryCallback<Object, Throwable>) context -> null);
    //     return retryTemplate;
    // }

}
