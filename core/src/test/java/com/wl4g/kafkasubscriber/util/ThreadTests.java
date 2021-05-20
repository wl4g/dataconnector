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

package com.wl4g.kafkasubscriber.util;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;

/**
 * The {@link ThreadTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ThreadTests {

    @Test
    public void testThreadYield() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Thread thread = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                System.out.println(System.nanoTime() + " thread1:" + i);
                if (i % 10 == 0) {
                    Thread.yield();
                }
            }
            latch.countDown();
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 100; i++) {
                System.out.println(System.nanoTime() + " thread2:" + i);
                if (i % 10 == 0) {
                    Thread.yield();
                }
            }
            latch.countDown();
        });
        thread.start();
        thread2.start();

        latch.await();
        System.out.println("main thread end");
    }

}
