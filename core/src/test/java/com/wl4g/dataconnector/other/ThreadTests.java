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

package com.wl4g.dataconnector.other;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.locks.LockSupport;

/**
 * The {@link ThreadTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ThreadTests {

    //@Test
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

    //@Test
    public void testLockSupportPark() throws InterruptedException {
        CountDownLatch latch = new CountDownLatch(2);
        Thread thread1 = new Thread(() -> {
            int i = 0;
            for (; ; ) {
                System.out.println(System.currentTimeMillis() + " thread1:" + ++i);
//                LockSupport.parkNanos(Duration.ofSeconds(2).toNanos());
                try {
                    Thread.sleep(100L);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
//            latch.countDown();
        });
        Thread thread2 = new Thread(() -> {
            for (int i = 0; i < 5; i++) {
                System.out.println(System.currentTimeMillis() + " thread2:" + i);
//                LockSupport.parkNanos(Duration.ofSeconds(2).toNanos());
                LockSupport.parkNanos(LockSupport.getBlocker(thread1), Duration.ofSeconds(2).toNanos());
            }
            latch.countDown();
        });
        thread1.start();
//        thread2.start();

        System.out.println("111111111");
        LockSupport.parkNanos(thread1, Duration.ofSeconds(8).toNanos());
        System.out.println("222222222");
        LockSupport.parkNanos(thread1, Duration.ofSeconds(8).toNanos());
        System.out.println("3333333333");

        latch.await();
        System.out.println("main thread end");
    }

}
