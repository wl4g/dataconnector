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
//package org.testcontainers.containers.wait.strategy;
//
//import com.github.dockerjava.api.command.LogContainerCmd;
//import lombok.SneakyThrows;
//import org.testcontainers.DockerClientFactory;
//import org.testcontainers.containers.ContainerLaunchException;
//import org.testcontainers.containers.output.FrameConsumerResultCallback;
//import org.testcontainers.containers.output.OutputFrame;
//import org.testcontainers.containers.output.WaitingConsumer;
//
//import java.io.IOException;
//import java.util.concurrent.TimeUnit;
//import java.util.concurrent.TimeoutException;
//import java.util.function.Predicate;
//
//import static org.testcontainers.containers.output.OutputFrame.OutputType.STDERR;
//import static org.testcontainers.containers.output.OutputFrame.OutputType.STDOUT;
//
//public class CustomLogMessageWaitStrategy extends AbstractWaitStrategy {
//
//    private String regEx;
//    private int times = 1;
//
//    @Override
//    @SneakyThrows(IOException.class)
//    protected void waitUntilReady() {
//        WaitingConsumer waitingConsumer = new WaitingConsumer();
//
//        LogContainerCmd cmd = DockerClientFactory.instance().client().logContainerCmd(waitStrategyTarget.getContainerId())
//                .withFollowStream(true)
//                .withSince(0)
//                .withStdOut(true)
//                .withStdErr(true);
//
//        FrameConsumerResultCallback callback = new FrameConsumerResultCallback();
//        try {
//            callback.addConsumer(STDOUT, waitingConsumer);
//            callback.addConsumer(STDERR, waitingConsumer);
//
//            cmd.exec(callback);
//
//            Predicate<OutputFrame> waitPredicate = outputFrame ->
//                    // [BEGIN] Fixed for remove regex prefix (?s)
//                    outputFrame.getUtf8String().matches(regEx);
//            // [END] Fixed for remove regex prefix (?s)
//
//            try {
//                waitingConsumer.waitUntil(waitPredicate, startupTimeout.getSeconds(), TimeUnit.SECONDS, times);
//            } catch (TimeoutException e) {
//                throw new ContainerLaunchException("Timed out waiting for log output matching '" + regEx + "'");
//            }
//        } finally {
//            callback.close();
//        }
//    }
//
//    public CustomLogMessageWaitStrategy withRegEx(String regEx) {
//        this.regEx = regEx;
//        return this;
//    }
//
//    public CustomLogMessageWaitStrategy withTimes(int times) {
//        this.times = times;
//        return this;
//    }
//}
