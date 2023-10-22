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

package com.wl4g.dataconnector.util;

import com.wl4g.dataconnector.util.assign.Assignments;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Executor;

import static java.lang.System.out;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;

/**
 * The {@link AssignmentsTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class AssignmentsTests {

    @Test
    public void testAssignment() {
        final List<Executor> executors = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            final int index = i;
            executors.add(command -> {
                //out.println("executor " + index + " ...");
                command.run();
            });
        }
        final List<Integer> assignments = new ArrayList<>();
        for (int i = 0; i < 1_000_000; i++) {
            final String msgKey = randomAlphabetic(16);
            final int index = Assignments.getInstance().assign(msgKey, executors.size());
            executors.get(index).execute(() -> {
                //out.println("msgKey: " + msgKey + " of executor " + index);
            });
            assignments.add(index);
        }
        assignments.stream().collect(groupingBy(a -> a, counting()))
                .forEach((value, count) ->
                        out.println("Assigned Index: " + value + ", Count: " + count));
    }

}
