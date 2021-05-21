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

package com.wl4g.kafkasubscriber.util.expression;

import com.fasterxml.jackson.databind.JsonNode;
import org.openjdk.jmh.annotations.*;
import org.openjdk.jmh.infra.Blackhole;

import java.util.Arrays;
import java.util.concurrent.TimeUnit;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;

/**
 * The {@link ExpressionOperatorBenchmark}
 *
 * @author James Wong
 * @since v1.0
 **/
@State(Scope.Benchmark)
@Threads(Threads.MAX)
@CompilerControl(CompilerControl.Mode.DONT_INLINE)
public class ExpressionOperatorBenchmark {

    public static final JsonNode inputJson = parseToNode("{\"a\":1,\"b\":2,\"u\":{\"wealth\":{\"money\":499999},\"age\":25,\"country\":\"CN\"}}");

    public static final ExpressionOperator.LogicalOperator condition5 = buildBenchmarkExpressionOperator();

    // ------------------------------------------------------------------------------------
    //            testCondition5(OR)
    //            /                \
    //     testCondition3(AND)      testCondition4
    //    /                 \                    \
    // testCondition1        testCondition2       u.country == 'US' || u.country == 'CN'
    //        |                   |
    //   a >= 1 && b <= 2    u.age >= 25 && u.wealth.money >= 500000
    // ------------------------------------------------------------------------------------
    // OUTPUT OPERATOR:
    // {"type":"LOGICAL","name":"testCondition5","logical":"OR","subConditions":[{"type":"LOGICAL","name":"testCondition3","logical":"AND",
    // "subConditions":[{"type":"RELATION","name":"testCondition1","fn":{"expression":"a >= 1 && b <= 2"}},{"type":"RELATION","name":"testCondition2",
    // "fn":{"expression":"u.age >= 25 && u.wealth.money >= 500000"}}]},{"type":"RELATION","name":"testCondition4","fn":{"expression":"u.country == 'US' || u.country == 'CN'"}}]}
    public static ExpressionOperator.LogicalOperator buildBenchmarkExpressionOperator() {
        ExpressionOperator.RelationOperator condition1 = new ExpressionOperator.RelationOperator();
        condition1.setName("testCondition1");
        condition1.setType(ExpressionOperator.OperatorType.RELATION.name());
        condition1.setFn(new AviatorFunction("a >= 1 && b <= 2"));

        ExpressionOperator.RelationOperator condition2 = new ExpressionOperator.RelationOperator();
        condition2.setName("testCondition2");
        condition2.setType(ExpressionOperator.OperatorType.RELATION.name());
        condition2.setFn(new AviatorFunction("u.age >= 25 && u.wealth.money >= 500000"));

        ExpressionOperator.LogicalOperator condition3 = new ExpressionOperator.LogicalOperator();
        condition3.setName("testCondition3");
        condition3.setType(ExpressionOperator.OperatorType.RELATION.name());
        condition3.setLogical(ExpressionOperator.LogicalType.AND);
        condition3.setSubConditions(Arrays.asList(condition1, condition2));

        ExpressionOperator.RelationOperator condition4 = new ExpressionOperator.RelationOperator();
        condition4.setName("testCondition4");
        condition4.setType(ExpressionOperator.OperatorType.RELATION.name());
        condition4.setFn(new AviatorFunction("u.country == 'US' || u.country == 'CN'"));

        ExpressionOperator.LogicalOperator condition5 = new ExpressionOperator.LogicalOperator();
        condition5.setName("testCondition5");
        condition5.setType(ExpressionOperator.OperatorType.RELATION.name());
        condition5.setLogical(ExpressionOperator.LogicalType.OR);
        condition5.setSubConditions(Arrays.asList(condition3, condition4));

        return condition5;
    }

    @Benchmark
    @BenchmarkMode(Mode.Throughput)
    public void measureThroughput(Blackhole bh) {
        bh.consume(condition5.apply(inputJson));
    }

    @Benchmark
    @BenchmarkMode(Mode.AverageTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureAvgTime(Blackhole bh) {
        bh.consume(condition5.apply(inputJson));
    }

    @Benchmark
    @BenchmarkMode(Mode.SampleTime)
    @OutputTimeUnit(TimeUnit.MILLISECONDS)
    public void measureSamples(Blackhole bh) {
        bh.consume(condition5.apply(inputJson));
    }

}
