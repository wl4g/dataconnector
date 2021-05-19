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

import com.wl4g.infra.common.serialize.JacksonUtils;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.Arrays;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;

/**
 * The {@link ExpressionOperatorTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class ExpressionOperatorTests {

    @Test
    public void testSimpleRelationOperator() {
        ExpressionOperator.RelationOperator filter = new ExpressionOperator.RelationOperator();
        filter.setName("testFilter");
        filter.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter.setFn(new AviatorFunction("a >= 1 && b <= 2"));
        Assertions.assertTrue(filter.apply(parseToNode("{\"a\":1,\"b\":2}")));
    }

    @Test
    public void testComplexLogicalOperator() {
        ExpressionOperator.RelationOperator filter1 = new ExpressionOperator.RelationOperator();
        filter1.setName("testFilter1");
        filter1.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter1.setFn(new AviatorFunction("a >= 1 && b <= 2"));

        ExpressionOperator.RelationOperator filter2 = new ExpressionOperator.RelationOperator();
        filter2.setName("testFilter2");
        filter2.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter2.setFn(new AviatorFunction("a >= 5"));

        ExpressionOperator.LogicalOperator filter3 = new ExpressionOperator.LogicalOperator();
        filter3.setName("testFilter3");
        filter3.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter3.setSubFilters(Arrays.asList(filter1, filter2));

        filter3.setLogical(ExpressionOperator.LogicalType.AND);
        Assertions.assertFalse(filter3.apply(parseToNode("{\"a\":1,\"b\":2}")));

        filter3.setLogical(ExpressionOperator.LogicalType.OR);
        Assertions.assertTrue(filter3.apply(parseToNode("{\"a\":1,\"b\":2}")));
    }

    @Test
    public void testSerialization() {
        ExpressionOperator.RelationOperator filter1 = new ExpressionOperator.RelationOperator();
        filter1.setName("testFilter1");
        filter1.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter1.setFn(new AviatorFunction("a >= 1 && b <= 2"));

        ExpressionOperator.RelationOperator filter2 = new ExpressionOperator.RelationOperator();
        filter2.setName("testFilter2");
        filter2.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter2.setFn(new AviatorFunction("a >= 5"));

        ExpressionOperator.LogicalOperator filter3 = new ExpressionOperator.LogicalOperator();
        filter3.setName("testFilter3");
        filter3.setType(ExpressionOperator.OperatorType.RELATION.name());
        filter3.setSubFilters(Arrays.asList(filter1, filter2));

        filter3.setLogical(ExpressionOperator.LogicalType.AND);

        String filter3Json = JacksonUtils.toJSONString(filter3);
        System.out.println(filter3Json);

        ExpressionOperator filter3Copy = JacksonUtils.parseJSON(filter3Json, ExpressionOperator.class);
        System.out.println(filter3Copy instanceof ExpressionOperator.LogicalOperator);
        System.out.println(filter3Copy);
    }

}
