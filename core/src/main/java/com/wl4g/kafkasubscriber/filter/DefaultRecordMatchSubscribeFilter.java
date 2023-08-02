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

package com.wl4g.kafkasubscriber.filter;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.serialize.JacksonUtils;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.util.expression.ExpressionOperator;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;

/**
 * The {@link DefaultRecordMatchSubscribeFilter}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class DefaultRecordMatchSubscribeFilter extends AbstractSubscribeFilter {

    public static final String TYPE_NAME = "EXPRESSION_MATCH";

    private final AtomicLong lastUpdateTime = new AtomicLong(0);

    private ExpressionOperator operator;

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    public boolean doMatch(SubscriberInfo subscriber,
                           ConsumerRecord<String, ObjectNode> record) {
        if (Objects.isNull(operator)) {
            log.warn("{} :: {} :: The configuration of the default record matching operator has not been injected!!! " +
                    "Please check if it is config item correctly at filterProps.", getName(), subscriber.getId());
            return false;
        }
        return operator.apply(record.value());
    }

    @Override
    public void updateConfigWithMergeSubscribers(List<SubscriberInfo> subscribers, long delayTime) {
        if (Math.abs(System.nanoTime()) - lastUpdateTime.get() > delayTime) {
            log.info("- :: {} :: Update default record matching operator config.", TYPE_NAME);
            lastUpdateTime.set(System.nanoTime());
            doUpdateConfigWithMergeSubscribers(subscribers);
        } else {
            log.info("- :: {} :: Skip update default record matching operator config.", TYPE_NAME);
        }
    }

    private void doUpdateConfigWithMergeSubscribers(List<SubscriberInfo> subscribers) {
        final ExpressionOperator.LogicalOperator rootOperator = new ExpressionOperator.LogicalOperator();
        rootOperator.setName("__ROOT_OPERATOR__");
        rootOperator.setType(ExpressionOperator.OperatorType.LOGICAL.name());
        rootOperator.setLogical(ExpressionOperator.LogicalType.OR);

        // Merge to all subscriber filter conditions to root operator condition.
        final List<ExpressionOperator> subConditions = safeList(subscribers).stream().map(s -> {
            final String exprJson = s.getSettings().getProperties().getProperty(getName());
            if (StringUtils.isBlank(exprJson)) {
                throw new IllegalArgumentException(String.format("Could not configuration item named '%s' was found from the subscriber '%s' properties",
                        getName(), s.getId()));
            }
            return JacksonUtils.parseJSON(exprJson, ExpressionOperator.class);
        }).collect(Collectors.toList());

        rootOperator.setSubConditions(subConditions);

        this.operator = rootOperator;
    }

}
