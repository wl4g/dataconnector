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
import com.wl4g.kafkasubscriber.util.expression.ExpressionOperator.LogicalOperator;
import com.wl4g.kafkasubscriber.util.expression.ExpressionOperator.LogicalType;
import com.wl4g.kafkasubscriber.util.expression.ExpressionOperator.OperatorType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.util.Collection;
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
    public void updateMergeConditions(Collection<SubscriberInfo> subscribers) {
        if (Math.abs(System.nanoTime()) - lastUpdateTime.get() > getUpdateMergeConditionsDelayTime()) {
            log.debug("- :: {} :: Update default record matching operator config.", getName());
            lastUpdateTime.set(System.nanoTime());

            doUpdateMergeConditions(subscribers);
        } else {
            log.debug("- :: {} :: Skip update default record matching operator config.", getName());
        }
    }

    private void doUpdateMergeConditions(Collection<SubscriberInfo> subscribers) {
        final LogicalOperator rootOperator = new LogicalOperator();
        rootOperator.setName("__ROOT_OPERATOR__");
        rootOperator.setType(OperatorType.LOGICAL.name());
        rootOperator.setLogical(LogicalType.OR);

        // Merge to all subscriber filter conditions.
        final List<ExpressionOperator> subConditions = safeList(subscribers)
                .stream()
                .flatMap(s ->
                        // Merge multiple conditions for each subscriber (granted policies, may is cross tenant)
                        safeList(s.getRule().getPolicies())
                                .stream()
                                .map(sga -> {
                                    if (StringUtils.isBlank(sga.getRecordFilter())) {
                                        throw new IllegalArgumentException(String.format("Could not match expression filter " +
                                                "named '%s' was found from the subscriber '%s' properties", getName(), s.getId()));
                                    }
                                    return JacksonUtils.parseJSON(sga.getRecordFilter(), ExpressionOperator.class);
                                })
                                .filter(Objects::nonNull))
                .collect(Collectors.toList());

        rootOperator.setSubConditions(subConditions);

        this.operator = rootOperator;
    }

}
