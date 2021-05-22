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

import com.wl4g.infra.common.serialize.JacksonUtils;
import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.dispatch.FilterBatchMessageDispatcher;
import com.wl4g.kafkasubscriber.util.expression.ExpressionOperator;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;

/**
 * The {@link DefaultRecordMatchSubscribeFilter}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class DefaultRecordMatchSubscribeFilter implements ISubscribeFilter {

    public static final String BEAN_NAME = "defaultRecordMatchSubscribeFilter";

    private ExpressionOperator operator;

    @Override
    public Boolean apply(FilterBatchMessageDispatcher.SubscriberRecord record) {
        if (Objects.isNull(operator)) {
            log.warn("- :: {} :: The configuration of the default record matching operator has not been injected!!! " +
                    "Please check if it is config item correctly at filterProps.{}", record.getSubscriber().getId(), BEAN_NAME);
            return false;
        }
        return operator.apply(record.getRecord().value());
    }

    @Override
    public DefaultRecordMatchSubscribeFilter updateConfigWithMergeSubscribers(List<SubscriberInfo> subscribers) {
        final ExpressionOperator.LogicalOperator rootOperator = new ExpressionOperator.LogicalOperator();
        rootOperator.setName("_ROOT_OPERATOR");
        rootOperator.setType(ExpressionOperator.OperatorType.LOGICAL.name());
        rootOperator.setLogical(ExpressionOperator.LogicalType.OR);

        // Merge to all subscriber filter conditions to root operator condition.
        final List<ExpressionOperator> subConditions = safeList(subscribers).stream().map(s -> {
            final String exprJson = s.getProperties().getProperty(BEAN_NAME);
            return JacksonUtils.parseJSON(exprJson, ExpressionOperator.class);
        }).collect(Collectors.toList());

        rootOperator.setSubConditions(subConditions);

        this.operator = rootOperator;
        return this;
    }

}
