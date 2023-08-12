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

package com.wl4g.streamconnect.stream.process.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.util.expression.ExpressionOperator;
import com.wl4g.streamconnect.util.expression.ExpressionOperator.LogicalOperator;
import com.wl4g.streamconnect.util.expression.ExpressionOperator.LogicalType;
import com.wl4g.streamconnect.util.expression.ExpressionOperator.OperatorType;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.util.Collection;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseJSON;
import static java.util.Objects.requireNonNull;

/**
 * The standard filter, support data permission filtering based on aviator
 * expression record.
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class StandardExprProcessFilter extends AbstractProcessFilter {

    public static final String TYPE_NAME = "STANDARD_EXPR_FILTER";

    private ExpressionOperator operator;

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    protected void doUpdateMergeConditions(Collection<ChannelInfo> channels) {
        final LogicalOperator rootOperator = new LogicalOperator();
        rootOperator.setName("__ROOT_OPERATOR__");
        rootOperator.setType(OperatorType.LOGICAL.name());
        rootOperator.setLogical(LogicalType.OR);

        // Merge to all channel filter conditions.
        final List<ExpressionOperator> subConditions = safeList(channels)
                .stream()
                .flatMap(s ->
                        // Merge multiple conditions for each channel (granted policies, may is cross tenant)
                        safeList(s.getSettingsSpec().getPolicySpec().getRules())
                                .stream()
                                .map(sga -> {
                                    if (StringUtils.isBlank(sga.getRecordFilter())) {
                                        throw new IllegalArgumentException(String.format("Could not match expression filter " +
                                                "named '%s' was found from the channel '%s' properties", getName(), s.getId()));
                                    }
                                    return parseJSON(sga.getRecordFilter(), ExpressionOperator.class);
                                })
                                .filter(Objects::nonNull))
                .collect(Collectors.toList());

        rootOperator.setSubConditions(subConditions);

        this.operator = rootOperator;
    }

    @Override
    public boolean doFilter(ChannelInfo channel,
                            MessageRecord<String, Object> record) {
        requireNonNull(operator, String.format("%s :: %s :: The configuration of the standard expression match operator has not been injected !",
                getName(), channel.getId()));
        if (record.getValue() instanceof JsonNode) {
            // TODO BUG，不同channel的rules会混乱:: 要么改为 operatorMap，要么每个channel对应一个operator（最好把Configurator#matchToChannelRecord合并到此，即每个channel对应一个chain对象）
            return operator.apply((JsonNode) record.getValue());
        } else {
            throw new UnsupportedOperationException(String.format("%s :: %s :: The type of the record value is not supported!",
                    getName(), channel.getId()));
        }
    }

}
