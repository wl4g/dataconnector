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
import com.wl4g.streamconnect.stream.AbstractStream.StreamContext;
import com.wl4g.streamconnect.util.expression.ExpressionOperator;
import com.wl4g.streamconnect.util.expression.ExpressionOperator.LogicalOperator;
import com.wl4g.streamconnect.util.expression.ExpressionOperator.LogicalType;
import com.wl4g.streamconnect.util.expression.ExpressionOperator.OperatorType;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;
import net.thisptr.jackson.jq.internal.functions.DelFunction;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.serialize.JacksonUtils.parseJSON;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

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

    @Getter
    private final ChannelInfo channel;
    @Getter
    private final StandardExprProcessFilterConfig processFilterConfig;

    private transient final ExpressionOperator operator;

    public StandardExprProcessFilter(@NotNull StreamContext context,
                                     @NotNull ChannelInfo channel,
                                     @NotNull StandardExprProcessFilterConfig processFilterConfig) {
        super(context);
        this.channel = requireNonNull(channel, "channel must not be null");
        this.processFilterConfig = requireNonNull(processFilterConfig, "processFilterConfig must not be null");

        final LogicalOperator rootOperator = new LogicalOperator();
        rootOperator.setName("__ROOT_OPERATOR__");
        rootOperator.setType(OperatorType.LOGICAL.name());
        rootOperator.setLogical(LogicalType.OR);
        // Merge to all channel filter conditions.
        final List<ExpressionOperator> subConditions = // Merge multiple conditions for each channel (granted policies, may is cross tenant)
                safeList(channel.getSettingsSpec().getPolicySpec().getRules())
                        .stream()
                        .map(r -> {
                            if (StringUtils.isBlank(r.getRecordFilter())) {
                                throw new IllegalArgumentException(String.format("Could not match record filter expression " +
                                        "was found from the channel '%s' properties", channel.getId()));
                            }
                            return parseJSON(r.getRecordFilter(), ExpressionOperator.class);
                        })
                        .filter(Objects::nonNull)
                        .collect(toList());
        rootOperator.setSubConditions(subConditions);
        this.operator = rootOperator;
    }

    @Override
    public boolean doFilter(MessageRecord<String, Object> record) {
        requireNonNull(operator, String.format("%s :: The configuration of the standard expression match operator has not been injected!",
                channel.getId()));
        if (record.getValue() instanceof JsonNode) {
            return operator.apply((JsonNode) record.getValue());
        } else {
            throw new UnsupportedOperationException(String.format("%s :: The type of the record value is not supported!",
                    channel.getId()));
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class StandardExprProcessFilterConfig extends ProcessFilterConfig {
        private @Builder.Default Map<String, String> registerScopes = new HashMap<String, String>() {
            {
                put("del", DelFunction.class.getName());
            }
        };
        private @Builder.Default int maxCacheSize = 128;
    }

    public static class StandardExprProcessFilterProvider extends ProcessFilterProvider {
        public static final String TYPE_NAME = "STANDARD_EXPR_FILTER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessFilter> T create(@NotNull StreamContext context,
                                                   @NotNull ProcessFilterConfig processFilterConfig,
                                                   @NotNull ChannelInfo channel) {
            return (T) new StandardExprProcessFilter(context, channel, (StandardExprProcessFilterConfig) processFilterConfig);
        }
    }

}
