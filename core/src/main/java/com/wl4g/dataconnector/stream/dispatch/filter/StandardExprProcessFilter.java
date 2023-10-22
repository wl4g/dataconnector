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

package com.wl4g.dataconnector.stream.dispatch.filter;

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.infra.common.expression.aviator.AviatorFunction;
import com.wl4g.infra.common.expression.aviator.ExpressionOperator;
import com.wl4g.infra.common.expression.aviator.ExpressionOperator.OperatorType;
import com.wl4g.infra.common.expression.aviator.ExpressionOperator.RelationOperator;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.ChannelInfo.RuleItem;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.filter.StandardExprProcessFilter.StandardExprProcessFilterConfig;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

/**
 * The standard filter, support data permission filtering based on aviator
 * expression record.
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
public class StandardExprProcessFilter extends AbstractProcessFilter<StandardExprProcessFilterConfig> {
    private transient final ExpressionOperator operator;

    public StandardExprProcessFilter(@NotNull DataConnectorConfiguration config,
                                     @NotNull StandardExprProcessFilterConfig filterConfig,
                                     @NotNull String channelId,
                                     @NotNull RuleItem rule) {
        super(config, filterConfig, channelId, rule);

        assert !isBlank(rule.getValue()) :
                String.format("Could not create null filter expression operator for rule %s, channelId: '%s'",
                        rule.getName(), channelId);

        final RelationOperator rootOperator = new RelationOperator();
        rootOperator.setName("__ROOT_OPERATOR__");
        rootOperator.setType(OperatorType.RELATION.name());
        rootOperator.setFn(new AviatorFunction(rule.getValue()));
        this.operator = rootOperator;
    }

    @Override
    public boolean doFilter(MessageRecord<String, Object> record) {
        requireNonNull(operator, String.format("%s :: The configuration of the standard expression match operator has not been injected!",
                getChannelId()));
        if (record.getValue() instanceof JsonNode) {
            return operator.apply((JsonNode) record.getValue());
        } else {
            throw new UnsupportedOperationException(String.format("%s :: The type of the record value is not supported!",
                    getChannelId()));
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class StandardExprProcessFilterConfig extends IProcessFilter.ProcessFilterConfig {
        private @Default String expression = "false";
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class StandardExprProcessFilterProvider extends ProcessFilterProvider {
        public static final String TYPE_NAME = "STANDARD_EXPR_FILTER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessFilter> T create(@NotNull DataConnectorConfiguration config,
                                                   @NotNull ChannelInfo channel,
                                                   @NotNull RuleItem rule) {
            return (T) new StandardExprProcessFilter(config,
                    (StandardExprProcessFilterConfig) getFilterConfig(),
                    channel.getId(), rule);
        }
    }

}
