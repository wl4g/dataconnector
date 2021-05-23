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

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.IntNode;
import com.fasterxml.jackson.databind.node.TextNode;
import com.googlecode.aviator.AviatorEvaluator;
import com.googlecode.aviator.Expression;
import com.googlecode.aviator.exception.CompileExpressionErrorException;
import com.googlecode.aviator.exception.ExpressionSyntaxErrorException;
import com.wl4g.infra.common.collection.CollectionUtils2;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static com.wl4g.infra.common.lang.Assert2.hasTextOf;
import static com.wl4g.infra.common.lang.Assert2.notNullOf;
import static java.lang.String.format;
import static java.util.Objects.nonNull;
import static java.util.Objects.isNull;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.startsWith;
import static org.apache.commons.lang3.StringUtils.replaceChars;

/**
 * The {@link AviatorFunction}
 * Condition that accepts aviator expression.
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
public class AviatorFunction implements Function<JsonNode, Boolean> {
    private final String expression;
    private @JsonIgnore Expression compiledExpression;

    @JsonCreator
    public AviatorFunction(@JsonProperty("expression") String expression) {
        this(expression, null);
    }

    public AviatorFunction(String expression, @Nullable String filterField) {
        this.expression = isBlank(filterField) ? requireNonNull(expression) :
                filterField + requireNonNull(expression);
        checkExpression(this.expression);
    }

    public Expression getCompiledExpression() {
        if (isNull(compiledExpression)) {
            synchronized (this) {
                if (isNull(compiledExpression)) {
                    this.compiledExpression =
                            notNullOf(AviatorEvaluator.compile(expression, false), "compiledExpression");
                }
            }
        }
        return compiledExpression;
    }

    @Override
    public Boolean apply(JsonNode record) {
        final List<String> variableNames = getCompiledExpression().getVariableFullNames();
        if (CollectionUtils2.isEmpty(variableNames)) {
            return true;
        }

        final Map<String, Object> variables = new HashMap<>();
        for (String variableName : variableNames) {
            final Object value = extractWithExprPath(record, ".".concat(variableName));
            if (nonNull(value)) {
                variables.put(variableName, value);
            } else {
                final String errmsg =
                        format("Unable to get path expr value '%s' from event: %s", variableName, record);
                log.warn(errmsg);
                // throw new IllegalArgumentException(errmsg);
            }
        }

        return (Boolean) getCompiledExpression().execute(variables);
    }

    private static void checkExpression(String expression) {
        try {
            AviatorEvaluator.validate(expression);
        } catch (ExpressionSyntaxErrorException | CompileExpressionErrorException e) {
            throw new IllegalArgumentException(
                    "The expression of AviatorCondition is invalid: " + e.getMessage());
        }
    }

    private static Object extractWithExprPath(@NotNull JsonNode record, @NotBlank String jqExpr) {
        hasTextOf(jqExpr, "jqExpr");
        if (!startsWith(jqExpr, ".")) {
            // @formatter:off
            // throw new IllegalArgumentException(format("Invalid json jq expression '%s', must start with '.'", jqExpr));
            // @formatter:on
            jqExpr = ".".concat(jqExpr);
        }

        String expr = replaceChars(jqExpr, ".", "/");
        int arrayIndex = -1;
        final int start = expr.indexOf("[");
        final int end = expr.indexOf("]");
        if (start >= 0 && start < end) {
            arrayIndex = Integer.parseInt(expr.substring(start + 1, end));
            expr = expr.substring(0, start);
        }

        final JsonNode value = record.at(expr);
        if (value instanceof ArrayNode) {
            if (arrayIndex >= 0) {
                final JsonNode arrayValue = ((ArrayNode) value).get(arrayIndex);
                return nonNull(arrayValue) ? convertValue(arrayValue) : null;
            } else {
                return convertValue(value);
            }
        }
        return convertValue(value);
    }

    private static Object convertValue(JsonNode value) {
        if (value instanceof TextNode) {
            return value.textValue();
        } else if (value instanceof IntNode) {
            return value.intValue();
        } else if (value.isBoolean()) {
            return value.booleanValue();
        } else if (value instanceof ArrayNode) {
            return ((ArrayNode) value);
        }
        return value;
    }

}