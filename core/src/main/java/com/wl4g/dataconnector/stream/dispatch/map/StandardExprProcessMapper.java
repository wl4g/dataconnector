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

package com.wl4g.dataconnector.stream.dispatch.map;

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.dataconnector.config.ChannelInfo;
import com.wl4g.dataconnector.config.ChannelInfo.RuleItem;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.stream.AbstractStream.DelegateMessageRecord;
import com.wl4g.dataconnector.stream.AbstractStream.MessageRecord;
import com.wl4g.dataconnector.stream.dispatch.map.StandardExprProcessMapper.StandardExprProcessMapperConfig;
import lombok.AllArgsConstructor;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import net.thisptr.jackson.jq.Function;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.internal.functions.DelFunction;
import org.apache.commons.lang3.ClassUtils;

import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.reflect.ObjectInstantiators.newInstance;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.trimToEmpty;

/**
 * The standard JQ expression mapper, for example: Using this mapper to modify
 * data permissions that support filtering by each record fields.
 *
 * @author James Wong
 * @since v1.0
 **/
public class StandardExprProcessMapper extends AbstractProcessMapper<StandardExprProcessMapperConfig> {

    private transient final ThreadLocal<Scope> jqScopeLocal;
    private transient final JsonQuery jqCached;

    public StandardExprProcessMapper(@NotNull DataConnectorConfiguration config,
                                     @NotNull StandardExprProcessMapperConfig mapperConfig,
                                     @NotNull String channelId,
                                     @NotNull RuleItem rule) {
        super(config, mapperConfig, channelId, rule);

        // Register JQ scopes.
        this.jqScopeLocal = ThreadLocal.withInitial(() -> {
            final Scope newScope = Scope.newEmptyScope();
            safeMap(mapperConfig.getRegisterScopes()).forEach((k, v) -> {
                try {
                    newScope.addFunction(k.concat("/1"), (Function) newInstance(ClassUtils.getClass(v)));
                } catch (Exception ex) {
                    throw new DataConnectorException(String.format("Failed to register JQ " +
                            "function for %s -> %s", k, v), ex);
                }
            });
            return newScope;
        });

        // Compile JQ expression.
        final String expr = isBlank(rule.getValue()) ? mapperConfig.getExpression() : rule.getValue();
        try {
            this.jqCached = JsonQuery.compile(trimToEmpty(expr));
        } catch (Exception ex) {
            throw new DataConnectorException(String.format("Failed to compile JQ expr for rule mapper : %s, channelId: %s",
                    rule.getName(), channelId), ex);
        }
    }

    @Override
    public MessageRecord<String, Object> doMap(MapperContext context) {
        final MessageRecord<String, Object> record = context.getRecord();
        Object mappedValue = record.getValue();
        if (mappedValue instanceof JsonNode) {
            try {
                // Notice: If there is another chain, it must be cloned to avoid affecting the
                // execution of the next chain/mapper.
                if (context.isHasNext()) {
                    mappedValue = ((JsonNode) mappedValue).deepCopy();
                }
                mappedValue = jqCached.apply(jqScopeLocal.get(), (JsonNode) mappedValue).get(0);
            } catch (Exception ex) {
                throw new DataConnectorException(String.format("Failed to invoke JQ expr of rule: %s, channelId : %s",
                        getRule().getName(), getChannelId()), ex);
            }
        }
        return new MappedMessageRecord<>(record, mappedValue);
    }

    @Getter
    @Setter
    @SuperBuilder
    @AllArgsConstructor
    public static class MappedMessageRecord<K, V> implements DelegateMessageRecord<K, V> {
        private final MessageRecord<K, V> original;
        private final V mappedValue;

        @Override
        public Map<String, V> getMetadata() {
            return original.getMetadata();
        }

        @Override
        public K getKey() {
            return original.getKey();
        }

        @Override
        public V getValue() {
            return mappedValue;
        }

        @Override
        public long getTimestamp() {
            return original.getTimestamp();
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class StandardExprProcessMapperConfig extends IProcessMapper.ProcessMapperConfig {
        private @Default Map<String, String> registerScopes = new HashMap<String, String>() {
            {
                put("del", DelFunction.class.getName());
            }
        };
        private @Default int maxCacheSize = 128;
        private @Default String expression = "";
    }

    @Getter
    @Setter
    @NoArgsConstructor
    public static class StandardExprProcessMapperProvider extends ProcessMapperProvider {
        public static final String TYPE_NAME = "STANDARD_EXPR_MAPPER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessMapper> T create(@NotNull DataConnectorConfiguration config,
                                                   @NotNull ChannelInfo channel,
                                                   @NotNull RuleItem rule) {
            return (T) new StandardExprProcessMapper(config,
                    (StandardExprProcessMapperConfig) getMapperConfig(),
                    channel.getId(),
                    rule);
        }
    }

}
