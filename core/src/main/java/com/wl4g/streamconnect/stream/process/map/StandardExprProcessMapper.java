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

package com.wl4g.streamconnect.stream.process.map;

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.ChannelInfo.RuleSpec;
import com.wl4g.streamconnect.stream.AbstractStream.DelegateMessageRecord;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
import com.wl4g.streamconnect.stream.AbstractStream.StreamContext;
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

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.reflect.ObjectInstantiators.newInstance;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The standard JQ expression mapper, for example: Using this mapper to modify
 * data permissions that support filtering by each record fields.
 *
 * @author James Wong
 * @since v1.0
 **/
public class StandardExprProcessMapper extends AbstractProcessMapper {
    @Getter
    private final ChannelInfo channel;
    @Getter
    private final StandardExprProcessMapperConfig processMapperConfig;

    private transient final ThreadLocal<Scope> jqScopeLocal;
    private transient final Map<String, JsonQuery> jqCached;

    public StandardExprProcessMapper(@NotNull StreamContext context,
                                     @NotNull ChannelInfo channel,
                                     @NotNull StandardExprProcessMapperConfig processMapperConfig) {
        super(context);
        this.channel = requireNonNull(channel, "channel must not be null");
        this.processMapperConfig = requireNonNull(processMapperConfig, "processMapperConfig must not be null");

        this.jqScopeLocal = ThreadLocal.withInitial(() -> {
            final Scope newScope = Scope.newEmptyScope();
            safeMap(processMapperConfig.getRegisterScopes()).forEach((k, v) -> {
                try {
                    newScope.addFunction(k.concat("/1"), (Function) newInstance(ClassUtils.getClass(v)));
                } catch (Throwable ex) {
                    throw new StreamConnectException(String.format("Failed to register JQ " +
                            "function for %s -> %s", k, v), ex);
                }
            });
            return newScope;
        });

        this.jqCached = safeList(channel.getSettingsSpec().getPolicySpec().getRules())
                .stream()
                .filter(p -> isNotBlank(p.getFieldFilter()))
                .collect(toMap(RuleSpec::getRuleId, p -> {
                    try {
                        return JsonQuery.compile(p.getFieldFilter());
                    } catch (Throwable ex) {
                        throw new StreamConnectException(String.format("Failed to compile JQ expr : %s",
                                p.getFieldFilter()), ex);
                    }
                }));
    }

    @Override
    public MessageRecord<String, Object> doMap(MessageRecord<String, Object> record) {
        Object mappedValue = record.getValue();
        if (mappedValue instanceof JsonNode) {
            JsonNode mv = (JsonNode) mappedValue;
            for (Map.Entry<String, JsonQuery> entry : jqCached.entrySet()) {
                try {
                    mappedValue = mv = entry.getValue().apply(jqScopeLocal.get(), mv).get(0);
                } catch (Throwable ex) {
                    throw new StreamConnectException(String.format("Failed to invoke JQ expr of rule: %s, channelId : %s",
                            entry.getKey(), channel.getId()), ex);
                }
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
    public static class StandardExprProcessMapperConfig extends ProcessMapperConfig {
        private @Default Map<String, String> registerScopes = new HashMap<String, String>() {
            {
                put("del", DelFunction.class.getName());
            }
        };
        private @Default int maxCacheSize = 128;
    }

    public static class StandardExprProcessMapperProvider extends ProcessMapperProvider {
        public static final String TYPE_NAME = "STANDARD_EXPR_MAPPER";

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @SuppressWarnings("unchecked")
        @Override
        public <T extends IProcessMapper> T create(@NotNull StreamContext context,
                                                   @NotNull ProcessMapperConfig processMapperConfig,
                                                   @NotNull ChannelInfo channel) {
            return (T) new StandardExprProcessMapper(context, channel, (StandardExprProcessMapperConfig) processMapperConfig);
        }
    }

}
