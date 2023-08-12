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

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.streamconnect.config.ChannelInfo;
import com.wl4g.streamconnect.config.ChannelInfo.RuleSpec;
import com.wl4g.streamconnect.config.configurator.IStreamConnectConfigurator;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.stream.AbstractStream.DelegateMessageRecord;
import com.wl4g.streamconnect.stream.AbstractStream.MessageRecord;
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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static com.wl4g.infra.common.collection.CollectionUtils2.safeMap;
import static com.wl4g.infra.common.reflect.ObjectInstantiators.newInstance;
import static java.util.Objects.nonNull;
import static java.util.stream.Collectors.toMap;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;

/**
 * The standard JQ expression mapper, you can refer to this mapper to implements
 * data permissions that support filtering by each record fields.
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
public class StandardExprProcessMapper extends AbstractProcessMapper {
    public static final String TYPE_NAME = "STANDARD_EXPR_MAPPER";

    private JQConfig jqConfig = new JQConfig();

    @JsonIgnore
    private transient Map<String, Map<String, JsonQuery>> cachedJQs = new ConcurrentHashMap<>(16);

    @JsonIgnore
    private transient final ThreadLocal<Scope> scopeLocal = ThreadLocal.withInitial(() -> {
        final Scope newScope = Scope.newEmptyScope();
        safeMap(jqConfig.getRegisterScopes()).forEach((k, v) -> {
            try {
                newScope.addFunction(k.concat("/1"), (Function) newInstance(ClassUtils.getClass(v)));
            } catch (Throwable ex) {
                throw new StreamConnectException(String.format("Failed to register JQ " +
                        "function for %s -> %s", k, v), ex);
            }
        });
        return newScope;
    });

    @Override
    public String getType() {
        return TYPE_NAME;
    }

    @Override
    protected void doUpdateMergeConditions(Collection<ChannelInfo> channels) {
        final Map<String, Map<String, JsonQuery>> newJQs = safeList(channels)
                .stream()
                .collect(toMap(ChannelInfo::getId,
                        s -> safeList(s.getSettingsSpec().getPolicySpec().getRules())
                                .stream()
                                .filter(p -> isNotBlank(p.getFieldFilter()))
                                .collect(toMap(RuleSpec::getTenantId,
                                        p -> {
                                            try {
                                                return JsonQuery.compile(p.getFieldFilter());
                                            } catch (Throwable ex) {
                                                throw new StreamConnectException(String.format("Failed to compile JQ expr : %s",
                                                        p.getFieldFilter()), ex);
                                            }
                                        }))));

        Map<String, Map<String, JsonQuery>> oldJQs = this.cachedJQs;
        this.cachedJQs = newJQs;
        oldJQs = null;
    }

    @Override
    public MessageRecord<String, Object> doMap(ChannelInfo channel,
                                               MessageRecord<String, Object> record) {
        final String recordTenantId = IStreamConnectConfigurator.getTenantIdForRecord(channel, record);
        Object mappedValue = record.getValue();
        if (mappedValue instanceof JsonNode) {
            JsonNode mappedValue0 = (JsonNode) mappedValue;
            for (RuleSpec policy : safeList(channel.getSettingsSpec().getPolicySpec().getRules())) {
                if (isBlank(policy.getFieldFilter())) {
                    continue;
                }
                try {
                    final JsonQuery jq = cachedJQs.get(channel.getId()).get(recordTenantId);
                    if (nonNull(jq)) {
                        mappedValue = mappedValue0 = jq.apply(scopeLocal.get(), mappedValue0).get(0);
                    }
                } catch (Throwable ex) {
                    throw new StreamConnectException(String.format("Failed to invoke JQ expr : %s, channelId : %s",
                            policy.getFieldFilter(), channel.getId()), ex);
                }
            }
        }
        return new MappedMessageRecord<>(record, mappedValue);
    }

    @Getter
    @Setter
    @SuperBuilder
    @NoArgsConstructor
    public static class JQConfig {
        private @Default Map<String, String> registerScopes = new HashMap<String, String>() {{
            put("del", DelFunction.class.getName());
        }};
        private @Default int maxCacheSize = 128;
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

}
