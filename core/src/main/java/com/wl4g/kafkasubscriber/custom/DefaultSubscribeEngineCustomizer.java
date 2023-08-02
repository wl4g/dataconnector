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

package com.wl4g.kafkasubscriber.custom;

import com.wl4g.kafkasubscriber.bean.SubscriberInfo;
import com.wl4g.kafkasubscriber.bean.TenantInfo;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration;
import com.wl4g.kafkasubscriber.config.KafkaSubscribeConfiguration.SubscribeSourceConfig;
import com.wl4g.kafkasubscriber.source.ISubscribeSourceProvider;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;

/**
 * The {@link DefaultSubscribeEngineCustomizer}
 *
 * @author James Wong
 * @since v1.0
 **/
@AllArgsConstructor
public class DefaultSubscribeEngineCustomizer implements SubscribeEngineCustomizer {

    private final KafkaSubscribeConfiguration config;

    @Override
    public List<SubscriberInfo> loadSubscribers(String pipelineName, SubscriberInfo query) {
        return safeList(config.getDefinitions().getSubscribers())
                .stream()
                .filter(SubscriberInfo::isEnable)
                .collect(Collectors.toList());
    }

    @Override
    public SubscribeSourceConfig loadSourceByTenant(String pipelineName, String tenantId) {
        // Getting tenant by ID.
        final TenantInfo tenant = config.getDefinitions().getTenantMap().get(tenantId);
        if (Objects.isNull(tenant)) {
            throw new IllegalArgumentException(String.format("Not found tenantId '%s'", tenantId));
        }

        // Getting the pipeline source provider by name.
        final ISubscribeSourceProvider sourceProvider = safeList(config.getPipelines())
                .stream()
                .filter(p -> StringUtils.equals(p.getName(), pipelineName))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Not found pipeline '%s'", pipelineName)))
                .getParsedSourceProvider();

        // Getting the source config of the tenant.
        return safeList(sourceProvider.loadSources(pipelineName))
                .stream()
                .filter(s -> StringUtils.equals(s.getName(), tenant.getSourceName()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(String.format("Not found subscribe source " +
                        "config by pipeline: %s, tenant: '%s'", pipelineName, tenant.getId())));
    }

}
