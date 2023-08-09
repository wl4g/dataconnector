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

package com.wl4g.streamconnect.custom;

import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.bean.TenantInfo;
import com.wl4g.streamconnect.config.StreamConnectConfiguration;
import com.wl4g.streamconnect.config.StreamConnectProperties.SourceProperties;
import com.wl4g.streamconnect.coordinator.IStreamConnectCoordinator;
import com.wl4g.streamconnect.exception.StreamConnectException;
import com.wl4g.streamconnect.source.ISourceProvider;
import com.wl4g.streamconnect.util.Crc32Util;
import lombok.AllArgsConstructor;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Null;
import java.util.List;
import java.util.Objects;
import java.util.function.Predicate;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;
import static java.util.stream.Collectors.toList;

/**
 * The {@link DefaultStreamConnectEngineCustomizer}
 *
 * @author James Wong
 * @since v1.0
 **/
@AllArgsConstructor
public class DefaultStreamConnectEngineCustomizer implements StreamConnectEngineCustomizer {

    private final StreamConnectConfiguration config;

    @Override
    public List<SubscriberInfo> loadSubscribers(@NotBlank String pipelineName,
                                                @Null IStreamConnectCoordinator.ShardingInfo sharding) {
        Predicate<SubscriberInfo> predicate = s -> true;
        if (Objects.nonNull(sharding)) {
            predicate = s -> safeList(sharding.getItems())
                    .contains(sharding.getTotal() % (int) Crc32Util.compute(s.getId()));
        }
        return safeList(config.getProperties().getDefinitions().getSubscribers())
                .stream()
                .filter(SubscriberInfo::isEnable)
                .filter(predicate)
                .collect(toList());
    }

    @Override
    public SourceProperties loadSourceByTenant(String pipelineName, String tenantId) {
        // Getting tenant by ID.
        final TenantInfo tenant = safeList(config.getProperties().getDefinitions().getTenants())
                .stream()
                .filter(t -> StringUtils.equals(t.getName(), tenantId))
                .findFirst()
                .orElseThrow(() -> new StreamConnectException(String.format("Not found tenant '%s'", pipelineName)));
        if (Objects.isNull(tenant)) {
            throw new StreamConnectException(String.format("Not found tenantId '%s'", tenantId));
        }

        // Getting the source provider by pipeline name.
        final ISourceProvider sourceProvider = safeList(config.getPipelines())
                .stream()
                .filter(p -> StringUtils.equals(p.getName(), pipelineName))
                .findFirst()
                .orElseThrow(() -> new StreamConnectException(String.format("Not found pipeline '%s'", pipelineName)))
                .getSourceProvider();

        // Getting the source config by tenant ID.
        return safeList(sourceProvider.loadSources(pipelineName))
                .stream()
                .filter(s -> StringUtils.equals(s.getName(), tenant.getSourceName()))
                .findFirst()
                .orElseThrow(() -> new StreamConnectException(String.format("Not found subscribe source " +
                        "config by pipeline: %s, tenant: '%s'", pipelineName, tenant.getId())));
    }

}
