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

package com.wl4g.dataconnector.facade;

import com.wl4g.dataconnector.exception.DataConnectorException;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.dataconnector.config.DataConnectorConfiguration;
import com.wl4g.dataconnector.config.DataConnectorConfiguration.ConnectorConfig;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap.ContainerTaskStatus;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap.DataConnectororBootstrap;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import javax.annotation.Nullable;
import java.util.Map;
import java.util.Objects;

import static java.util.Objects.isNull;

/**
 * The {@link DataConnectorEngineFacade}
 *
 * @author James Wong
 * @since v1.0
 **/
@Slf4j
@Getter
@AllArgsConstructor
@SuppressWarnings("unused")
public class DataConnectorEngineFacade {
    private final @Getter(AccessLevel.NONE) DataConnectorConfiguration config;
    private final @Getter(AccessLevel.NONE) DataConnectorEngineBootstrap engineBootstrap;

    public ConnectorConfig getConnectorConfig(@NotBlank String connectorName) {
        return config.getConnectorConfig(connectorName, false);
    }

    public DataConnectororBootstrap registerConnector(ConnectorConfig connectorConfig) {
        return engineBootstrap.registerConnector(connectorConfig);
    }

    public Map<String, DataConnectororBootstrap> getConnectorBootstraps() {
        return engineBootstrap.getConnectorBootstraps();
    }

    public @NotNull Map<String, Boolean> startSources(@NotBlank String connectorName,
                                                      String... sourceNames) {
        return getRequiredConnectorBootstrap(connectorName).startSources(sourceNames);
    }

    public @NotNull Map<String, Boolean> startSinks(@NotBlank String connectorName,
                                                    String... channelIds) {
        return getRequiredConnectorBootstrap(connectorName).startSinks(channelIds);
    }

    public @NotNull Map<String, Boolean> stopSources(@NotBlank String connectorName,
                                                     long perFilterTimeout,
                                                     @Nullable String... sourceNames) {
        return getRequiredConnectorBootstrap(connectorName).stopSources(perFilterTimeout, sourceNames);
    }

    public @NotNull Map<String, Boolean> stopSinks(@NotBlank String connectorName,
                                                   long perSinkTimeout,
                                                   @Nullable String... channelIds) {
        return getRequiredConnectorBootstrap(connectorName).stopSinks(perSinkTimeout, channelIds);
    }

    public @NotNull Map<String, ContainerTaskStatus> statusSources(@NotBlank String connectorName,
                                                                   String... sourceNames) {
        return getRequiredConnectorBootstrap(connectorName).statusSources(sourceNames);
    }

    public @NotNull Map<String, ContainerTaskStatus> statusSinks(@NotBlank String connectorName,
                                                                 String... channelIds) {
        return getRequiredConnectorBootstrap(connectorName).statusSinks(channelIds);
    }

    public Map<String, Integer> scalingSources(@NotBlank String connectorName,
                                               int perConcurrency,
                                               boolean restart,
                                               long perRestartTimeout,
                                               String... sourceNames) {
        return getRequiredConnectorBootstrap(connectorName).scalingSources(
                perConcurrency, restart, perRestartTimeout, sourceNames);
    }

    public Map<String, Integer> scalingSinks(@NotBlank String connectorName,
                                             int perConcurrency,
                                             boolean restart,
                                             long perRestartTimeout,
                                             String... channelIds) {
        return getRequiredConnectorBootstrap(connectorName).scalingSinks(
                perConcurrency, restart, perRestartTimeout, channelIds);
    }

    private DataConnectororBootstrap getRequiredConnectorBootstrap(
            @NotBlank String connectorName) {
        Assert2.hasTextOf(connectorName, "connectorName");
        final DataConnectororBootstrap connector = getConnectorBootstraps().get(connectorName);
        if (isNull(connector)) {
            throw new DataConnectorException(String.format("Not found the connector bootstrap %s for stop.", connectorName));
        }
        return connector;
    }

}
