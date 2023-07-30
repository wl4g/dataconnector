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

package com.wl4g.kafkasubscriber.bean;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonSubTypes;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.kafkasubscriber.util.expression.ExpressionOperator;
import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

import static com.fasterxml.jackson.annotation.JsonSubTypes.Type;

/**
 * The {@link SubscriberInfo}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@SuperBuilder
@ToString(callSuper = true)
@NoArgsConstructor
public class SubscriberInfo {
    private Long id;
    private String name;
    private Boolean enable;
    private Boolean isSequence;
    private ExpressionOperator defaultRecordMatchFilterConfig;
    private SinkConfigProperties sinkConfig;

    public void validate() {
        Assert2.notNullOf(id, "id");
        Assert2.hasTextOf(name, "name");
        Assert2.notNullOf(enable, "enable");
        Assert2.notNullOf(isSequence, "isSequence");
        //Assert2.notNullOf(defaultRecordMatchFilterConfig, "defaultRecordMatchFilterConfig");
        //Assert2.notNullOf(sinkConfig, "sinkConfig");
    }

    public static enum OffsetStrategy {
        DEFAULT, EARLIEST, LATEST
    }

    @Schema(oneOf = {KafkaBrokerConnect.class, RabbitmqBrokerConnect.class,
            RocketmqBrokerConnect.class}, discriminatorProperty = "type")
    @JsonTypeInfo(use = JsonTypeInfo.Id.NAME, include = JsonTypeInfo.As.PROPERTY, property = "type"
            , visible = true)
    @JsonSubTypes({@Type(value = KafkaBrokerConnect.class, name = "KAFKA"),
            @Type(value = RabbitmqBrokerConnect.class, name = "RABBITMQ"),
            @Type(value = RocketmqBrokerConnect.class, name = "ROCKETMQ"),})
    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static abstract class SinkConfigProperties {
        @Schema(name = "type", implementation = BrokerType.class)
        @JsonProperty(value = "type", access = JsonProperty.Access.WRITE_ONLY)
        @NotNull
        transient BrokerType type;
        private String connectString;
        private String username;
        private String password;
        private String certsBase64;
        private String version;
        private Long subscribedOffset;
        private OffsetStrategy strategy;
        private Boolean isConnected;
        private String lastConnectMsg;
    }

    public static enum BrokerType {
        KAFKA, RABBITMQ, ROCKETMQ
    }

    public static class KafkaBrokerConnect extends SinkConfigProperties {
    }

    public static class RabbitmqBrokerConnect extends SinkConfigProperties {
    }

    public static class RocketmqBrokerConnect extends SinkConfigProperties {
    }

}
