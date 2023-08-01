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

package com.wl4g.kafkasubscriber.config;

import com.wl4g.infra.common.lang.Assert2;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.springframework.util.unit.DataSize;

import java.time.Duration;
import java.util.Properties;

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
    private String id;
    private String name;
    private String tenantId;
    private Boolean enable;
    private @Builder.Default SubscribeSettings settings = new SubscribeSettings();

    public void validate() {
        Assert2.notNullOf(id, "id");
        Assert2.hasTextOf(name, "name");
        Assert2.hasTextOf(tenantId, "tenantId");
        Assert2.notNullOf(enable, "enable");
        Assert2.notNullOf(settings, "settings");
        this.settings.validate();
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class SubscribeSettings {
        private @Builder.Default Boolean isSequence = false;
        private @Builder.Default Duration logRetentionTime = Duration.ofDays(3);
        private @Builder.Default DataSize logRetentionBytes = DataSize.ofMegabytes(512);
        private @Builder.Default Properties properties = new Properties();

        public void validate() {
            Assert2.notNullOf(isSequence, "isSequence");
            Assert2.notNullOf(properties, "properties");
        }
    }

}
