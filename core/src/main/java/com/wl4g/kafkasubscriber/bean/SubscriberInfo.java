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

package com.wl4g.kafkasubscriber.bean;

import com.wl4g.infra.common.lang.Assert2;
import lombok.Builder;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import org.springframework.util.unit.DataSize;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import javax.validation.constraints.Null;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
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
    private @Default boolean enable = true;
    private @Builder.Default SubscribeRule rule = new SubscribeRule();

    public SubscriberInfo validate() {
        Assert2.notNullOf(id, "id");
        Assert2.hasTextOf(name, "name");
        Assert2.notNullOf(enable, "enable");
        Assert2.notNullOf(rule, "settings");
        this.rule.validate();
        return this;
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class SubscribeRule {
        @NotNull
        @Builder.Default
        private Boolean isSequence = false;
        @NotEmpty
        @Builder.Default
        private List<SubscribeGrantedPolicy> policies = new ArrayList<>(2);
        @NotNull
        @Builder.Default
        private Duration logRetentionTime = Duration.ofDays(3);
        @NotNull
        @Builder.Default
        private DataSize logRetentionBytes = DataSize.ofMegabytes(512);
        @Null
        private Properties properties;

        public void validate() {
            Assert2.notNullOf(isSequence, "isSequence");
            Assert2.notEmptyOf(policies, "policies");

            // The current subscriber is only allowed to belong to one tenant.
            //final long thisTenantPolicies = safeList(getPolicies()).stream()
            //        .filter(SubscribeGrantedPolicy::isSelfTenant).count();
            //Assert2.isTrue(thisTenantPolicies <= 1, "The current subscriber " +
            //        "is only allowed to belong to one tenant or all other tenants.");
        }
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString(callSuper = true)
    @NoArgsConstructor
    public static class SubscribeGrantedPolicy {
        @NotBlank
        private String tenantId;
        //private boolean selfTenant;
        @NotBlank
        private String recordFilter;
        @Null
        private String fieldFilter;
    }

}
