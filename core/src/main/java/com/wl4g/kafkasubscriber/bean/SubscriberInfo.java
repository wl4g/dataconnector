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

import com.wl4g.infra.common.lang.Assert2;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

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
    private Long id;
    private String name;
    private Boolean enable;
    private Boolean isSequence;
    private Properties properties;

    public void validate() {
        Assert2.notNullOf(id, "id");
        Assert2.hasTextOf(name, "name");
        Assert2.notNullOf(enable, "enable");
        Assert2.notNullOf(isSequence, "isSequence");
        Assert2.notNullOf(properties, "properties");
    }

}
