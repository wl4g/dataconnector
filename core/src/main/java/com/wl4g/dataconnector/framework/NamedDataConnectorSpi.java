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

package com.wl4g.dataconnector.framework;

import com.wl4g.dataconnector.util.SpringContextHolder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import java.util.Objects;


/**
 * The {@link NamedDataConnectorSpi}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@SuperBuilder
@NoArgsConstructor
@Slf4j
public abstract class NamedDataConnectorSpi implements IDataConnectorSpi {

    private String name;

    protected Object getBean(String beanName) {
        Objects.requireNonNull(beanName, "beanName must not be null.");
        return SpringContextHolder.getBean(beanName);
    }

    protected <T> T getBean(Class<T> beanClass, Object... args) {
        Objects.requireNonNull(beanClass, "beanClass must not be null.");
        return SpringContextHolder.getBean(beanClass, args);
    }

    protected <T> T getBean(String beanName, Class<T> beanClass) {
        Objects.requireNonNull(beanName, "beanName must not be null.");
        Objects.requireNonNull(beanClass, "beanClass must not be null.");
        return SpringContextHolder.getBean(beanName, beanClass);
    }

}
