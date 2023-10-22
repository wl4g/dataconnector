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

package com.wl4g.dataconnector.util;

import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Component;

import java.util.Objects;

/**
 * The {@link SpringContextHolder}
 *
 * @author James Wong
 * @since v1.0
 **/
@Component
@Lazy(false)
@Order(Ordered.HIGHEST_PRECEDENCE + 10)
public class SpringContextHolder implements ApplicationContextAware {

    private static ApplicationContext context;

    @Override
    public void setApplicationContext(ApplicationContext context) throws BeansException {
        Objects.requireNonNull(context, "SpringContextHolder.context must not be null.");
        SpringContextHolder.context = context;
    }

    public static <T> T getBean(Class<T> beanClass, Object... args) {
        Objects.requireNonNull(beanClass, "beanClass must not be null.");
        Objects.requireNonNull(context, "SpringContextHolder.context must not be null.");
        return context.getBean(beanClass, args);
    }

    public static Object getBean(String beanName) {
        Objects.requireNonNull(beanName, "beanName must not be null.");
        Objects.requireNonNull(context, "SpringContextHolder.context must not be null.");
        return context.getBean(beanName);
    }


    public static <T> T getBean(String beanName, Class<T> beanClass) {
        Objects.requireNonNull(beanName, "beanName must not be null.");
        Objects.requireNonNull(context, "SpringContextHolder.context must not be null.");
        return context.getBean(beanName, beanClass);
    }

}
