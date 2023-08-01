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

import com.wl4g.kafkasubscriber.filter.ISubscribeFilter;
import com.wl4g.kafkasubscriber.sink.ISubscribeSink;
import com.wl4g.kafkasubscriber.source.ISubscribeSourceProvider;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.ServiceLoader;

import static com.wl4g.infra.common.lang.Assert2.notNullOf;

/**
 * {@link SubscribeYamlConstructor}
 *
 * @author James Wong
 * @since v1.0
 */
public class SubscribeYamlConstructor extends Constructor {

    public static void configure(BaseConstructor constructor) {
        notNullOf(constructor, "constructor");
        // Register subscribe source provider.
        for (ISubscribeSourceProvider provider : ServiceLoader.load(ISubscribeSourceProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }
        // Register subscribe filter.
        for (ISubscribeFilter filter : ServiceLoader.load(ISubscribeFilter.class)) {
            constructor.addTypeDescription(new TypeDescription(filter.getClass(), "!".concat(filter.getType())));
        }
        // Register subscribe sink.
        for (ISubscribeSink sink : ServiceLoader.load(ISubscribeSink.class)) {
            constructor.addTypeDescription(new TypeDescription(sink.getClass(), "!".concat(sink.getType())));
        }

    }

}