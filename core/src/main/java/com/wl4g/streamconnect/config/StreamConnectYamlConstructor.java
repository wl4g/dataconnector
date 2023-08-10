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
package com.wl4g.streamconnect.config;

import com.wl4g.streamconnect.checkpoint.IProcessCheckpoint;
import com.wl4g.streamconnect.process.filter.IProcessFilter;
import com.wl4g.streamconnect.process.map.IProcessMapper;
import com.wl4g.streamconnect.process.sink.IProcessSink;
import com.wl4g.streamconnect.source.ISourceProvider;
import org.yaml.snakeyaml.TypeDescription;
import org.yaml.snakeyaml.constructor.BaseConstructor;
import org.yaml.snakeyaml.constructor.Constructor;

import java.util.ServiceLoader;

import static com.wl4g.infra.common.lang.Assert2.notNullOf;

/**
 * {@link StreamConnectYamlConstructor}
 *
 * @author James Wong
 * @since v1.0
 */
public class StreamConnectYamlConstructor extends Constructor {

    public static void configure(BaseConstructor constructor) {
        notNullOf(constructor, "constructor");
        // Register subscribe checkpoint.
        for (IProcessCheckpoint checkpoint : ServiceLoader.load(IProcessCheckpoint.class)) {
            constructor.addTypeDescription(new TypeDescription(checkpoint.getClass(), "!".concat(checkpoint.getType())));
        }
        // Register subscribe sources provider.
        for (ISourceProvider provider : ServiceLoader.load(ISourceProvider.class)) {
            constructor.addTypeDescription(new TypeDescription(provider.getClass(), "!".concat(provider.getType())));
        }
        // Register subscribe filters.
        for (IProcessFilter filter : ServiceLoader.load(IProcessFilter.class)) {
            constructor.addTypeDescription(new TypeDescription(filter.getClass(), "!".concat(filter.getType())));
        }
        // Register subscribe mappers.
        for (IProcessMapper mapper : ServiceLoader.load(IProcessMapper.class)) {
            constructor.addTypeDescription(new TypeDescription(mapper.getClass(), "!".concat(mapper.getType())));
        }
        // Register subscribe sinks.
        for (IProcessSink sink : ServiceLoader.load(IProcessSink.class)) {
            constructor.addTypeDescription(new TypeDescription(sink.getClass(), "!".concat(sink.getType())));
        }
    }

}