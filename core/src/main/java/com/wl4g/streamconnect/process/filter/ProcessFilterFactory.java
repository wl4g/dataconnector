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

package com.wl4g.streamconnect.process.filter;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.infra.common.reflect.ObjectInstantiators;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotEmpty;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.StreamSupport;


/**
 * The {@link ProcessFilterFactory}
 *
 * @author James Wong
 * @since v1.0
 **/
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class ProcessFilterFactory {

    private final static Map<String, IProcessFilter[]> CACHED = new ConcurrentHashMap<>(2);

    /**
     * Get subscribe filter.
     *
     * @param pipelineName subscribe pipeline name.
     * @param types        subscribe filter types.
     * @return subscribe filter
     */
    public static IProcessFilter[] obtainFilters(@NotBlank final String pipelineName,
                                                 @NotEmpty final List<String> types) {
        Assert2.hasTextOf(pipelineName, "pipelineName");
        Assert2.notEmptyOf(types, "types");

        IProcessFilter[] filters = CACHED.get(pipelineName);
        if (Objects.isNull(filters)) {
            synchronized (ProcessFilterFactory.class) {
                filters = CACHED.get(pipelineName);
                if (Objects.isNull(filters)) {
                    CACHED.put(pipelineName, filters = createFilters(types));
                }
            }
        }
        return filters;
    }

    private static IProcessFilter[] createFilters(List<String> types) {
        return StreamSupport
                .stream(ServiceLoader.load(IProcessFilter.class).spliterator(), false)
                .filter(f -> types.contains(f.getType()))
                .map(f -> ObjectInstantiators.newInstance(f.getClass())) // no-cached-instance
                .sorted(Comparator.comparingInt(IProcessFilter::getOrder))
                .toArray(IProcessFilter[]::new);
    }

}
