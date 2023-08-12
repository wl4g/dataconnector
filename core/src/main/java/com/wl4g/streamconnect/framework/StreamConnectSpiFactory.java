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

package com.wl4g.streamconnect.framework;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.exception.StreamConnectException;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.commons.collections4.IterableUtils;
import org.apache.commons.lang3.StringUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.List;
import java.util.Map;
import java.util.ServiceLoader;
import java.util.concurrent.ConcurrentHashMap;

import static com.wl4g.infra.common.collection.CollectionUtils2.safeList;


/**
 * The {@link StreamConnectSpiFactory}
 *
 * @author James Wong
 * @since v1.0
 **/
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class StreamConnectSpiFactory {

    private final static Map<Class<? extends IStreamConnectSpi>, Map<String, IStreamConnectSpi>>
            CACHED = new ConcurrentHashMap<>(4);

    /**
     * Obtain the stream connect SPI instance.
     *
     * @param type stream connect SPI type name.
     * @return {@link IStreamConnectSpi}
     */
    @SuppressWarnings("unchecked")
    public static <T extends IStreamConnectSpi> T get(@NotNull Class<T> spiClass,
                                                      @NotBlank final String type) {
        Assert2.hasTextOf(type, "type");
        return (T) CACHED.computeIfAbsent(spiClass,
                        spiClass0 -> new ConcurrentHashMap<>(4))
                .computeIfAbsent(type, t -> obtain(spiClass, t));
    }

    public static IStreamConnectSpi obtain(Class<? extends IStreamConnectSpi> spiClass,
                                           String type) {
        return safeList(load(spiClass))
                .stream()
                .filter(f -> StringUtils.equals(type, f.getType()))
                .findFirst()
                .orElseThrow(() -> new StreamConnectException(String.format("The custom stream connect SPI " +
                        "named %s was not found, if it is a custom, please make sure it is configured correctly " +
                        "in META-INF/services/%s", type, spiClass.getName())));
    }

    public static <T extends IStreamConnectSpi> List<T> load(Class<T> spiClass) {
        return IterableUtils.toList(ServiceLoader.load(spiClass));
    }

}
