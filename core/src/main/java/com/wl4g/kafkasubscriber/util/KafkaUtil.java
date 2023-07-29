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

package com.wl4g.kafkasubscriber.util;

import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import java.util.Iterator;
import java.util.Objects;

/**
 * The {@link KafkaUtil}
 *
 * @author James Wong
 * @since v1.0
 **/
public abstract class KafkaUtil {

    public static Boolean getFirstValueAsBoolean(Headers headers, String key) {
        return Boolean.parseBoolean(getFirstValueAsString(headers, key));
    }

    public static String getFirstValueAsString(Headers headers, String key) {
        return new String(Objects.requireNonNull(getFirstValue(headers, key)));
    }

    public static byte[] getFirstValue(Headers headers, String key) {
        if (Objects.nonNull(headers)) {
            final Iterator<Header> it = headers.headers(key).iterator();
            if (it.hasNext()) {
                Header first = it.next(); // first only
                if (Objects.nonNull(first)) {
                    return first.value();
                }
            }
        }
        return null;
    }

}
