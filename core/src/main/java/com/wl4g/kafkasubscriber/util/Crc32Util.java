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

package com.wl4g.kafkasubscriber.util;

import com.google.common.base.Charsets;
import com.wl4g.infra.common.lang.Assert2;
import org.apache.kafka.common.utils.Crc32C;

import javax.validation.constraints.NotBlank;

/**
 * The {@link Crc32Util}
 *
 * @author James Wong
 * @since v1.0
 **/
public abstract class Crc32Util {

    public static long compute(@NotBlank String key) {
        Assert2.hasTextOf(key, "key");
        final byte[] bys = key.getBytes(Charsets.UTF_8);
        return Crc32C.compute(bys, 0, bys.length);
    }

}
