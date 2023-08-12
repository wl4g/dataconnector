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

package com.wl4g.streamconnect.util;

import com.google.common.base.Charsets;
import com.wl4g.infra.common.lang.Assert2;

import javax.validation.constraints.NotBlank;
import java.util.zip.CRC32;

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
        final CRC32 crc32 = new CRC32();
        crc32.update(bys, 0, bys.length);
        return crc32.getValue();
    }

}
