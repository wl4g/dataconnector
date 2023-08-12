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

import javax.validation.constraints.Min;
import javax.validation.constraints.NotBlank;

import static com.wl4g.streamconnect.util.Crc32Util.compute;
import static java.lang.Math.abs;
import static java.util.Objects.isNull;

/**
 * The {@link Assignments}
 *
 * @author James Wong
 * @since v1.0
 **/
public abstract class Assignments {
    public static int assign(@NotBlank String key, @Min(1) int parallelism) {
        if (isNull(key) || key.isEmpty()) {
            throw new IllegalArgumentException(String.format("Key must not be empty, but actual: %s", key));
        }
        if (parallelism <= 0) {
            throw new IllegalArgumentException(String.format("Must be parallelism <= 0, but actual: %s",
                    parallelism));
        }
        final long crcValue = abs(compute(key));
        return (int) abs(crcValue % parallelism);
    }
}
