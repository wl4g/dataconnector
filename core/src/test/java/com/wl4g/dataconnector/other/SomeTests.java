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

package com.wl4g.dataconnector.other;

import com.wl4g.infra.common.yaml.YamlUtils;
import org.junit.Test;
import org.junit.jupiter.api.Assertions;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * The {@link SomeTests}
 *
 * @author James Wong
 * @since v1.0
 **/
@SuppressWarnings({"all"})
public class SomeTests {

    @Test
    @SuppressWarnings("all")
    public void testTypeConvert() {
        final HashMap<String, Object> map = new HashMap<>();
        String value = (String) map.get("notExist");
        Assertions.assertNull(value);

        // ----------------------------

        String configurePropsYaml = "consumerProps:\n  key1: \"value1\"\n  key2: 1001\n";
        Map<String, String> configureProps = YamlUtils.parse(configurePropsYaml,
                null, null, "consumerProps", HashMap.class);

        final Properties defaultProps = new Properties();
        defaultProps.put("key2", "1002");

        Assertions.assertThrows(ClassCastException.class, () -> {
            // putIfAbsent() 返回值的隐式类型转换
            configureProps.forEach((k, v) ->
                    defaultProps.putIfAbsent(String.valueOf(k), String.valueOf(v)));
        });
    }

}
