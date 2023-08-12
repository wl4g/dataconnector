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

package com.wl4g.streamconnect.util.json;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.infra.common.serialize.JacksonUtils;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.NotNull;
import java.util.Objects;

/**
 * The {@link JacksonFilterUtil}
 *
 * @author James Wong
 * @since v1.0
 **/
public abstract class JacksonFilterUtil extends JacksonUtils {

    public static void removeWithJsonPath(@NotNull ObjectNode node,
                                          @NotBlank String jsonPath) {
        Assert2.notNullOf(node, "node");
        Assert2.hasTextOf(jsonPath, "jsonPath");

        final String[] parts = jsonPath.split("\\.");
        JsonNode currentNode = node, parentNode = null;
        for (String part : parts) {
            if (part.isEmpty()) {
                continue;
            }
            if (currentNode instanceof ObjectNode) {
                parentNode = currentNode;
                final JsonNode childNode = currentNode.get(part);
                if (Objects.nonNull(childNode)) {
                    currentNode = childNode;
                }
            }
        }
        // Remove unnecessary field nodes.
        if (Objects.nonNull(currentNode) && currentNode.isArray()) {
            currentNode.forEach(n -> {
                if (n instanceof ObjectNode) {
                    ((ObjectNode) n).remove(parts[parts.length - 1]);
                }
            });
        }
        if (Objects.nonNull(parentNode) && parentNode.isObject()) {
            ((ObjectNode) parentNode).remove(parts[parts.length - 1]);
        }
    }

}
