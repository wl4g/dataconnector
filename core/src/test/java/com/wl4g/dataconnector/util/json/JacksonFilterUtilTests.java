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

package com.wl4g.dataconnector.util.json;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

/**
 * The {@link JacksonFilterUtilTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class JacksonFilterUtilTests {


    @Test
    public void testRemoveWithJsonPath_for_ObjectNode() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        String jsonString = "{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}";
        ObjectNode rootNode = objectMapper.readValue(jsonString, ObjectNode.class);

        String removeJsonPath = ".address";
        JacksonFilterUtil.removeWithJsonPath(rootNode, removeJsonPath);

        String filteredJsonString = objectMapper.writeValueAsString(rootNode);
        Assertions.assertEquals("{\"name\":\"John\",\"age\":30}", filteredJsonString);
    }

    @Test
    public void testRemoveWithJsonPath_for_ArrayNode() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        String jsonString = "{\"name\":\"John\",\"age\":30,\"addresses\":[{\"city\":\"New York\",\"zipcode\":\"12345\"}]}";
        ObjectNode rootNode = objectMapper.readValue(jsonString, ObjectNode.class);

        String removeJsonPath = ".addresses";
        JacksonFilterUtil.removeWithJsonPath(rootNode, removeJsonPath);

        String filteredJsonString = objectMapper.writeValueAsString(rootNode);
        Assertions.assertEquals("{\"name\":\"John\",\"age\":30}", filteredJsonString);
    }

    @Test
    public void testRemoveWithJsonPath_for_ObjectNode_TextNode() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        String jsonString = "{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}";
        ObjectNode rootNode = objectMapper.readValue(jsonString, ObjectNode.class);

        String removeJsonPath = ".address.zipcode";
        JacksonFilterUtil.removeWithJsonPath(rootNode, removeJsonPath);

        String filteredJsonString = objectMapper.writeValueAsString(rootNode);
        Assertions.assertEquals("{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"New York\"}}",
                filteredJsonString);
    }

    @Test
    public void testRemoveWithJsonPath_for_ArrayNode_TextNode() throws Exception {
        ObjectMapper objectMapper = new ObjectMapper();

        String jsonString = "{\"name\":\"John\",\"age\":30,\"addresses\":[{\"city\":\"New York\",\"zipcode\":\"12345\"}]}";
        ObjectNode rootNode = objectMapper.readValue(jsonString, ObjectNode.class);

        String removeJsonPath = ".addresses.zipcode";
        JacksonFilterUtil.removeWithJsonPath(rootNode, removeJsonPath);

        String filteredJsonString = objectMapper.writeValueAsString(rootNode);
        Assertions.assertEquals("{\"name\":\"John\",\"age\":30,\"addresses\":[{\"city\":\"New York\"}]}", filteredJsonString);
    }

}
