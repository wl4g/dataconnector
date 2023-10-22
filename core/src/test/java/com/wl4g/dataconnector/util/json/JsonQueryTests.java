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

import com.fasterxml.jackson.databind.JsonNode;
import com.wl4g.infra.common.serialize.JacksonUtils;
import net.thisptr.jackson.jq.JsonQuery;
import net.thisptr.jackson.jq.Scope;
import net.thisptr.jackson.jq.exception.JsonQueryException;
import net.thisptr.jackson.jq.internal.functions.DelFunction;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Assertions;

import java.util.List;

/**
 * The {@link JsonQueryTests}
 *
 * @author James Wong
 * @since v1.0
 **/
public class JsonQueryTests {

    @Test
    public void testJsonQueryFn() throws Exception {
        final String json = "{\"name\":\"John\",\"age\":30,\"address\":{\"city\":\"New York\",\"zipcode\":\"12345\"}}";
        final List<JsonNode> result = callJsonQueryFn(json, "del(.address.city)");
        Assertions.assertEquals(1, result.size());
        Assertions.assertFalse(result.get(0).has("/address/city"));
    }

    static List<JsonNode> callJsonQueryFn(String json, String path) throws JsonQueryException {
        final JsonQuery jq = JsonQuery.compile(path);
        final Scope scope = Scope.newEmptyScope();
        scope.addFunction("del", new DelFunction());
        return jq.apply(scope, JacksonUtils.parseToNode(json));
    }

}
