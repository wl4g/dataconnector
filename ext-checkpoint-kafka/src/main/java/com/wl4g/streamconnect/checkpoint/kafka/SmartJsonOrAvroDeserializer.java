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

package com.wl4g.streamconnect.checkpoint.kafka;

import com.fasterxml.jackson.databind.node.ObjectNode;
import com.wl4g.streamconnect.checkpoint.ICheckpoint.PointDeserializer;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static com.wl4g.infra.common.serialize.JacksonUtils.parseToNode;

/**
 * The {@link SmartJsonOrAvroDeserializer}
 *
 * @author James Wong
 * @since v1.0
 **/
public class SmartJsonOrAvroDeserializer implements PointDeserializer<ObjectNode> {

    @Override
    public ObjectNode deserialize(String topic,
                                  Map<String, ObjectNode> headers,
                                  byte[] data) {
        if (headers.containsKey("avro")) { // TODO
            return deserializeWithAvro(data);
        }
        // Deserialize for json.
        return (ObjectNode) parseToNode(new String(data, StandardCharsets.UTF_8));
    }

    protected ObjectNode deserializeWithAvro(byte[] data) {
        // TODO deserialize for avro
        //AvroSchemaUtils.getSchema("")
        return null;
    }

}
