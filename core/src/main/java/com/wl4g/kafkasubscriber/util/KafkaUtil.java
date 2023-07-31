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

import com.wl4g.infra.common.lang.Assert2;
import org.apache.kafka.clients.admin.*;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.Headers;

import javax.validation.constraints.NotBlank;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

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

    public static Collection<MemberDescription> getGroupConsumers(@NotBlank String bootstrapServers,
                                                                  @NotBlank String groupId,
                                                                  long timeout) throws ExecutionException, InterruptedException, TimeoutException {
        Assert2.hasTextOf(bootstrapServers, "bootstrapServers");
        Assert2.hasTextOf(groupId, "groupId");

        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);

        try (AdminClient adminClient = AdminClient.create(properties)) {
            DescribeConsumerGroupsOptions describeOptions = new DescribeConsumerGroupsOptions()
                    .includeAuthorizedOperations(false);

            DescribeConsumerGroupsResult describeResult = adminClient.describeConsumerGroups(
                    Collections.singletonList(groupId), describeOptions);

            Map<String, ConsumerGroupDescription> consumerGroupDescriptionMap =
                    describeResult.all().get(timeout, TimeUnit.MILLISECONDS);

            if (consumerGroupDescriptionMap.containsKey(groupId)) {
                ConsumerGroupDescription consumerGroupDescription =
                        consumerGroupDescriptionMap.get(groupId);
                return consumerGroupDescription.members();
            }
        }

        return Collections.emptyList();
    }

}
