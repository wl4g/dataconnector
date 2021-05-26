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

package com.wl4g.kafkasubscriber.coordinator;

import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.simple.job.SimpleJob;

/**
 * The {@link AbstractSubscribeCoordinator}
 * based on elastic-job or nature zookeeper to implement find subscribers by sharding item.
 *
 * @author James Wong
 * @since v1.0
 **/
public class ElasticAbstractSubscribeCoordinator extends AbstractSubscribeCoordinator implements SimpleJob {

    @Override
    public void execute(ShardingContext shardingContext) {
        onReBalancing(new ShardingInfo(shardingContext.getShardingTotalCount(), shardingContext.getShardingItem()));
    }

}