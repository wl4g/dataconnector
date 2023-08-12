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

package com.wl4g.streamconnect.stream.source.jdbc;

import org.apache.shardingsphere.elasticjob.api.ShardingContext;
import org.apache.shardingsphere.elasticjob.dataflow.job.DataflowJob;

import java.util.List;
import java.util.Map;

/**
 * The {@link JDBCSourceStream}
 *
 * @author James Wong
 * @since v1.0
 **/
public class JDBCSourceFetcher {

    static class JDBCFetchJob implements DataflowJob<Map<String, Object>> {
        @Override
        public List<Map<String, Object>> fetchData(ShardingContext context) {
            // TODO
            return null;
        }

        @Override
        public void processData(ShardingContext context,
                                List<Map<String, Object>> records) {
            // TODO
        }
    }

}
