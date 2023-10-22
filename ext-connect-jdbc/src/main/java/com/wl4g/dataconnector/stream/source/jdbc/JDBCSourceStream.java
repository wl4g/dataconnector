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

package com.wl4g.dataconnector.stream.source.jdbc;

import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.stream.AbstractStream;
import com.wl4g.dataconnector.stream.source.SourceStream;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * The {@link JDBCSourceStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class JDBCSourceStream extends SourceStream {

    private final JDBCSourceStreamConfig sourceStreamConfig;
    private final JDBCSourceFetcher jdbcSourceFetcher;

    public JDBCSourceStream(@NotNull final AbstractStream.StreamContext context,
                            @NotNull final JDBCSourceStreamConfig sourceStreamConfig,
                            @NotNull final JDBCSourceFetcher jdbcSourceFetcher) {
        super(context);
        this.sourceStreamConfig = requireNonNull(sourceStreamConfig,
                "sourceStreamConfig must not be null");
        this.jdbcSourceFetcher = requireNonNull(jdbcSourceFetcher,
                "jdbcFetchSourceJob must not be null");
    }

    @Override
    public String getDescription() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getInternalTask() {
        return jdbcSourceFetcher;
    }

    @Override
    public void close() throws IOException {
        super.close();
        // TODO example closing for JDBC data source
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class JDBCSourceStreamConfig extends SourceStreamConfig {
        public static final String TYPE_NAME = "JDBC_SOURCE";

        @Override
        public String getType() {
            return TYPE_NAME;
        }
    }

    public static class JDBCSourceStreamProvider extends SourceStreamProvider {
        public static final String TYPE_NAME = AbstractStream.BaseStreamConfig.getStreamProviderTypeName(JDBCSourceStreamConfig.TYPE_NAME);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public JDBCSourceStreamBootstrap create(@NotNull final AbstractStream.StreamContext context,
                                                @NotNull final SourceStreamConfig sourceStreamConfig,
                                                @NotNull final CachingChannelRegistry registry) {
            throw new UnsupportedOperationException();
        }
    }

}
