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

package com.wl4g.streamconnect.stream.source.tdengine;

import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.stream.source.jdbc.JDBCSourceFetcher;
import com.wl4g.streamconnect.stream.source.jdbc.JDBCSourceStream;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;

import javax.validation.constraints.NotNull;

import static com.wl4g.streamconnect.stream.AbstractStream.BaseStreamConfig.getStreamProviderTypeName;

/**
 * The {@link TDengineCDCSourceStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
public class TDengineCDCSourceStream extends JDBCSourceStream {

    public TDengineCDCSourceStream(@NotNull final StreamContext context,
                                   @NotNull final TDengineCDCSourceStreamConfig sourceStreamConfig,
                                   @NotNull final JDBCSourceFetcher jdbcSourceFetcher) {
        super(context, sourceStreamConfig, jdbcSourceFetcher);
    }

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static class TDengineCDCSourceStreamConfig extends JDBCSourceStreamConfig {
        public static final String TYPE_NAME = "TDENGINE_SOURCE";

        // TODO add more properties.

        @Override
        public String getType() {
            return TYPE_NAME;
        }
    }

    public static class TDengineCDCSourceStreamProvider extends SourceStreamProvider {
        public static final String TYPE_NAME = getStreamProviderTypeName(TDengineCDCSourceStreamConfig.TYPE_NAME);

        @Override
        public String getType() {
            return TYPE_NAME;
        }

        @Override
        public TDengineCDCSourceStreamBootstrap create(@NotNull final StreamContext context,
                                                       @NotNull final SourceStreamConfig sourceStreamConfig,
                                                       @NotNull final CachingChannelRegistry registry) {
            throw new UnsupportedOperationException();
        }
    }

}
