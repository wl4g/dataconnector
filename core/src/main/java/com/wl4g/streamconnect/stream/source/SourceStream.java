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

package com.wl4g.streamconnect.stream.source;

import com.wl4g.streamconnect.coordinator.CachingChannelRegistry;
import com.wl4g.streamconnect.framework.NamedStreamConnectSpi;
import com.wl4g.streamconnect.stream.process.ProcessStream;
import com.wl4g.streamconnect.stream.AbstractStream;
import com.wl4g.streamconnect.stream.StreamConnectEngineBootstrap.StreamBootstrap;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.validation.constraints.NotNull;
import java.io.IOException;

import static java.util.Objects.requireNonNull;

/**
 * The {@link SourceStream}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Slf4j
public abstract class SourceStream extends AbstractStream {

    private final ProcessStream processStream;

    public SourceStream(@NotNull final StreamContext context) {
        super(context);
        this.processStream = requireNonNull(createProcessStream(context),
                "processStream must not be null");
    }

    protected ProcessStream createProcessStream(StreamContext context) {
        return new ProcessStream(context, this);
    }

    @Override
    public void close() throws IOException {
        this.processStream.close();
    }

    public abstract SourceStreamConfig getSourceStreamConfig();

    protected abstract Object getInternalTask();

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static abstract class SourceStreamConfig extends BaseStreamConfig {
    }

    public static abstract class SourceStreamProvider extends NamedStreamConnectSpi {
        public abstract StreamBootstrap<? extends SourceStream> create(@NotNull final StreamContext context,
                                                                       @NotNull final SourceStreamConfig sourceStreamConfig,
                                                                       @NotNull final CachingChannelRegistry registry);
    }

}

