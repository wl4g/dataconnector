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

package com.wl4g.dataconnector.stream.source;

import com.wl4g.dataconnector.coordinator.CachingChannelRegistry;
import com.wl4g.dataconnector.framework.NamedDataConnectorSpi;
import com.wl4g.dataconnector.stream.dispatch.DispatchStream;
import com.wl4g.dataconnector.stream.AbstractStream;
import com.wl4g.dataconnector.stream.DataConnectorEngineBootstrap.StreamBootstrap;
import lombok.Builder.Default;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;
import lombok.experimental.SuperBuilder;
import lombok.extern.slf4j.Slf4j;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;
import java.io.IOException;
import java.util.List;

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

    private final DispatchStream dispatchStream;

    public SourceStream(@NotNull final StreamContext context) {
        super(context);
        this.dispatchStream = requireNonNull(createProcessStream(context),
                "processStream must not be null");
    }

    protected DispatchStream createProcessStream(StreamContext context) {
        return new DispatchStream(context, this);
    }

    @Override
    public void close() throws IOException {
        this.dispatchStream.close();
    }

    public abstract SourceStreamConfig getSourceStreamConfig();

    protected abstract Object getInternalTask();

    @Getter
    @Setter
    @SuperBuilder
    @ToString
    @NoArgsConstructor
    public static abstract class SourceStreamConfig extends BaseStreamConfig {
        private @Nullable  List<String> labels;
        private @Default boolean enable = true;
    }

    public static abstract class SourceStreamProvider extends NamedDataConnectorSpi {
        public abstract StreamBootstrap<? extends SourceStream> create(@NotNull final StreamContext context,
                                                                       @NotNull final SourceStreamConfig sourceStreamConfig,
                                                                       @NotNull final CachingChannelRegistry registry);
    }

}

