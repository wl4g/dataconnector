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

import com.wl4g.streamconnect.stream.StreamConnectEngineBootstrap;

/**
 * The {@link JDBCSourceStreamBootstrap}
 *
 * @author James Wong
 * @since v1.0
 **/
public class JDBCSourceStreamBootstrap extends StreamConnectEngineBootstrap.StreamBootstrap<JDBCSourceStream> {

    public JDBCSourceStreamBootstrap(JDBCSourceStream stream,
                                     Object internalTask) {
        super(stream, internalTask);
    }

    @Override
    public void start() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean stop(long timeoutMs, boolean force) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean scaling(int concurrency, boolean restart, long restartTimeout) throws Exception {
        throw new UnsupportedOperationException();
    }

    @Override
    public void pause() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void resume() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isRunning() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isHealthy() {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getSubTaskCount() {
        throw new UnsupportedOperationException();
    }
}
