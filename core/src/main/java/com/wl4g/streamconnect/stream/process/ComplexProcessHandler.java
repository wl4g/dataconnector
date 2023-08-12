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

package com.wl4g.streamconnect.stream.process;

import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

/**
 * The {@link ComplexProcessHandler}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@Slf4j
public abstract class ComplexProcessHandler /*extends NamedStreamConnectSpi */ {
//    private long mergeConditionsDelay = 30_000L;
//
//    @Getter(AccessLevel.NONE)
//    @Setter(AccessLevel.NONE)
//    private transient long _lastUpdateTime = 0L;
//
//    public synchronized void updateMergeConditions(Collection<ChannelInfo> channels) {
//        if (Math.abs(System.nanoTime()) - _lastUpdateTime > getMergeConditionsDelay()) {
//            log.trace(":: {} :: Updating to merge channel conditions ...", getName());
//            _lastUpdateTime = System.nanoTime();
//            doUpdateMergeConditions(channels);
//        } else {
//            log.debug(":: {} :: Skip update merge channel conditions.", getName());
//        }
//    }
//
//    protected abstract void doUpdateMergeConditions(Collection<ChannelInfo> channels);

}
