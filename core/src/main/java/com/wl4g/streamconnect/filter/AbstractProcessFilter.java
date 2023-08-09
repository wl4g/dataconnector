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

package com.wl4g.streamconnect.filter;

import com.wl4g.infra.common.lang.Assert2;
import com.wl4g.streamconnect.bean.SubscriberInfo;
import com.wl4g.streamconnect.config.StreamConnectProperties.SubscribeProcessProperties;
import lombok.AccessLevel;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;

import java.util.Collection;

/**
 * The {@link AbstractProcessFilter}
 *
 * @author James Wong
 * @since v1.0
 **/
@Getter
@Setter
@Slf4j
public abstract class AbstractProcessFilter implements IProcessFilter {

    private String name;
    private SubscribeProcessProperties filterProps = new SubscribeProcessProperties();
    private Long updateMergeConditionsDelayTime = 2_000L;

    @Setter(AccessLevel.NONE)
    private transient long _lastUpdateTime = 0L;

    @Override
    public void validate() {
        Assert2.hasTextOf(name, "name");
        filterProps.validate();
    }

    @Override
    public void updateMergeSubscribeConditions(Collection<SubscriberInfo> subscribers) {
        if (Math.abs(System.nanoTime()) - _lastUpdateTime > getUpdateMergeConditionsDelayTime()) {
            log.trace(":: {} :: Updating to merge subscribe conditions ...", getName());
            _lastUpdateTime = System.nanoTime();
            doUpdateMergeConditions(subscribers);
        } else {
            log.debug(":: {} :: Skip update merge subscribe conditions.", getName());
        }
    }

    protected abstract void doUpdateMergeConditions(Collection<SubscriberInfo> subscribers);

}
