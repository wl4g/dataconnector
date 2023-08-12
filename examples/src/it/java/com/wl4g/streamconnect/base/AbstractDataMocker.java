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

package com.wl4g.streamconnect.base;

import org.slf4j.Logger;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;

import static org.slf4j.LoggerFactory.getLogger;

/**
 * The {@link AbstractDataMocker}
 *
 * @author James Wong
 * @since v1.0
 **/
@Order(Ordered.HIGHEST_PRECEDENCE)
public abstract class AbstractDataMocker implements Runnable {

    protected final Logger log = getLogger(getClass());

    public abstract void printStatistics();

    //public static class MockCustomHostResolver implements HostResolver {
    //    @Override
    //    public InetAddress[] resolve(String host) throws UnknownHostException {
    //        return new InetAddress[] {InetAddress.getLocalHost()};
    //    }
    //}

}
