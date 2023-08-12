/**
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

package org.testcontainers.utility;

import com.github.dockerjava.api.model.Bind;
import com.github.dockerjava.api.model.Volume;
import org.testcontainers.DockerClientFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;

/**
 * {@link RyukResourceReaper}
 */
class RyukContainer extends GenericContainer<RyukContainer> {

    RyukContainer() {
        // [BEGIN] modify image name.
        // see:testcontainers/ryuk:0.5.1
        // see:registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/testcontainers_ryuk:0.5.1
        super(System.getenv().getOrDefault("IT_TEST_CONTAINER_RYUK_IMAGE_NAME",
                "registry.cn-shenzhen.aliyuncs.com/wl4g-k8s/testcontainers_ryuk:0.5.1"));
        // [END] modify image name.
        withExposedPorts(8080);
        withCreateContainerCmdModifier(cmd -> {
            cmd.withName("testcontainers-ryuk-" + DockerClientFactory.SESSION_ID);
            cmd.withHostConfig(
                    cmd.getHostConfig()
                            .withAutoRemove(true)
                            .withPrivileged(TestcontainersConfiguration.getInstance().isRyukPrivileged())
                            .withBinds(
                                    new Bind(
                                            DockerClientFactory.instance().getRemoteDockerUnixSocketPath(),
                                            new Volume("/var/run/docker.sock")
                                    )
                            )
            );
        });

        waitingFor(Wait.forLogMessage(".*Started.*", 1));
    }
}
