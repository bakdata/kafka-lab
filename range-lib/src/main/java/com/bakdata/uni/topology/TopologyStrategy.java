/*
 *    Copyright 2022 bakdata GmbH
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *        http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

package com.bakdata.uni.topology;

import org.apache.kafka.streams.kstream.KStream;

/**
 * Strategy for different topologies.
 */
public interface TopologyStrategy {
    /**
     * Includes the implementation logic of the topology.
     */
    <K, V> void create(final KStream<K, V> stream);

}
