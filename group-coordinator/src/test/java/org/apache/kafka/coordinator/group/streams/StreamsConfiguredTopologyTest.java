/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.coordinator.group.streams;

import org.apache.kafka.common.message.StreamsGroupDescribeResponseData;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredInternalTopic;
import org.apache.kafka.coordinator.group.streams.topics.ConfiguredSubtopology;
import org.junit.jupiter.api.Test;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class StreamsConfiguredTopologyTest {

    @Test
    public void streamsTopologyIdShouldBeCorrect() {
        StreamsConfiguredTopology topology = new StreamsConfiguredTopology("topology-id", Collections.emptyMap(), Collections.emptyMap());
        assertEquals("topology-id", topology.topologyId());
    }

    @Test
    public void subtopologiesShouldBeCorrect() {
        Map<String, ConfiguredSubtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new ConfiguredSubtopology()),
            mkEntry("subtopology-2", new ConfiguredSubtopology())
        );
        StreamsConfiguredTopology topology = new StreamsConfiguredTopology("topology-id", subtopologies, Collections.emptyMap());
        assertEquals(subtopologies, topology.subtopologies());
    }

    @Test
    public void asStreamsGroupDescribeTopologyShouldReturnCorrectSubtopologies() {
        Map<String, ConfiguredSubtopology> subtopologies = mkMap(
            mkEntry("subtopology-1", new ConfiguredSubtopology()
                .setSourceTopics(Collections.singleton("source-topic-1"))
                .setRepartitionSinkTopics(Collections.singleton("sink-topic-1"))
                .setRepartitionSourceTopics(
                    Collections.singletonMap("repartition-topic-1", new ConfiguredInternalTopic("repartition-topic-1", Collections.emptyMap(), Optional.of(2), Optional.of((short) 3))))
                .setStateChangelogTopics(
                    Collections.singletonMap("changelog-topic-1", new ConfiguredInternalTopic("changelog-topic-1", Collections.emptyMap(), Optional.of(1), Optional.of((short) 2))))
            ),
            mkEntry("subtopology-2", new ConfiguredSubtopology()
                .setSourceTopics(Collections.singleton("source-topic-2"))
                .setRepartitionSinkTopics(Collections.singleton("sink-topic-2"))
                .setRepartitionSourceTopics(
                    Collections.singletonMap("repartition-topic-2", new ConfiguredInternalTopic("repartition-topic-2", Collections.emptyMap(), Optional.of(2), Optional.of((short) 3))))
                .setStateChangelogTopics(
                    Collections.singletonMap("changelog-topic-2", new ConfiguredInternalTopic("changelog-topic-2", Collections.emptyMap(), Optional.of(1), Optional.of((short) 2))))
            )
        );
        StreamsConfiguredTopology topology = new StreamsConfiguredTopology("topology-id", subtopologies, Collections.emptyMap());
        List<StreamsGroupDescribeResponseData.Subtopology> result = topology.asStreamsGroupDescribeTopology();
        assertEquals(2, result.size());
        assertEquals("subtopology-1", result.get(0).subtopologyId());
        assertEquals(Collections.singletonList("source-topic-1"), result.get(0).sourceTopics());
        assertEquals(Collections.singletonList("sink-topic-1"), result.get(0).repartitionSinkTopics());
        assertEquals("repartition-topic-1", result.get(0).repartitionSourceTopics().get(0).name());
        assertEquals((short) 3, result.get(0).repartitionSourceTopics().get(0).replicationFactor());
        assertEquals(2, result.get(0).repartitionSourceTopics().get(0).partitions());
        assertEquals("changelog-topic-1", result.get(0).stateChangelogTopics().get(0).name());
        assertEquals((short) 2, result.get(0).stateChangelogTopics().get(0).replicationFactor());
        assertEquals(1, result.get(0).stateChangelogTopics().get(0).partitions());
        assertEquals("subtopology-2", result.get(1).subtopologyId());
        assertEquals(Collections.singletonList("source-topic-2"), result.get(1).sourceTopics());
        assertEquals(Collections.singletonList("sink-topic-2"), result.get(1).repartitionSinkTopics());
        assertEquals("repartition-topic-2", result.get(1).repartitionSourceTopics().get(0).name());
        assertEquals((short) 3, result.get(1).repartitionSourceTopics().get(0).replicationFactor());
        assertEquals(2, result.get(1).repartitionSourceTopics().get(0).partitions());
        assertEquals("changelog-topic-2", result.get(1).stateChangelogTopics().get(0).name());
        assertEquals((short) 2, result.get(1).stateChangelogTopics().get(0).replicationFactor());
        assertEquals(1, result.get(1).stateChangelogTopics().get(0).partitions());
    }
}
