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
package org.apache.kafka.coordinator.group.streams.topics;

import org.apache.kafka.common.Uuid;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopic;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfig;
import org.apache.kafka.common.message.CreateTopicsRequestData.CreatableTopicConfigCollection;
import org.apache.kafka.common.utils.LogContext;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue;
import org.apache.kafka.coordinator.group.generated.StreamsGroupTopologyValue.Subtopology;
import org.apache.kafka.coordinator.group.streams.TopicMetadata;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.apache.kafka.common.utils.Utils.mkEntry;
import static org.apache.kafka.common.utils.Utils.mkMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

class InternalTopicManagerTest {

    @Test
    void testMissingTopics() {
        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put("source_topic1", new TopicMetadata(Uuid.randomUuid(), "source_topic1", 2, Collections.emptyMap()));
        topicMetadata.put("source_topic2", new TopicMetadata(Uuid.randomUuid(), "source_topic2", 2, Collections.emptyMap()));
        topicMetadata.put("state_changelog_topic2",
            new TopicMetadata(Uuid.randomUuid(), "state_changelog_topic2", 2, Collections.emptyMap()));
        Map<String, ConfiguredSubtopology> subtopologyMap = makeExpectedConfiguredTopology();

        Map<String, CreatableTopic> missingTopics = InternalTopicManager.missingTopics(subtopologyMap, topicMetadata);

        assertEquals(2, missingTopics.size());
        assertEquals(
            new CreatableTopic()
                .setName("repartition_topic")
                .setNumPartitions(2)
                .setReplicationFactor((short) 3),
            missingTopics.get("repartition_topic")
        );
        assertEquals(
            new CreatableTopic()
                .setName("state_changelog_topic1")
                .setNumPartitions(2)
                .setReplicationFactor((short) -1)
                .setConfigs(
                    new CreatableTopicConfigCollection(
                        Collections.singletonList(new CreatableTopicConfig().setName("cleanup.policy").setValue("compact")).iterator())
                ),
            missingTopics.get("state_changelog_topic1"));
    }

    @Test
    void testConfigureTopics() {
        Map<String, TopicMetadata> topicMetadata = new HashMap<>();
        topicMetadata.put("source_topic1", new TopicMetadata(Uuid.randomUuid(), "source_topic1", 2, Collections.emptyMap()));
        topicMetadata.put("source_topic2", new TopicMetadata(Uuid.randomUuid(), "source_topic2", 2, Collections.emptyMap()));
        List<Subtopology> subtopologyList = makeTestTopology();

        Map<String, ConfiguredSubtopology> configuredSubtopologies =
            InternalTopicManager.configureTopics(new LogContext(), subtopologyList, topicMetadata);

        Map<String, ConfiguredSubtopology> expectedConfiguredSubtopologyMap = makeExpectedConfiguredTopology();
        assertEquals(expectedConfiguredSubtopologyMap, configuredSubtopologies);
    }

    private static Map<String, ConfiguredSubtopology> makeExpectedConfiguredTopology() {
        return mkMap(
            mkEntry("subtopology1",
                new ConfiguredSubtopology()
                    .setSourceTopics(Set.of("source_topic1"))
                    .setStateChangelogTopics(Collections.singletonMap("state_changelog_topic1",
                        new ConfiguredInternalTopic("state_changelog_topic1",
                            Collections.singletonMap("cleanup.policy", "compact"),
                            Optional.empty(),
                            Optional.empty()
                        ).setNumberOfPartitions(2)))
                    .setRepartitionSinkTopics(Set.of("repartition_topic"))
            ),
            mkEntry("subtopology2",
                new ConfiguredSubtopology()
                    .setSourceTopics(Set.of("source_topic2"))
                    .setRepartitionSourceTopics(Collections.singletonMap("repartition_topic",
                        new ConfiguredInternalTopic("repartition_topic",
                            Collections.emptyMap(),
                            Optional.empty(),
                            Optional.of((short) 3)
                        ).setNumberOfPartitions(2)
                    ))
                    .setStateChangelogTopics(Collections.singletonMap("state_changelog_topic2",
                        new ConfiguredInternalTopic("state_changelog_topic2",
                            Collections.emptyMap(),
                            Optional.empty(),
                            Optional.empty()
                        ).setNumberOfPartitions(2)))
            )
        );
    }

    private static List<Subtopology> makeTestTopology() {
        // Create a subtopology source -> repartition
        Subtopology subtopology1 = new Subtopology()
            .setSubtopologyId("subtopology1")
            .setSourceTopics(Collections.singletonList("source_topic1"))
            .setRepartitionSinkTopics(Collections.singletonList("repartition_topic"))
            .setStateChangelogTopics(Collections.singletonList(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName("state_changelog_topic1")
                    .setTopicConfigs(Collections.singletonList(
                        new StreamsGroupTopologyValue.TopicConfig()
                            .setKey("cleanup.policy")
                            .setValue("compact")
                    ))
            ));
        // Create a subtopology repartition/source2 -> sink (copartitioned)
        Subtopology subtopology2 = new Subtopology()
            .setSubtopologyId("subtopology2")
            .setSourceTopics(Collections.singletonList("source_topic2"))
            .setRepartitionSourceTopics(Collections.singletonList(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName("repartition_topic")
                    .setReplicationFactor((short) 3)
            ))
            .setStateChangelogTopics(Collections.singletonList(
                new StreamsGroupTopologyValue.TopicInfo()
                    .setName("state_changelog_topic2")
            ))
            .setCopartitionGroups(Collections.singletonList(
                new StreamsGroupTopologyValue.CopartitionGroup()
                    .setSourceTopics(Collections.singletonList((short) 0))
                    .setRepartitionSourceTopics(Collections.singletonList((short) 0))
            ));
        return Arrays.asList(subtopology1, subtopology2);
    }

}