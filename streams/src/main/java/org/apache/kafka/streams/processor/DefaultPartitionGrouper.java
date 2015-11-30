/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.kafka.streams.processor;

import org.apache.kafka.common.Cluster;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class DefaultPartitionGrouper extends PartitionGrouper {

    public Map<TaskId, Set<TopicPartition>> partitionGroups(Cluster metadata) {
        Map<TaskId, Set<TopicPartition>> groups = new HashMap<>();

        for (Map.Entry<Integer, Set<String>> entry : topicGroups.entrySet()) {
            Integer topicGroupId = entry.getKey();
            Set<String> topicGroup = entry.getValue();

            int maxNumPartitions = maxNumPartitions(metadata, topicGroup);

            for (int partitionId = 0; partitionId < maxNumPartitions; partitionId++) {
                Set<TopicPartition> group = new HashSet<>(topicGroup.size());

                for (String topic : topicGroup) {
                    if (partitionId < metadata.partitionsForTopic(topic).size()) {
                        group.add(new TopicPartition(topic, partitionId));
                    }
                }
                groups.put(new TaskId(topicGroupId, partitionId), Collections.unmodifiableSet(group));
            }
        }

        return Collections.unmodifiableMap(groups);
    }

    protected int maxNumPartitions(Cluster metadata, Set<String> topics) {
        int maxNumPartitions = 0;
        for (String topic : topics) {
            List<PartitionInfo> infos = metadata.partitionsForTopic(topic);

            if (infos == null)
                throw new KafkaException("topic not found :" + topic);

            int numPartitions = infos.size();
            if (numPartitions > maxNumPartitions)
                maxNumPartitions = numPartitions;
        }
        return maxNumPartitions;
    }

}
