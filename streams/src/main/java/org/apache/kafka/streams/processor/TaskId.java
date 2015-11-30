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

import java.nio.ByteBuffer;

public class TaskId implements Comparable<TaskId> {

    public final int topicGroupId;
    public final int partition;

    public TaskId(int topicGroupId, int partition) {
        this.topicGroupId = topicGroupId;
        this.partition = partition;
    }

    public String toString() {
        return topicGroupId + "_" + partition;
    }

    public static TaskId parse(String string) {
        int index = string.indexOf('_');
        if (index <= 0 || index + 1 >= string.length()) throw new TaskIdFormatException();

        try {
            int topicGroupId = Integer.parseInt(string.substring(0, index));
            int partition = Integer.parseInt(string.substring(index + 1));

            return new TaskId(topicGroupId, partition);
        } catch (Exception e) {
            throw new TaskIdFormatException();
        }
    }

    public void writeTo(ByteBuffer buf) {
        buf.putInt(topicGroupId);
        buf.putInt(partition);
    }

    public static TaskId readFrom(ByteBuffer buf) {
        return new TaskId(buf.getInt(), buf.getInt());
    }

    @Override
    public boolean equals(Object o) {
        if (o instanceof TaskId) {
            TaskId other = (TaskId) o;
            return other.topicGroupId == this.topicGroupId && other.partition == this.partition;
        } else {
            return false;
        }
    }

    @Override
    public int hashCode() {
        long n = ((long) topicGroupId << 32) | (long) partition;
        return (int) (n % 0xFFFFFFFFL);
    }

    @Override
    public int compareTo(TaskId other) {
        return
            this.topicGroupId < other.topicGroupId ? -1 :
                (this.topicGroupId > other.topicGroupId ? 1 :
                    (this.partition < other.partition ? -1 :
                        (this.partition > other.partition ? 1 :
                            0)));
    }

    public static class TaskIdFormatException extends RuntimeException {
    }
}
