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

package org.apache.kafka.streams.processor.internals;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Serializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class RecordCollector {

    /**
     * A supplier of a {@link RecordCollector} instance.
     */
    public static interface Supplier {
        /**
         * Get the record collector.
         * @return the record collector
         */
        public RecordCollector recordCollector();
    }

    private static final Logger log = LoggerFactory.getLogger(RecordCollector.class);

    private final Producer<byte[], byte[]> producer;
    private final Map<TopicPartition, Long> offsets;
    private final Callback callback = new Callback() {
        public void onCompletion(RecordMetadata metadata, Exception exception) {
            if (exception == null) {
                TopicPartition tp = new TopicPartition(metadata.topic(), metadata.partition());
                offsets.put(tp, metadata.offset());
            } else {
                log.error("Error sending record: ", exception);
            }
        }
    };


    public RecordCollector(Producer<byte[], byte[]> producer) {
        this.producer = producer;
        this.offsets = new HashMap<>();
    }

    public <K, V> void send(ProducerRecord<K, V> record, Serializer<K> keySerializer, Serializer<V> valueSerializer) {
        byte[] keyBytes = keySerializer.serialize(record.topic(), record.key());
        byte[] valBytes = valueSerializer.serialize(record.topic(), record.value());
        this.producer.send(new ProducerRecord<>(record.topic(), keyBytes, valBytes), callback);
    }

    public void flush() {
        this.producer.flush();
    }

    /**
     * Closes this RecordCollector
     */
    public void close() {
        producer.close();
    }

    /**
     * The last ack'd offset from the producer
     *
     * @return the map from TopicPartition to offset
     */
    Map<TopicPartition, Long> offsets() {
        return this.offsets;
    }
}
