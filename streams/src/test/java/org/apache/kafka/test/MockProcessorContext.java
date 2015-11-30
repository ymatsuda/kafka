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

package org.apache.kafka.test;

import org.apache.kafka.streams.StreamingMetrics;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.StateRestoreCallback;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.processor.TaskId;
import org.apache.kafka.streams.processor.internals.RecordCollector;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public class MockProcessorContext implements ProcessorContext, RecordCollector.Supplier {

    private final KStreamTestDriver driver;
    private final Serializer keySerializer;
    private final Serializer valueSerializer;
    private final Deserializer keyDeserializer;
    private final Deserializer valueDeserializer;
    private final RecordCollector.Supplier recordCollectorSupplier;

    private Map<String, StateStore> storeMap = new HashMap<>();

    long timestamp = -1L;

    public MockProcessorContext(KStreamTestDriver driver, Serializer<?> serializer, Deserializer<?> deserializer) {
        this(driver, serializer, deserializer, serializer, deserializer, (RecordCollector.Supplier) null);
    }

    public MockProcessorContext(KStreamTestDriver driver, Serializer<?> keySerializer, Deserializer<?> keyDeserializer,
            Serializer<?> valueSerializer, Deserializer<?> valueDeserializer,
            final RecordCollector collector) {
        this(driver, keySerializer, keyDeserializer, valueSerializer, valueDeserializer,
                collector == null ? null : new RecordCollector.Supplier() {
                    @Override
                    public RecordCollector recordCollector() {
                        return collector;
                    }
                });
    }

    public MockProcessorContext(KStreamTestDriver driver, Serializer<?> keySerializer, Deserializer<?> keyDeserializer,
            Serializer<?> valueSerializer, Deserializer<?> valueDeserializer,
            RecordCollector.Supplier collectorSupplier) {
        this.driver = driver;
        this.keySerializer = keySerializer;
        this.valueSerializer = valueSerializer;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
        this.recordCollectorSupplier = collectorSupplier;
    }

    @Override
    public RecordCollector recordCollector() {
        if (recordCollectorSupplier == null) {
            throw new UnsupportedOperationException("No RecordCollector specified");
        }
        return recordCollectorSupplier.recordCollector();
    }

    public void setTime(long timestamp) {
        this.timestamp = timestamp;
    }

    @Override
    public TaskId id() {
        return new TaskId(0, 0);
    }

    @Override
    public Serializer<?> keySerializer() {
        return keySerializer;
    }

    @Override
    public Serializer<?> valueSerializer() {
        return valueSerializer;
    }

    @Override
    public Deserializer<?> keyDeserializer() {
        return keyDeserializer;
    }

    @Override
    public Deserializer<?> valueDeserializer() {
        return valueDeserializer;
    }

    @Override
    public File stateDir() {
        throw new UnsupportedOperationException("stateDir() not supported.");
    }

    @Override
    public StreamingMetrics metrics() {
        throw new UnsupportedOperationException("metrics() not supported.");
    }

    @Override
    public void register(StateStore store, StateRestoreCallback func) {
        if (func != null) throw new UnsupportedOperationException("StateRestoreCallback not supported.");
        storeMap.put(store.name(), store);
    }

    @Override
    public StateStore getStateStore(String name) {
        return storeMap.get(name);
    }

    @Override
    public void schedule(long interval) {
        throw new UnsupportedOperationException("schedule() not supported");
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value) {
        driver.forward(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <K, V> void forward(K key, V value, int childIndex) {
        driver.forward(key, value, childIndex);
    }

    @Override
    public void commit() {
        throw new UnsupportedOperationException("commit() not supported.");
    }

    @Override
    public String topic() {
        throw new UnsupportedOperationException("topic() not supported.");
    }

    @Override
    public int partition() {
        throw new UnsupportedOperationException("partition() not supported.");
    }

    @Override
    public long offset() {
        throw new UnsupportedOperationException("offset() not supported.");
    }

    @Override
    public long timestamp() {
        return this.timestamp;
    }

}
