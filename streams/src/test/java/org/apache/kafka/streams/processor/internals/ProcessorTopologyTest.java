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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.apache.kafka.streams.processor.TopologyBuilder;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.KeyValueStore;
import org.apache.kafka.streams.state.StateUtils;
import org.apache.kafka.streams.state.Stores;
import org.apache.kafka.test.MockProcessorSupplier;
import org.apache.kafka.test.ProcessorTopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.util.Properties;

public class ProcessorTopologyTest {

    private static final Serializer<String> STRING_SERIALIZER = new StringSerializer();
    private static final Deserializer<String> STRING_DESERIALIZER = new StringDeserializer();

    protected static final String INPUT_TOPIC = "input-topic";
    protected static final String OUTPUT_TOPIC_1 = "output-topic-1";
    protected static final String OUTPUT_TOPIC_2 = "output-topic-2";

    private static long timestamp = 1000L;

    private ProcessorTopologyTestDriver driver;
    private StreamingConfig config;

    @Before
    public void setup() {
        // Create a new directory in which we'll put all of the state for this test, enabling running tests in parallel ...
        File localState = StateUtils.tempDir();
        Properties props = new Properties();
        props.setProperty(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
        props.setProperty(StreamingConfig.STATE_DIR_CONFIG, localState.getAbsolutePath());
        props.setProperty(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
        props.setProperty(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.setProperty(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.setProperty(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        this.config = new StreamingConfig(props);
    }

    @After
    public void cleanup() {
        if (driver != null) {
            driver.close();
        }
        driver = null;
    }

    @Test
    public void testTopologyMetadata() {
        final TopologyBuilder builder = new TopologyBuilder();

        builder.addSource("source-1", "topic-1");
        builder.addSource("source-2", "topic-2", "topic-3");
        builder.addProcessor("processor-1", new MockProcessorSupplier<>(), "source-1");
        builder.addProcessor("processor-2", new MockProcessorSupplier<>(), "source-1", "source-2");
        builder.addSink("sink-1", "topic-3", "processor-1");
        builder.addSink("sink-2", "topic-4", "processor-1", "processor-2");

        final ProcessorTopology topology = builder.build(null);

        assertEquals(6, topology.processors().size());

        assertEquals(2, topology.sources().size());

        assertEquals(3, topology.sourceTopics().size());

        assertNotNull(topology.source("topic-1"));

        assertNotNull(topology.source("topic-2"));

        assertNotNull(topology.source("topic-3"));

        assertEquals(topology.source("topic-2"), topology.source("topic-3"));
    }

    @Test
    public void testDrivingSimpleTopology() {
        driver = new ProcessorTopologyTestDriver(config, createSimpleTopology());
        driver.process(INPUT_TOPIC, "key1", "value1", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1");
        assertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.process(INPUT_TOPIC, "key2", "value2", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2");
        assertNoOutputRecord(OUTPUT_TOPIC_2);

        driver.process(INPUT_TOPIC, "key3", "value3", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key4", "value4", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key5", "value5", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNoOutputRecord(OUTPUT_TOPIC_2);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5");
    }

    @Test
    public void testDrivingMultiplexingTopology() {
        driver = new ProcessorTopologyTestDriver(config, createMultiplexingTopology());
        driver.process(INPUT_TOPIC, "key1", "value1", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key1", "value1(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key1", "value1(2)");

        driver.process(INPUT_TOPIC, "key2", "value2", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key2", "value2(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key2", "value2(2)");

        driver.process(INPUT_TOPIC, "key3", "value3", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key4", "value4", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key5", "value5", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key3", "value3(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key4", "value4(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_1, "key5", "value5(1)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key3", "value3(2)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key4", "value4(2)");
        assertNextOutputRecord(OUTPUT_TOPIC_2, "key5", "value5(2)");
    }

    @Test
    public void testDrivingStatefulTopology() {
        String storeName = "entries";
        driver = new ProcessorTopologyTestDriver(config, createStatefulTopology(storeName), storeName);
        driver.process(INPUT_TOPIC, "key1", "value1", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key2", "value2", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key3", "value3", STRING_SERIALIZER, STRING_SERIALIZER);
        driver.process(INPUT_TOPIC, "key1", "value4", STRING_SERIALIZER, STRING_SERIALIZER);
        assertNoOutputRecord(OUTPUT_TOPIC_1);

        KeyValueStore<String, String> store = driver.getKeyValueStore("entries");
        assertEquals("value4", store.get("key1"));
        assertEquals("value2", store.get("key2"));
        assertEquals("value3", store.get("key3"));
        assertNull(store.get("key4"));
    }

    protected void assertNextOutputRecord(String topic, String key, String value) {
        assertProducerRecord(driver.readOutput(topic, STRING_DESERIALIZER, STRING_DESERIALIZER), topic, key, value);
    }

    protected void assertNoOutputRecord(String topic) {
        assertNull(driver.readOutput(topic));
    }

    private void assertProducerRecord(ProducerRecord<String, String> record, String topic, String key, String value) {
        assertEquals(topic, record.topic());
        assertEquals(key, record.key());
        assertEquals(value, record.value());
        // Kafka Streaming doesn't set the partition, so it's always null
        assertNull(record.partition());
    }

    protected TopologyBuilder createSimpleTopology() {
        return new TopologyBuilder().addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC)
                                    .addProcessor("processor", define(new ForwardingProcessor()), "source")
                                    .addSink("sink", OUTPUT_TOPIC_1, "processor");
    }

    protected TopologyBuilder createMultiplexingTopology() {
        return new TopologyBuilder().addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC)
                                    .addProcessor("processor", define(new MultiplexingProcessor(2)), "source")
                                    .addSink("sink1", OUTPUT_TOPIC_1, "processor")
                                    .addSink("sink2", OUTPUT_TOPIC_2, "processor");
    }

    protected TopologyBuilder createStatefulTopology(String storeName) {
        return new TopologyBuilder().addSource("source", STRING_DESERIALIZER, STRING_DESERIALIZER, INPUT_TOPIC)
                                    .addProcessor("processor", define(new StatefulProcessor(storeName)), "source")
                                    .addStateStore(
                                            Stores.create(storeName).withStringKeys().withStringValues().inMemory().build(),
                                            "processor"
                                    )
                                    .addSink("counts", OUTPUT_TOPIC_1, "processor");
    }

    /**
     * A processor that simply forwards all messages to all children.
     */
    protected static class ForwardingProcessor extends AbstractProcessor<String, String> {

        @Override
        public void process(String key, String value) {
            context().forward(key, value);
        }

        @Override
        public void punctuate(long streamTime) {
            context().forward(Long.toString(streamTime), "punctuate");
        }
    }

    /**
     * A processor that forwards slightly-modified messages to each child.
     */
    protected static class MultiplexingProcessor extends AbstractProcessor<String, String> {

        private final int numChildren;

        public MultiplexingProcessor(int numChildren) {
            this.numChildren = numChildren;
        }

        @Override
        public void process(String key, String value) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(key, value + "(" + (i + 1) + ")", i);
            }
        }

        @Override
        public void punctuate(long streamTime) {
            for (int i = 0; i != numChildren; ++i) {
                context().forward(Long.toString(streamTime), "punctuate(" + (i + 1) + ")", i);
            }
        }
    }

    /**
     * A processor that stores each key-value pair in an in-memory key-value store registered with the context. When
     * {@link #punctuate(long)} is called, it outputs the total number of entries in the store.
     */
    protected static class StatefulProcessor extends AbstractProcessor<String, String> {

        private KeyValueStore<String, String> store;
        private final String storeName;

        public StatefulProcessor(String storeName) {
            this.storeName = storeName;
        }

        @Override
        @SuppressWarnings("unchecked")
        public void init(ProcessorContext context) {
            super.init(context);
            store = (KeyValueStore<String, String>) context.getStateStore(storeName);
        }

        @Override
        public void process(String key, String value) {
            store.put(key, value);
        }

        @Override
        public void punctuate(long streamTime) {
            int count = 0;
            for (KeyValueIterator<String, String> iter = store.all(); iter.hasNext();) {
                iter.next();
                ++count;
            }
            context().forward(Long.toString(streamTime), count);
        }

        @Override
        public void close() {
            store.close();
        }
    }

    protected <K, V> ProcessorSupplier<K, V> define(final Processor<K, V> processor) {
        return new ProcessorSupplier<K, V>() {
            @Override
            public Processor<K, V> get() {
                return processor;
            }
        };
    }

    public static class CustomTimestampExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record) {
            return timestamp;
        }
    }
}
