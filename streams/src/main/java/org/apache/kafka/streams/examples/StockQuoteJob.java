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

package org.apache.kafka.streams.examples;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KafkaStreaming;
import org.apache.kafka.streams.StreamingConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.ValueMapper;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.ProcessorSupplier;
import org.apache.kafka.streams.processor.TimestampExtractor;

import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Properties;

/**
 * Created by yasuhiro on 10/7/15.
 */
public class StockQuoteJob {

    public static class Quote {
        final long date;
        final double open;
        final double high;
        final double low;
        final double close;
        final double volume;

        Quote(long date, double open, double high, double low, double close, double volume) {
            this.date = date;
            this.open = open;
            this.high = high;
            this.low = low;
            this.close = close;
            this.volume = volume;
        }

        public String toString() {
            Date d = new Date(date);

            return "(DATE: " + d.toString() +
                    " OPEN: " + open +
                    " HIGH: " + high +
                    " LOW: " + low +
                    " CLOSE: " + close +
                    " VOLUME: " + volume + ")";
        }
    }

    public static class QuoteProcessorSupplier implements ProcessorSupplier<String, Quote> {

        @Override
        public Processor<String, Quote> get() {
            return new Processor<String, Quote>() {

                private final long startTime = System.currentTimeMillis();
                private int count = 0;
                private final String threadName = Thread.currentThread().getName();

                @Override
                public void init(ProcessorContext context) {

                }

                @Override
                public void process(String key, Quote value) {
                    count++;
                    System.out.println((System.currentTimeMillis() - startTime) + "@" + count + " : " + key + " " + value.toString() + " thread: " + threadName);
                }

                @Override
                public void punctuate(long timestamp) {

                }

                @Override
                public void close() {

                }
            };
        }
    }

    public static class QuoteTimeExtractor implements TimestampExtractor {
        @Override
        public long extract(ConsumerRecord<Object, Object> record) {
            if (record.topic().equals("stockquote")) {
                String[] values = ((String) record.value()).split(",");
                return toDatetime(values[0]);
            } else {
                throw new IllegalArgumentException("unsupported topic: " + record.topic());
            }
        }
    }

    private static Quote toQuote(String csv) {
        String[] values = csv.split(",");
        return new Quote(
                toDatetime(values[0]),
                Double.parseDouble(values[2]),
                Double.parseDouble(values[3]),
                Double.parseDouble(values[4]),
                Double.parseDouble(values[5]),
                Double.parseDouble(values[6])
        );
    }

    private static long toDatetime(String yyyymmdd) {
        Calendar cal = new GregorianCalendar();
        int year = Integer.parseInt(yyyymmdd.substring(0, 4));
        int month = Integer.parseInt(yyyymmdd.substring(4, 6));
        int date = Integer.parseInt(yyyymmdd.substring(6, 8));
        cal.set(year, month, date);

        return cal.getTimeInMillis();
    }

    class VVMapper implements ValueMapper<String, Void> {
        @Override
        public Void apply(String value) {
            return null;
        }
    }

    public static void main(String[] args) throws Exception {
        Properties props = new Properties();
        props.put(StreamingConfig.CLIENT_ID_CONFIG, "Example-StockQuote-Job");
        props.put(StreamingConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(StreamingConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(StreamingConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(StreamingConfig.TIMESTAMP_EXTRACTOR_CLASS_CONFIG, QuoteTimeExtractor.class);
        props.put(StreamingConfig.NUM_STREAM_THREADS_CONFIG, 3);
        props.put(StreamingConfig.NUM_STANDBY_REPLICAS_CONFIG, 1);

        //props.put(ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG, 0);
        props.put(ConsumerConfig.MAX_PARTITION_FETCH_BYTES_CONFIG, 300000);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "Example-StockQuote-Job");
        props.put(StreamingConfig.BUFFERED_RECORDS_PER_PARTITION_CONFIG, 1000);

        StreamingConfig config = new StreamingConfig(props);

        KStreamBuilder builder = new KStreamBuilder();

        KStream<String, String> stream = builder.from("stockquote");

        stream.mapValues(new ValueMapper<String, Quote>() {
            @Override
            public Quote apply(String value) {
                return toQuote(value);
            }
        }).process(new QuoteProcessorSupplier());

        KafkaStreaming kstream = new KafkaStreaming(builder, config);

        kstream.start();
    }

}
