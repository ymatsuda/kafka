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

package org.apache.kafka.streams.kstream.internals;

import org.apache.kafka.streams.kstream.ValueJoiner;
import org.apache.kafka.streams.processor.AbstractProcessor;
import org.apache.kafka.streams.processor.Processor;
import org.apache.kafka.streams.processor.ProcessorContext;

class KTableKTableOuterJoin<K, V, V1, V2> implements KTableProcessorSupplier<K, V1, V> {

    private final KTableValueGetterSupplier<K, V1> valueGetterSupplier1;
    private final KTableValueGetterSupplier<K, V2> valueGetterSupplier2;
    private final ValueJoiner<V1, V2, V> joiner;

    KTableKTableOuterJoin(KTableImpl<K, ?, V1> table1,
                          KTableImpl<K, ?, V2> table2,
                          ValueJoiner<V1, V2, V> joiner) {
        this.valueGetterSupplier1 = table1.valueGetterSupplier();
        this.valueGetterSupplier2 = table2.valueGetterSupplier();
        this.joiner = joiner;
    }

    @Override
    public Processor<K, V1> get() {
        return new KTableKTableOuterJoinProcessor(valueGetterSupplier2.get());
    }

    @Override
    public KTableValueGetterSupplier<K, V> view() {
        return new KTableValueGetterSupplier<K, V>() {

            public KTableValueGetter<K, V> get() {
                return new KTableKTableOuterJoinValueGetter(valueGetterSupplier1.get(), valueGetterSupplier2.get());
            }

        };
    }

    private class KTableKTableOuterJoinProcessor extends AbstractProcessor<K, V1> {

        private final KTableValueGetter<K, V2> valueGetter;

        public KTableKTableOuterJoinProcessor(KTableValueGetter<K, V2> valueGetter) {
            this.valueGetter = valueGetter;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void init(ProcessorContext context) {
            super.init(context);
            valueGetter.init(context);
        }

        @Override
        public void process(K key, V1 value1) {
            V newValue = null;
            V2 value2 = valueGetter.get(key);

            if (value1 != null || value2 != null)
                newValue = joiner.apply(value1, value2);

            context().forward(key, newValue);
        }
    }

    private class KTableKTableOuterJoinValueGetter implements KTableValueGetter<K, V> {

        private final KTableValueGetter<K, V1> valueGetter1;
        private final KTableValueGetter<K, V2> valueGetter2;

        public KTableKTableOuterJoinValueGetter(KTableValueGetter<K, V1> valueGetter1, KTableValueGetter<K, V2> valueGetter2) {
            this.valueGetter1 = valueGetter1;
            this.valueGetter2 = valueGetter2;
        }

        @Override
        public void init(ProcessorContext context) {
            valueGetter1.init(context);
            valueGetter2.init(context);
        }

        @Override
        public V get(K key) {
            V newValue = null;
            V1 value1 = valueGetter1.get(key);
            V2 value2 = valueGetter2.get(key);

            if (value1 != null || value2 != null)
                newValue = joiner.apply(value1, value2);

            return newValue;
        }

    }

}
