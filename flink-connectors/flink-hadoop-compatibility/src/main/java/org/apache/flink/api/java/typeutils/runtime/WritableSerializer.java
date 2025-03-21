/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.api.java.typeutils.runtime;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.GenericTypeSerializerSnapshot;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.util.InstantiationUtil;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.util.DefaultInstantiatorStrategy;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Writable;
import org.objenesis.strategy.StdInstantiatorStrategy;

import java.io.IOException;

/**
 * A {@link TypeSerializer} for {@link Writable}.
 *
 * @param <T>
 */
@Internal
public final class WritableSerializer<T extends Writable> extends TypeSerializer<T> {

    private static final long serialVersionUID = 1L;

    private final Class<T> typeClass;

    private transient Kryo kryo;

    private transient T copyInstance;

    public WritableSerializer(Class<T> typeClass) {
        this.typeClass = typeClass;
    }

    @SuppressWarnings("unchecked")
    @Override
    public T createInstance() {
        if (typeClass == NullWritable.class) {
            return (T) NullWritable.get();
        }
        return InstantiationUtil.instantiate(typeClass);
    }

    @Override
    public T copy(T from) {
        checkKryoInitialized();

        return KryoUtils.copy(from, kryo, this);
    }

    @Override
    public T copy(T from, T reuse) {
        checkKryoInitialized();

        return KryoUtils.copy(from, reuse, kryo, this);
    }

    @Override
    public int getLength() {
        return -1;
    }

    @Override
    public void serialize(T record, DataOutputView target) throws IOException {
        record.write(target);
    }

    @Override
    public T deserialize(DataInputView source) throws IOException {
        return deserialize(createInstance(), source);
    }

    @Override
    public T deserialize(T reuse, DataInputView source) throws IOException {
        reuse.readFields(source);
        return reuse;
    }

    @Override
    public void copy(DataInputView source, DataOutputView target) throws IOException {
        ensureInstanceInstantiated();
        copyInstance.readFields(source);
        copyInstance.write(target);
    }

    @Override
    public boolean isImmutableType() {
        return false;
    }

    @Override
    public WritableSerializer<T> duplicate() {
        return new WritableSerializer<T>(typeClass);
    }

    // --------------------------------------------------------------------------------------------

    private void ensureInstanceInstantiated() {
        if (copyInstance == null) {
            copyInstance = createInstance();
        }
    }

    private void checkKryoInitialized() {
        if (this.kryo == null) {
            this.kryo = new Kryo();

            DefaultInstantiatorStrategy instantiatorStrategy = new DefaultInstantiatorStrategy();
            instantiatorStrategy.setFallbackInstantiatorStrategy(new StdInstantiatorStrategy());
            kryo.setInstantiatorStrategy(instantiatorStrategy);

            this.kryo.register(typeClass);
        }
    }

    // --------------------------------------------------------------------------------------------

    @Override
    public int hashCode() {
        return this.typeClass.hashCode();
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof WritableSerializer) {
            WritableSerializer<?> other = (WritableSerializer<?>) obj;

            return typeClass == other.typeClass;
        } else {
            return false;
        }
    }

    // --------------------------------------------------------------------------------------------
    // Serializer configuration snapshotting & compatibility
    // --------------------------------------------------------------------------------------------

    @Override
    public TypeSerializerSnapshot<T> snapshotConfiguration() {
        return new WritableSerializerSnapshot<>(typeClass);
    }

    /** {@link WritableSerializer} snapshot class. */
    @Internal
    public static final class WritableSerializerSnapshot<T extends Writable>
            extends GenericTypeSerializerSnapshot<T, WritableSerializer> {

        @SuppressWarnings("unused")
        public WritableSerializerSnapshot() {}

        WritableSerializerSnapshot(Class<T> typeClass) {
            super(typeClass);
        }

        @Override
        protected TypeSerializer<T> createSerializer(Class<T> typeClass) {
            return new WritableSerializer<>(typeClass);
        }

        @SuppressWarnings("unchecked")
        @Override
        protected Class<T> getTypeClass(WritableSerializer serializer) {
            return serializer.typeClass;
        }

        @Override
        protected Class<?> serializerClass() {
            return WritableSerializer.class;
        }
    }
}
