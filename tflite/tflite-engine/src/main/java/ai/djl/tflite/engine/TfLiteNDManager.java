/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"). You may not use this file except in compliance
 * with the License. A copy of the License is located at
 *
 * http://aws.amazon.com/apache2.0/
 *
 * or in the "license" file accompanying this file. This file is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES
 * OR CONDITIONS OF ANY KIND, either express or implied. See the License for the specific language governing permissions
 * and limitations under the License.
 */
package ai.djl.tflite.engine;

import ai.djl.Device;
import ai.djl.engine.Engine;
import ai.djl.ndarray.BaseNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import java.nio.Buffer;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.DoubleBuffer;
import java.nio.FloatBuffer;
import java.nio.IntBuffer;
import java.nio.LongBuffer;

/** {@code TfLiteNDManager} is the TFLite implementation of {@link NDManager}. */
public class TfLiteNDManager extends BaseNDManager {

    private static final TfLiteNDManager SYSTEM_MANAGER = new SystemManager();

    private TfLiteNDManager(NDManager parent, Device device) {
        super(parent, device);
    }

    static TfLiteNDManager getSystemManager() {
        return SYSTEM_MANAGER;
    }

    /** {@inheritDoc} */
    @Override
    public ByteBuffer allocateDirect(int capacity) {
        return ByteBuffer.allocateDirect(capacity).order(ByteOrder.nativeOrder());
    }

    /** {@inheritDoc} */
    @Override
    public TfLiteNDArray create(Buffer data, Shape shape, DataType dataType) {
        if (data.isDirect() && data instanceof ByteBuffer) {
            return new TfLiteNDArray(this, (ByteBuffer) data, shape, dataType);
        }

        int size = data.remaining();
        // int8, uint8, boolean use ByteBuffer, so need to explicitly input DataType
        DataType inputType = DataType.fromBuffer(data);

        int numOfBytes = inputType.getNumOfBytes();
        ByteBuffer buf = allocateDirect(size * numOfBytes);

        switch (inputType) {
            case FLOAT32:
                buf.asFloatBuffer().put((FloatBuffer) data);
                break;
            case FLOAT64:
                buf.asDoubleBuffer().put((DoubleBuffer) data);
                break;
            case UINT8:
            case INT8:
            case BOOLEAN:
                buf.put((ByteBuffer) data);
                break;
            case INT32:
                buf.asIntBuffer().put((IntBuffer) data);
                break;
            case INT64:
                buf.asLongBuffer().put((LongBuffer) data);
                break;
            case FLOAT16:
            default:
                throw new AssertionError("Show never happen");
        }
        buf.rewind();
        return new TfLiteNDArray(this, buf, shape, dataType);
    }

    /** {@inheritDoc} */
    @Override
    public NDArray zeros(Shape shape, DataType dataType) {
        int bytes = dataType.getNumOfBytes();
        int size = Math.toIntExact(bytes * shape.size());
        ByteBuffer buffer = allocateDirect(size);
        return create(dataType.asDataType(buffer), shape, dataType);
    }

    /** {@inheritDoc} */
    @Override
    public NDArray ones(Shape shape, DataType dataType) {
        long size = shape.size();
        int bytes = Math.toIntExact(dataType.getNumOfBytes() * size);
        ByteBuffer buffer = allocateDirect(bytes);
        for (int i = 0; i < size; ++i) {
            switch (dataType) {
                case BOOLEAN:
                case INT8:
                case UINT8:
                    buffer.put((byte) 1);
                    break;
                case FLOAT32:
                    buffer.putFloat(1f);
                    break;
                case FLOAT64:
                    buffer.putDouble(1);
                    break;
                case INT32:
                    buffer.putInt(1);
                    break;
                case INT64:
                    buffer.putLong(1);
                    break;
                default:
                    throw new UnsupportedOperationException("Unsupported dataType: " + dataType);
            }
        }
        buffer.rewind();
        return create(dataType.asDataType(buffer), shape, dataType);
    }

    /** {@inheritDoc} */
    @Override
    public TfLiteNDManager newSubManager(Device device) {
        TfLiteNDManager manager = new TfLiteNDManager(this, device);
        attachInternal(manager.uid, manager);
        return manager;
    }

    /** {@inheritDoc} */
    @Override
    public final Engine getEngine() {
        return Engine.getEngine(TfLiteEngine.ENGINE_NAME);
    }

    /** The SystemManager is the root {@link TfLiteNDManager} of which all others are children. */
    private static final class SystemManager extends TfLiteNDManager {

        SystemManager() {
            super(null, null);
        }

        /** {@inheritDoc} */
        @Override
        public void attachInternal(String resourceId, AutoCloseable resource) {}

        /** {@inheritDoc} */
        @Override
        public void detachInternal(String resourceId) {}

        /** {@inheritDoc} */
        @Override
        public void close() {}
    }
}
