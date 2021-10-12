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

package ai.djl.dlr.engine;

import ai.djl.dlr.jni.JniUtils;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.AbstractSymbolBlock;
import ai.djl.nn.SymbolBlock;
import ai.djl.training.ParameterStore;
import ai.djl.util.PairList;
import java.util.concurrent.atomic.AtomicReference;

/**
 * {@code DlrSymbolBlock} is the DLR implementation of {@link SymbolBlock}.
 *
 * <p>You can create a {@code DlrSymbolBlock} using {@link ai.djl.Model#load(java.nio.file.Path,
 * String)}.
 */
public class DlrSymbolBlock extends AbstractSymbolBlock implements AutoCloseable {

    private static final byte VERSION = 1;

    private AtomicReference<Long> handle;

    /**
     * Constructs a {@code DlrSymbolBlock}.
     *
     * <p>You can create a {@code DlrSymbolBlock} using {@link ai.djl.Model#load(java.nio.file.Path,
     * String)}.
     *
     * @param handle the handle for native DLR model
     */
    public DlrSymbolBlock(long handle) {
        super(VERSION);
        this.handle = new AtomicReference<>(handle);
    }

    /** {@inheritDoc} */
    @Override
    protected NDList forwardInternal(
            ParameterStore parameterStore,
            NDList inputs,
            boolean training,
            PairList<String, Object> params) {
        long modelHandle = handle.get();
        NDManager manager = inputs.head().getManager();
        // TODO maybe verify the number of inputs
        // currently we assume the order of the input NDList is the same
        // as the model input
        for (int i = 0; i < inputs.size(); ++i) {
            JniUtils.setDlrInput(modelHandle, inputs.get(i), i);
        }
        JniUtils.runDlrModel(modelHandle);
        return JniUtils.getDlrOutputs(modelHandle, manager);
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        Long pointer = handle.getAndSet(null);
        if (pointer != null) {
            JniUtils.deleteDlrModel(pointer);
        }
    }
}
