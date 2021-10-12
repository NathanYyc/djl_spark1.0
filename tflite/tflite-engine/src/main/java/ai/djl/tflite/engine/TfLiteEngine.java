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
import ai.djl.Model;
import ai.djl.engine.Engine;
import ai.djl.ndarray.NDManager;
import ai.djl.nn.SymbolBlock;
import ai.djl.training.GradientCollector;

/**
 * The {@code TfLiteEngine} is an implementation of the {@link Engine} based on the <a
 * href="https://www.tensorflow.org/lite">TFLite Deep Learning Library</a>.
 *
 * <p>To get an instance of the {@code TfLiteEngine} when it is not the default Engine, call {@link
 * Engine#getEngine(String)} with the Engine name "TFLite".
 */
public final class TfLiteEngine extends Engine {

    public static final String ENGINE_NAME = "TFLite";
    static final int RANK = 10;

    private Engine alternativeEngine;

    private TfLiteEngine() {
        LibUtils.loadLibrary();
    }

    static Engine newInstance() {
        return new TfLiteEngine();
    }

    /** {@inheritDoc} */
    @Override
    public String getEngineName() {
        return ENGINE_NAME;
    }

    /** {@inheritDoc} */
    @Override
    public int getRank() {
        return RANK;
    }

    private Engine getAlternativeEngine() {
        if (Boolean.getBoolean("ai.djl.tflite.disable_alternative")) {
            return null;
        }
        if (alternativeEngine == null) {
            Engine engine = Engine.getInstance();
            if (engine.getRank() < getRank()) {
                // alternativeEngine should not have the same rank as ORT
                alternativeEngine = engine;
            }
        }
        return alternativeEngine;
    }

    /** {@inheritDoc} */
    @Override
    public String getVersion() {
        return "2.4.1";
    }

    /** {@inheritDoc} */
    @Override
    public boolean hasCapability(String capability) {
        // TODO: Support GPU
        return false;
    }

    /** {@inheritDoc} */
    @Override
    public Model newModel(String name, Device device) {
        // We need pass TfLiteManager explicitly
        return new TfLiteModel(name, newBaseManager(device));
    }

    /** {@inheritDoc} */
    @Override
    public SymbolBlock newSymbolBlock(NDManager manager) {
        throw new UnsupportedOperationException("TFLite does not support empty SymbolBlock");
    }

    /** {@inheritDoc} */
    @Override
    public NDManager newBaseManager() {
        return newBaseManager(null);
    }

    /** {@inheritDoc} */
    @Override
    public NDManager newBaseManager(Device device) {
        if (getAlternativeEngine() != null) {
            return alternativeEngine.newBaseManager(device);
        }
        return TfLiteNDManager.getSystemManager().newSubManager(device);
    }

    /** {@inheritDoc} */
    @Override
    public GradientCollector newGradientCollector() {
        throw new UnsupportedOperationException("Not supported for TFLite");
    }

    /** {@inheritDoc} */
    @Override
    public void setRandomSeed(int seed) {
        throw new UnsupportedOperationException("Not supported for TFLite");
    }

    /** {@inheritDoc} */
    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(200);
        sb.append(getEngineName()).append(':').append(getVersion()).append(", ");
        if (alternativeEngine != null) {
            sb.append("Alternative engine: ").append(alternativeEngine.getEngineName());
        } else {
            sb.append("No alternative engine found");
        }
        return sb.toString();
    }
}
