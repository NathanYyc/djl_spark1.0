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

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import org.testng.Assert;
import org.testng.SkipException;
import org.testng.annotations.Test;

public class DlrNDManagerTest {

    @Test
    public void testNDArray() {
        if (System.getProperty("os.name").toLowerCase().startsWith("win")) {
            throw new SkipException("test only work on mac and Linux");
        }
        try (NDManager manager = DlrNDManager.getSystemManager().newSubManager()) {
            NDArray zeros = manager.zeros(new Shape(1, 2));
            float[] data = zeros.toFloatArray();
            Assert.assertEquals(data[0], 0);

            NDArray ones = manager.ones(new Shape(1, 2));
            data = ones.toFloatArray();
            Assert.assertEquals(data[0], 1);

            NDArray array = manager.create(new float[] {0f, 1f, 2f, 3f});
            float[] expected = new float[] {0f, 1f, 2f, 3f};
            Assert.assertEquals(array.toFloatArray(), expected);
        }
    }
}
