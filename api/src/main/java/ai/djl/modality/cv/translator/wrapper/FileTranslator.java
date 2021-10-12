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
package ai.djl.modality.cv.translator.wrapper;

import ai.djl.Model;
import ai.djl.modality.cv.Image;
import ai.djl.modality.cv.ImageFactory;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.translate.Batchifier;
import ai.djl.translate.Translator;
import ai.djl.translate.TranslatorContext;
import java.io.IOException;
import java.nio.file.Path;

/**
 * Built-in {@code Translator} that provides image pre-processing from file path.
 *
 * @param <T> the output object type
 */
public class FileTranslator<T> implements Translator<Path, T> {

    private Translator<Image, T> translator;

    /**
     * Creates a {@code FileTranslator} instance.
     *
     * @param translator a {@code Translator} that can process image
     */
    public FileTranslator(Translator<Image, T> translator) {
        this.translator = translator;
    }

    /** {@inheritDoc} */
    @Override
    public NDList processInput(TranslatorContext ctx, Path input) throws Exception {
        Image image = ImageFactory.getInstance().fromFile(input);
        return translator.processInput(ctx, image);
    }

    /** {@inheritDoc} */
    @Override
    public T processOutput(TranslatorContext ctx, NDList list) throws Exception {
        return translator.processOutput(ctx, list);
    }

    /** {@inheritDoc} */
    @Override
    public Batchifier getBatchifier() {
        return translator.getBatchifier();
    }

    /** {@inheritDoc} */
    @Override
    public void prepare(NDManager manager, Model model) throws IOException {
        translator.prepare(manager, model);
    }
}
