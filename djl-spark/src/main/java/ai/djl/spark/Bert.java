package ai.djl.spark;


import ai.djl.Device;
import ai.djl.MalformedModelException;
import ai.djl.Model;
import ai.djl.basicdataset.cv.classification.Mnist;
import ai.djl.engine.Engine;
import ai.djl.examples.training.util.Arguments;
import ai.djl.examples.training.util.BertCodeDataset;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Block;
import ai.djl.nn.Parameter;
import ai.djl.nn.transformer.BertBlock;
import ai.djl.nn.transformer.BertPretrainingBlock;
import ai.djl.nn.transformer.BertPretrainingLoss;
import ai.djl.training.DefaultTrainingConfig;
import ai.djl.training.EasyTrain;
import ai.djl.training.Trainer;
import ai.djl.training.TrainingConfig;
import ai.djl.training.TrainingResult;
import ai.djl.training.dataset.Dataset;
import ai.djl.training.dataset.RandomAccessDataset;
import ai.djl.training.initializer.TruncatedNormalInitializer;
import ai.djl.training.listener.TrainingListener.Defaults;
import ai.djl.training.optimizer.Adam;
import ai.djl.training.optimizer.Optimizer;
import ai.djl.training.tracker.PolynomialDecayTracker;
import ai.djl.training.tracker.Tracker;
import ai.djl.training.tracker.WarmUpTracker;
import ai.djl.training.tracker.WarmUpTracker.Mode;
import ai.djl.training.util.ProgressBar;
import ai.djl.translate.TranslateException;
import ai.djl.util.Pair;
import org.apache.spark.sql.SparkSession;

import javax.validation.constraints.Max;
import java.io.IOException;
import java.util.Arrays;

/** Simple example that performs Bert pretraining on the java source files in this repo. */
public final class Bert{

    private static final int DEFAULT_BATCH_SIZE = 48;
    private static final int DEFAULT_EPOCHS = 10;
    private static int MaxSequenceLength = 0;

    private Bert() {}

    public static void main(String[] args) throws IOException, TranslateException {
        Bert.runExample(args);
    }

    public static TrainingResult runExample(String[] args) throws IOException, TranslateException {
        BertArguments arguments = (BertArguments) new BertArguments().parseArgs(args);

        BertCodeDataset dataset =
                new BertCodeDataset(arguments.getBatchSize(), arguments.getLimit());
        dataset.prepare();

        MaxSequenceLength = dataset.getMaxSequenceLength();
        //split Dataset
        BertCodeDataset[] trainingDataset = dataset.splitDataset(3);
        // Create model & trainer
        try (Model model = createBertPretrainingModel(dataset.getDictionarySize())) {

            TrainingConfig config = createTrainingConfig(arguments);
            try (Trainer trainer = model.newTrainer(config)) {

                // Initialize training
                Shape inputShape = getShape();
                trainer.initialize(inputShape, inputShape, inputShape, inputShape);

                for(Pair<String, Parameter> p: model.getBlock().getParameters()){
                    System.out.println(p.getValue().getName());
                }
                SparkSession spark = SparkSession.builder().master("spark://Master:7077").config("spark.rpc.message.maxSize", 512).getOrCreate();


                System.out.println("dictionary size:" + dataset.getDictionarySize());
                //EasyTrain.fit(trainer, argu.getEpoch(), trainingSet, validateSet);
                DistributedTrain.fit(trainer, 5, trainingDataset,  null,spark, arguments);

                //EasyTrain.fit(trainer, arguments.getEpoch(), dataset, null);
                return trainer.getTrainingResult();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (MalformedModelException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
    public static Shape getShape(){
        return new Shape(MaxSequenceLength, 512);
    }

    public static Model createBertPretrainingModel(int dictionarySize) {
        Block block =
                new BertPretrainingBlock(
                        BertBlock.builder().micro().setTokenDictionarySize(dictionarySize));
        block.setInitializer(new TruncatedNormalInitializer(0.02f), Parameter.Type.WEIGHT);

        Model model = Model.newInstance("Bert Pretraining");
        model.setBlock(block);
        return model;
    }

    public static TrainingConfig createTrainingConfig(BertArguments arguments) {
        Tracker learningRateTracker =
                WarmUpTracker.builder()
                        .optWarmUpBeginValue(0f)
                        .optWarmUpSteps(1000)
                        .optWarmUpMode(Mode.LINEAR)
                        .setMainTracker(
                                PolynomialDecayTracker.builder()
                                        .setBaseValue(5e-5f)
                                        .setEndLearningRate(5e-5f / 1000)
                                        .setDecaySteps(100000)
                                        .optPower(1f)
                                        .build())
                        .build();
        Optimizer optimizer =
                Adam.builder()
                        .optEpsilon(1e-5f)
                        .optLearningRateTracker(learningRateTracker)
                        .build();
        return new DefaultTrainingConfig(new BertPretrainingLoss())
                .optOptimizer(optimizer)
                .optDevices(new Device[]{Device.cpu()})
                .addTrainingListeners(Defaults.logging());
    }

    public static class BertArguments extends Arguments {
        private static final long serialVersionUID = 1L;

        /** {@inheritDoc} */
        @Override
        protected void initialize() {
            super.initialize();
            epoch = DEFAULT_EPOCHS;
            batchSize = DEFAULT_BATCH_SIZE;
        }
    }
}