//package ai.djl.spark;
//
//import ai.djl.Device;
//import ai.djl.MalformedModelException;
//import ai.djl.Model;
//
//import ai.djl.basicdataset.cv.classification.Mnist;
//import ai.djl.basicmodelzoo.basic.Mlp;
//import ai.djl.engine.Engine;
//import ai.djl.examples.training.util.Arguments;
//import ai.djl.metric.Metrics;
//import ai.djl.ndarray.types.Shape;
//import ai.djl.nn.Block;
//import ai.djl.training.DefaultTrainingConfig;
//import ai.djl.training.Trainer;
//import ai.djl.training.TrainingResult;
//import ai.djl.training.dataset.Dataset;
//import ai.djl.training.dataset.RandomAccessDataset;
//import ai.djl.training.evaluator.Accuracy;
//import ai.djl.training.listener.SaveModelTrainingListener;
//import ai.djl.training.listener.TrainingListener;
//import ai.djl.training.loss.Loss;
//import ai.djl.training.util.ProgressBar;
//import ai.djl.translate.TranslateException;
//import org.apache.log4j.Logger;
//import org.apache.spark.sql.SparkSession;
//
//import java.io.IOException;
//import java.util.Arrays;
//
//public class Run {
//
//    public static Arguments argu;
//
//    public static int PARTITION_NUM = 3;
//
//    private final static Logger logger = Logger.getLogger(Run.class);
//
//    public static void main(String[] args) throws IOException, TranslateException {
//        Run.runExample(args);
//    }
//
//    public static Shape getShape(){
//        Shape inputShape = new Shape(1, Mnist.IMAGE_HEIGHT * Mnist.IMAGE_WIDTH);
//        return inputShape;
//    }
//
//    //TODO block fac
//
//    public static Block getBlock(){
//        // Construct neural network
//        Block block =
//                new Mlp(
//                        Mnist.IMAGE_HEIGHT * Mnist.IMAGE_WIDTH,
//                        Mnist.NUM_CLASSES,
//                        new int[] {128, 64});
//        return block;
//    }
//
//    public static TrainingResult runExample(String[] args) throws IOException, TranslateException { argu = new Arguments().parseArgs(args);
//        if (argu == null) {
//            return null;
//        }
//
//        Block block = getBlock();
//
//        try (Model model = Model.newInstance("mlp")) {
//            // get training and validation dataset
//            RandomAccessDataset[] trainingSet = getSplitDatasets(Dataset.Usage.TRAIN, argu);
//            RandomAccessDataset validateSet = getDataset(Dataset.Usage.TEST, argu);
//
//            // setup training configuration
//            DefaultTrainingConfig config = setupTrainingConfig(argu);
//
//            model.setBlock(block);
//
//            try (Trainer trainer = new Trainer(model, config)) {
//                trainer.setMetrics(new Metrics());
//
//                /*
//                 * MNIST is 28x28 grayscale image and pre processed into 28 * 28 NDArray.
//                 * 1st axis is batch axis, we can use 1 for initialization.
//                 */
//
//                Shape inputShape = getShape();
//                // initialize trainer with proper input shape
//                trainer.initialize(inputShape);
//
//                SparkSession spark = SparkSession.builder().master("spark://Master:7077").getOrCreate();
//                PARTITION_NUM = spark.sparkContext().getExecutorMemoryStatus().size();
//
//                //EasyTrain.fit(trainer, argu.getEpoch(), trainingSet, validateSet);
//                DistributedTrain.fit(trainer, 5, trainingSet, validateSet, spark, argu);
//
//                return trainer.getTrainingResult();
//            } catch (InterruptedException | MalformedModelException e) {
//                e.printStackTrace();
//
//                logger.error(e.fillInStackTrace());
//            }
//        }
//
//        return null;
//    }
//
//    public static DefaultTrainingConfig setupTrainingConfig(Arguments arguments) {
//        if(arguments == null){
//            arguments = argu;
//        }
//        String outputDir = arguments.getOutputDir();
//        SaveModelTrainingListener listener = new SaveModelTrainingListener(outputDir);
//        listener.setSaveModelCallback(
//                trainer -> {
//                    TrainingResult result = trainer.getTrainingResult();
//                    Model model = trainer.getModel();
//                    float accuracy = result.getValidateEvaluation("Accuracy");
//                    model.setProperty("Accuracy", String.format("%.5f", accuracy));
//                    model.setProperty("Loss", String.format("%.5f", result.getValidateLoss()));
//                });
//        return new DefaultTrainingConfig(Loss.softmaxCrossEntropyLoss())
//                .addEvaluator(new Accuracy())
//                .optDevices(Engine.getInstance().getDevices(arguments.getMaxGpus()))
//                .addTrainingListeners(TrainingListener.Defaults.logging(outputDir))
//                .addTrainingListeners(listener);
//    }
//
//    private static RandomAccessDataset[] getSplitDatasets(Dataset.Usage usage, Arguments arguments)
//            throws IOException, TranslateException {
//        Mnist mnist =
//                Mnist.builder()
//                        .optUsage(usage)
//                        .setSampling(60000, true)
//                        .optLimit(arguments.getLimit())
//                        .build();
//        int[] partitions = new int[PARTITION_NUM];
//        Arrays.fill(partitions, 1);
//        RandomAccessDataset[] results = mnist.randomSplit(partitions);
//
//        for(RandomAccessDataset result: results){
//            result.prepare(new ProgressBar());
//        }
//        return results;
//    }
//
//    private static RandomAccessDataset getDataset(Dataset.Usage usage, Arguments arguments)
//            throws IOException {
//        Mnist mnist =
//                Mnist.builder()
//                        .optUsage(usage)
//                        .setSampling(60000/arguments.getBatchSize(), true)
//                        .optLimit(arguments.getLimit())
//                        .build();
//
//        mnist.prepare(new ProgressBar());
//        return mnist;
//    }
//}
