package ai.djl.spark.training;

import ai.djl.Device;
import ai.djl.Model;
import ai.djl.metric.Metrics;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDArrays;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.nn.Parameter;
import ai.djl.training.*;
import ai.djl.training.evaluator.Evaluator;
import ai.djl.training.listener.EpochTrainingListener;
import ai.djl.training.listener.EvaluatorTrainingListener;
import ai.djl.training.listener.TrainingListener;
import ai.djl.training.loss.Loss;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * The {@code Trainer} interface provides a session for model training.
 *
 * <p>{@code Trainer} provides an easy, and manageable interface for training. {@code Trainer} is
 * not thread-safe.
 *
 * <p>See the tutorials on:
 *
 * <ul>
 *   <li><a
 *       href="https://github.com/awslabs/djl/blob/master/jupyter/tutorial/02_train_your_first_model.ipynb">Training
 *       your first model</a>
 *   <li><a
 *       href="https://github.com/awslabs/djl/blob/master/jupyter/transfer_learning_on_cifar10.ipynb">Training
 *       using transfer learning</a>
 *   <li><a
 *       href="https://github.com/awslabs/djl/blob/master/jupyter/load_mxnet_model.ipynb">Inference
 *       with an MXNet model</a>
 * </ul>
 */
public class DistributedTrainer extends Trainer {

    private static final Logger logger = LoggerFactory.getLogger(ai.djl.training.Trainer.class);

    private Model model;
    private NDManager manager;
    private Metrics metrics;
    private List<TrainingListener> listeners;
    private Device[] devices;
    private ParameterStore parameterStore;
    private List<Evaluator> evaluators;
    private Loss loss;

    private boolean gradientsChecked;

    /**
     * Creates an instance of {@code DistributedTrainer} with the given {@link Model} and {@link
     * TrainingConfig}. And use {@link DistributedParameterServer} as parameter server
     *
     * @param model the model the trainer will train on
     * @param trainingConfig the configuration used by the trainer
     */
    public DistributedTrainer(Model model, TrainingConfig trainingConfig) throws InterruptedException {
        super(model, trainingConfig);
        this.model = super.getModel();
        this.manager = super.getManager();
        this.metrics = super.getMetrics();
        this.listeners = trainingConfig.getTrainingListeners();
        this.devices = super.getDevices();
        this.evaluators = super.getEvaluators();
        this.loss = super.getLoss();

        ParameterServer nativeParameterServer = manager.getEngine().newParameterServer(trainingConfig.getOptimizer());
        ParameterServer parameterServer = new DistributedParameterServer(nativeParameterServer);

        parameterStore = new ParameterStore(manager, false);
        parameterStore.setParameterServer(parameterServer, devices);

        listeners = trainingConfig.getTrainingListeners();

        //notifyListeners(listener -> listener.onTrainingBegin(this));
    }

    /**
     * Applies the forward function of the model once on the given input {@link NDList}.
     *
     * @param input the input {@link NDList}
     * @return the output of the forward function
     */
    public NDList forward(NDList input) {
        long begin = System.nanoTime();
        try {
            return model.getBlock().forward(parameterStore, input, true);
        } finally {
            addMetric("forward", begin);
        }
    }

    /**
     * Applies the forward function of the model once with both data and labels.
     *
     * @param data the input data {@link NDList}
     * @param labels the input labels {@link NDList}
     * @return the output of the forward function
     */
    public NDList forward(NDList data, NDList labels) {
        long begin = System.nanoTime();
        try {
            return model.getBlock().forward(parameterStore, data, labels, null);
        } finally {
            addMetric("forward", begin);
        }
    }

    /**
     * Evaluates function of the model once on the given input {@link NDList}.
     *
     * @param input the input {@link NDList}
     * @return the output of the predict function
     */
    public NDList evaluate(NDList input) {
        return model.getBlock().forward(parameterStore, input, false, null);
    }

    /** Updates all of the parameters of the model once. */
    public void step(){
        if (!gradientsChecked) {
            checkGradients();
        }

        long begin = System.nanoTime();

        parameterStore.updateAllParameters();

        addMetric("step", begin);
    }

    /**
     * Returns the Metrics param used for benchmarking.
     *
     * @return the the Metrics param used for benchmarking
     */
    public Metrics getMetrics() {
        return metrics;
    }

    /**
     * Attaches a Metrics param to use for benchmarking.
     *
     * @param metrics the Metrics class
     */
    public void setMetrics(Metrics metrics) {
        this.metrics = metrics;
    }


    /**
     * Gets the training {@link Loss} function of the trainer.
     *
     * @return the {@link Loss} function
     */
    public Loss getLoss() {
        return loss;
    }

    /**
     * Returns the model used to create this trainer.
     *
     * @return the model associated with this trainer
     */
    public Model getModel() {
        return model;
    }

    /**
     * Returns the {@link TrainingResult}.
     *
     * @return the {@code TrainingResult}
     */
    public TrainingResult getTrainingResult() {
        TrainingResult result = new TrainingResult();
        for (TrainingListener listener : listeners) {
            if (listener instanceof EpochTrainingListener) {
                result.setEpoch(((EpochTrainingListener) listener).getNumEpochs());
            } else if (listener instanceof EvaluatorTrainingListener) {
                EvaluatorTrainingListener l = (EvaluatorTrainingListener) listener;
                result.setEvaluations(l.getLatestEvaluations());
            }
        }
        return result;
    }

    /**
     * Gets the {@link NDManager} from the model.
     *
     * @return the {@link NDManager}
     */
    public NDManager getManager() {
        return manager;
    }

    /** {@inheritDoc} */
    @SuppressWarnings("deprecation")
    @Override
    protected void finalize() throws Throwable {
        if (manager.isOpen()) {
            if (logger.isDebugEnabled()) {
                logger.warn("Trainer for {} was not closed explicitly.", model.getName());
            }
            close();
        }
        super.finalize();
    }

    /** {@inheritDoc} */
    @Override
    public void close() {
        //notifyListeners(listener -> listener.onTrainingEnd(this));

        parameterStore.sync();
        manager.close();
    }

    /**
     * Checks if all gradients are zeros. This prevent users from calling step() without running
     * {@code backward}.
     */
    private void checkGradients() {
        List<NDArray> grads = new ArrayList<>();
        model.getBlock()
                .getParameters()
                .values()
                .stream()
                .filter(Parameter::requiresGradient)
                .forEach(
                        param ->
                                grads.add(
                                        parameterStore
                                                .getValue(param, devices[0], true)
                                                .getGradient()));



        NDList list = new NDList(grads.stream().map(NDArray::sum).toArray(NDArray[]::new));
        NDArray gradSum = NDArrays.stack(list);
        list.close();

        NDArray array = gradSum.sum();

        float[] sums = array.toFloatArray();

        array.close();
        gradSum.close();

        float sum = 0f;
        for (float num : sums) {
            sum += num;
        }
        if (sum == 0f) {
            throw new IllegalStateException(
                    "Gradient values are all zeros, please call gradientCollector.backward() on"
                            + "your target NDArray (usually loss), before calling step() ");
        }

        gradientsChecked = true;
    }

    /**
     * Helper to add a metric for a time difference.
     *
     * @param metricName the metric name
     * @param begin the time difference start (this method is called at the time difference end)
     */
    public void addMetric(String metricName, long begin) {
        if (metrics != null && begin > 0L) {
            metrics.addMetric(metricName, System.nanoTime() - begin);
        }
    }

    public void initialize(Shape shape) {
        model.getBlock().initialize(model.getNDManager(), model.getDataType(), shape);
        // call getValue on all params to initialize on all devices
        model.getBlock()
                .getParameters()
                .forEach(
                        pair -> {
                            for (Device device : devices) {
                                parameterStore.getValue(pair.getValue(), device, true);
                            }
                        });
    }
}
