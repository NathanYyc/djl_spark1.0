package ai.djl.spark.training;

import ai.djl.ndarray.NDArray;
import ai.djl.spark.Util.Dispatcher;
import ai.djl.training.ParameterServer;


public class DistributedParameterServer implements ParameterServer {

    private ParameterServer nativeParameterParameterServer;

    public DistributedParameterServer(ParameterServer parameterServer) throws InterruptedException {
        nativeParameterParameterServer = parameterServer;
    }

    /**
     * Initializes the {@code ParameterStore} for the given parameter.····
     *
     * @param parameterId the parameter ID
     * @param value       the values to be set for the given parameter
     */
    public void init(String parameterId, NDArray[] value) {
        nativeParameterParameterServer.init(parameterId, value);
    }

    /**
     * Updates the parameter of a key from Parameter ParameterServer.
     *
     * @param parameterId the key to identify the parameter
     * @param grads       the gradient NDArrays in different devices to apply the update.
     * @param params      the parameter NDArrays in different devices to be updated.
     */
    public void update(String parameterId, NDArray[] grads, NDArray[] params){
        //get the sum of grads from all the devices
        NDArray sum = grads[0].duplicate();
        sum.setName(params[0].getName());

        //同一设备
        for(int i = 1; i< grads.length; i++){
            sum.add(grads[i]);
        }
        sum.div(grads.length);

        NDArray result = null;
        try {
            result = Dispatcher.dispatch(sum, grads[0].getManager());
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        for(int i = 0 ; i < grads.length; i++){
            grads[i] = result.duplicate().toDevice(grads[i].getDevice(), true);
        }

        nativeParameterParameterServer.update(parameterId, grads, params);

        sum.detach();
        sum.close();

        result.detach();
        result.close();
    }

    /**
     * {@inheritDoc}
     */
    public void close() {
        nativeParameterParameterServer.close();
    }
}
