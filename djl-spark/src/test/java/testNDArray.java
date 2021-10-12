import ai.djl.Device;
import ai.djl.mxnet.engine.MxNDManager;
import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.internal.NDFormat;
import ai.djl.ndarray.types.DataType;
import ai.djl.ndarray.types.Shape;
import org.junit.Test;

import java.util.Arrays;

public class testNDArray {
    @Test
    public void testArraySplit(){
        try (NDManager manager = NDManager.newBaseManager(Device.cpu())) {
            NDArray ndArray= manager.arange(8f);

            NDArray ndArray1 = manager.arange(16f);

            ndArray.concat(ndArray1);
            ndArray1.detach();
            ndArray1.close();
            System.out.println(ndArray.getShape().get(0));
        }
    }
}
