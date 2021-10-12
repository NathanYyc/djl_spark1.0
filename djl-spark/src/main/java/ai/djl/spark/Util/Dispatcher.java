package ai.djl.spark.Util;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDList;
import ai.djl.ndarray.NDManager;
import ai.djl.ndarray.types.Shape;
import ai.djl.spark.server.Client;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.*;
import java.util.concurrent.CountDownLatch;

public class Dispatcher {
    private static Set<String> serverIPs;
    private static Map<String, Client> connections;
    private static int serverNumber;

    public static void initial(Set<String> serverIPs) throws InterruptedException {
        Dispatcher.serverIPs = serverIPs;
        serverNumber = serverIPs.size();
        connections = new HashMap<String, Client>();
        Iterator<String> iterator = serverIPs.iterator();
        while(iterator.hasNext()){
            String temp = iterator.next();
            connections.put(temp, new Client(temp, 1888));
        }
    }

    public static NDArray dispatch(NDArray ndArray, NDManager ndManager) throws InterruptedException{
        Shape shape = ndArray.getShape();

        long[] indices = new long[serverNumber - 1];
        NDArray flattenArray = ndArray.flatten();
        long size = flattenArray.getShape().get(0);

        NDList list = null;
        if(size > serverNumber) {
            long t = size / serverNumber;
            long temp = t;
            for (int i = 0; i < serverNumber - 1; i++) {
                indices[i] = temp;
                temp += t;
            }

            list = flattenArray.split(indices);

            for (NDArray array : list) {
                array.setName(ndArray.getName());
            }

            Iterator<String> iterator = serverIPs.iterator();
            int index = 0;
            NDArray[] resultArrays = new NDArray[serverNumber];
            CountDownLatch countDownLatch = new CountDownLatch(serverNumber);

            while (iterator.hasNext()) {
                String ip = iterator.next();
                int finalIndex = index;

                NDList finalList = list;
                new Thread(new Runnable() {
                    @Override
                    public void run() {
                        try {
                            ByteBuf result = connections.get(ip).getResult(Unpooled.wrappedBuffer(finalList.get(finalIndex).encode()));
                            byte[] resultArray = new byte[result.nioBuffer().remaining()];
                            result.readBytes(resultArray);
                            NDArray sum = NDArray.decode(ndManager, resultArray);

                            resultArrays[finalIndex] = sum;

                            countDownLatch.countDown();
                            result.release();
                        } catch (InterruptedException e) {
                            e.printStackTrace();
                        }
                    }
                }).start();

                index++;
            }

            countDownLatch.await();
            for (int i = 1; i < serverNumber; i++) {
                resultArrays[0] = resultArrays[0].concat(resultArrays[i]);


                resultArrays[i].detach();
                resultArrays[i].close();
            }

            NDArray result = resultArrays[0].reshape(shape);
            return result;
        }else{
            return ndArray;
        }
    }
}
