package ai.djl.spark.Util;

import ai.djl.ndarray.NDArray;
import ai.djl.ndarray.NDManager;
import io.netty.buffer.ByteBuf;

import java.nio.ByteBuffer;
import java.util.concurrent.CountDownLatch;

import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.locks.ReentrantLock;

public class Grads {
    public static volatile NDArray[] grads = null;
    private static int count = 0;

    private static CyclicBarrier[] cyclicBarrier;
    private static NDManager ndManager;
    private static ReentrantLock[] locks;
    private static CountDownLatch[] latches;

    private static int nodeNum;
    private static int paramNum;
    /**
     * Initialize the NDManager
     * @param nodeNum: the number of nodes in spark.
     * @param
     */
    public static void init(int nodeNum, int paramNum){
        Grads.nodeNum = nodeNum;
        Grads.paramNum = paramNum;

        cyclicBarrier = new CyclicBarrier[paramNum];
        grads = new NDArray[paramNum];
        latches = new CountDownLatch[nodeNum];
        for(int i = 0; i< nodeNum; i++){
            latches[i] = new CountDownLatch(nodeNum);
        }

        locks = new ReentrantLock[paramNum];
        for(int i = 0; i < paramNum; i++){
            locks[i] = new ReentrantLock();
            cyclicBarrier[i] = new CyclicBarrier(nodeNum);
        }

        ndManager = NDManager.newBaseManager();
    }

    /**
     * reset the grads & counter
     */
    public static void reset(int index){

        if(grads[index] != null){
            locks[index].lock();

            NDArray temp = null;
            try {
                if (grads[index] != null) {
                    temp = grads[index];
                    grads[index] = null;

                    latches[index] = new CountDownLatch(nodeNum);
                }
            }finally {
                if(temp != null) {
                    temp.detach();
                    temp.close();
                }
                locks[index].unlock();
            }
        }

    }

    /**
     * add up grads
     * @param grad gradient to add
     */
    public static ByteBuffer addGrads(ByteBuf grad) throws Exception {
        byte[] gradByte = new byte[grad.nioBuffer().remaining()];
        grad.readBytes(gradByte);
        grad.release();

        //bytebuf input stream
        NDArray gradArray = NDArray.decode(ndManager, gradByte);

        int index = -1;

        for(int i = 0; i < paramNum; i++) {
            if (grads[i] == null) {
                locks[i].lock();

                boolean ifTheSame = true;
                try {
                    if (grads[i] == null) {
                        index = i;
                        grads[i] = gradArray;
                        latches[i].countDown();
                    }else if(grads[i].getName().equals(gradArray.getName())){
                        index = i;
                        latches[i].countDown();
                    }else{
                        ifTheSame = false;
                    }
                } finally {
                    locks[i].unlock();
                    if(ifTheSame)
                        break;
                }
            }else if (grads[i].getName().equals(gradArray.getName())) {
                index = i;
                break;
            }
        }

        if(index!=-1 && grads[index]!=gradArray) {
            locks[index].lock();
            try {
                grads[index].add(gradArray);
                latches[index].countDown();
            }finally {
                gradArray.detach();
                gradArray.close();

                locks[index].unlock();
            }
        }else if(index == -1){
            throw new Exception("cannot find any room or gradient");
        }

        latches[index].await();

        byte[] result;
        result = grads[index].encode();

        cyclicBarrier[index].await();

        reset(index);
        return ByteBuffer.wrap(result);
    }
}
