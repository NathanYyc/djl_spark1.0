import ai.djl.spark.Util.Dispatcher;
import ai.djl.spark.server.ParameterServer;
import ai.djl.spark.server.ParameterServerHeartBeatClient;
import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

public class testServer {
    @Test
    public void testServer() throws InterruptedException {
        Object isSeverOn = new Object();
        Thread serverThread = new Thread(new Runnable() {
            @Override
            public void run() {
                ParameterServer parameterServer = new ParameterServer(1888);
                parameterServer.start(1, 1, isSeverOn);
            }
        });
        serverThread.setName("serverThread");
        serverThread.start();
        synchronized (isSeverOn) {
            isSeverOn.wait();
            Thread clientThread = new Thread(new Runnable() {
                @Override
                public void run() {
                    ParameterServerHeartBeatClient parameterServerHeartBeatClient = new ParameterServerHeartBeatClient("localhost", 1888);
                    parameterServerHeartBeatClient.start();
                }
            });

            clientThread.setName("clientThread");
            clientThread.start();
        }

        System.out.println("test finished!");
    }

    @Test
    public void testDispatcher() throws InterruptedException {
        ParameterServer parameterServer = new ParameterServer(1888);
        Object lock = new Object();
        Set<String> serverIPs = new HashSet<String>();
        serverIPs.add("0.0.0.0");

        new Thread(new Runnable() {
            @Override
            public void run() {
                parameterServer.start(1,1,lock);
            }
        }).start();



        synchronized (lock){
            lock.wait();

            Dispatcher.initial(serverIPs);
        }
    }
}
