package ai.djl.spark.server;

import ai.djl.spark.initializer.HeartBeatServerInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.ResourceLeakDetector;

import java.util.concurrent.atomic.AtomicInteger;

public class HeartBeatServer {
    private int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;
    private int psNumber;
    private AtomicInteger connectionCount;

    public HeartBeatServer(int port, int psNumber){
        this.port = port;
        this.psNumber = psNumber;
        connectionCount = new AtomicInteger(0);
    }

    public void start(Object lock){
        System.out.println("Heart Beat HeartBeatServer started!");
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try{

            synchronized (lock) {
                ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(bossGroup, workerGroup)
                        .handler(new LoggingHandler(LogLevel.INFO))
                        .channel(NioServerSocketChannel.class)
                        .childHandler(new HeartBeatServerInitializer(connectionCount));
                channelFuture = serverBootstrap.bind(port).sync();

                lock.notify();
            }
            channelFuture.channel().closeFuture().sync();

            ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);

        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally{
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    public boolean isAllConnected(){
        System.out.println(connectionCount.get());
        return connectionCount.get() >= psNumber-1;
    }
}
