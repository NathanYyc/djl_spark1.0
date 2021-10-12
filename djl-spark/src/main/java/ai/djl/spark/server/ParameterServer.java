package ai.djl.spark.server;

import ai.djl.spark.Util.Grads;
import ai.djl.spark.initializer.ParameterServerInitializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.util.ResourceLeakDetector;

public class ParameterServer {

    private int port;
    private EventLoopGroup bossGroup;
    private EventLoopGroup workerGroup;
    private ChannelFuture channelFuture;

    public ParameterServer(int port){
        this.port = port;
    }

    public void start(int num, int paramNum, Object lock){

        Grads.init(num, paramNum);
        System.out.println("Parameter ParameterServer started!");
        bossGroup = new NioEventLoopGroup();
        workerGroup = new NioEventLoopGroup();
        try{
            synchronized (lock) {
                ServerBootstrap serverBootstrap = new ServerBootstrap();
                serverBootstrap.group(bossGroup, workerGroup).channel(NioServerSocketChannel.class)
                        .childHandler(new ParameterServerInitializer());
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
}
