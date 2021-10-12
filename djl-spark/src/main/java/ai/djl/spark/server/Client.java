package ai.djl.spark.server;

import ai.djl.spark.handler.ClientHandler;
import ai.djl.spark.initializer.ParameterClientInitializer;
import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelPromise;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.util.ResourceLeakDetector;


public class Client {
    private ClientHandler clientHandler = new ClientHandler();

    private String host;
    private int port;
    private SocketChannel socketChannel;
    private EventLoopGroup workerGroup;

    /**
     * Constructor of client
     * @param port the port of parameter server
     */
    public Client(String host, int port) throws InterruptedException {
        this.host = host;
        this.port = port;
        this.start();
    }

    /**
     * start the client service
     */
    public void start() throws InterruptedException {
        this.workerGroup = new NioEventLoopGroup();
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.group(workerGroup).channel(NioSocketChannel.class)
                .handler(new ParameterClientInitializer(clientHandler));
        ChannelFuture channelFuture = bootstrap.connect(host,port).sync();

        if(channelFuture.isSuccess()){
            socketChannel = (SocketChannel) channelFuture.channel();
            System.out.println("successfully connected to " + host +":"+ port + " ！");
        }
        ResourceLeakDetector.setLevel(ResourceLeakDetector.Level.ADVANCED);
    }

    public ByteBuf getResult(ByteBuf msg) throws InterruptedException {
        ChannelPromise promise = clientHandler.sendMessage(msg);
        promise.await();
        return clientHandler.getData();
    }

    /**
     * close the socket Channel & worker Group
     * @throws InterruptedException
     */
    public void close() throws InterruptedException {
        socketChannel.closeFuture().sync();
        workerGroup.shutdownGracefully();
    }

    /**
     * get the communication channel
     * @return the channel
     */
    public Channel getChannel(){
        return socketChannel;
    }
}

