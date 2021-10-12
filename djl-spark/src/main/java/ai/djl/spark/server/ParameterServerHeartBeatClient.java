package ai.djl.spark.server;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.util.Random;

public class ParameterServerHeartBeatClient  {

    String ip;
    int port;
    Channel channel;
    Random random ;

    public ParameterServerHeartBeatClient(String ip,int port){
        this.ip = ip;
        this.port = port;
        random = new Random();
    }

    public void start() {
        EventLoopGroup eventLoopGroup = new NioEventLoopGroup();
        try{
            Bootstrap bootstrap = new Bootstrap();
            bootstrap.group(eventLoopGroup).channel(NioSocketChannel.class)
                    .handler(new HeartBeatClientInitializer());

            connect(bootstrap,ip,port);

            String localAddr = InetAddress.getLocalHost().getHostAddress();
            String  text = "is alive";
            System.out.println("heart beat client started!");
            while (channel.isActive()){
                sendMsg(localAddr + text);
            }
        }catch(Exception e){
            // do something
        }finally {
            eventLoopGroup.shutdownGracefully();
        }
    }

    public void connect(Bootstrap bootstrap,String ip, int port) throws Exception{
        channel = bootstrap.connect(ip,port).sync().channel();
    }

    public void sendMsg(String text) throws Exception{
        int num = random.nextInt(10);
        channel.writeAndFlush(text);
        Thread.sleep(num * 1000);
    }

    static class HeartBeatClientInitializer extends ChannelInitializer<Channel> {

        @Override
        protected void initChannel(Channel ch) throws Exception {
            ChannelPipeline pipeline = ch.pipeline();
            pipeline.addLast("decoder", new StringDecoder());
            pipeline.addLast("encoder", new StringEncoder());
            pipeline.addLast(new HeartBeatClientHandler());
        }
    }

    static class HeartBeatClientHandler extends SimpleChannelInboundHandler<String> {
        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) throws Exception {
            System.out.println(" client received :" +msg);
            if(msg!= null && msg.equals("you are out")) {
                System.out.println(" server closed connection , so client will close too");
                ctx.channel().closeFuture();
            }
        }
    }
}