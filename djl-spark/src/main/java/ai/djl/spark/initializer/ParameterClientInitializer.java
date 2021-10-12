package ai.djl.spark.initializer;

import ai.djl.spark.handler.ClientHandler;
import ai.djl.spark.handler.Decoder;
import ai.djl.spark.handler.Encoder;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.socket.SocketChannel;

public class ParameterClientInitializer extends ChannelInitializer<SocketChannel> {
    /**
     * This method will be called once the {@link Channel} was registered. After the method returns this instance
     * will be removed from the {@link ChannelPipeline} of the {@link Channel}.
     *
     * @param socketChannel the {@link Channel} which was registered.
     * @throws Exception is thrown if an error occurs. In that case it will be handled by
     *                   {@link #exceptionCaught(ChannelHandlerContext, Throwable)} which will by default close
     *                   the {@link Channel}.
     */
    ClientHandler clientHandler;

    public ParameterClientInitializer(ClientHandler clientHandler){
        super();
        this.clientHandler = clientHandler;
    }

    @Override
    protected void initChannel(SocketChannel socketChannel) throws Exception {
        ChannelPipeline pipeline = socketChannel.pipeline();
        pipeline.addLast(new Encoder(),
                new Decoder(), clientHandler);
    }
}
