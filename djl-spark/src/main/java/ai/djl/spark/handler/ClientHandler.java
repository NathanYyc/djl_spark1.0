package ai.djl.spark.handler;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPromise;

public class ClientHandler extends ChannelInboundHandlerAdapter {
    private ChannelHandlerContext ctx;
    private ChannelPromise promise;
    private ByteBuf data;

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        super.channelActive(ctx);
        this.ctx = ctx;
    }

    public ChannelPromise sendMessage(Object message) {
        if (ctx == null)
            throw new IllegalStateException();
        promise = ctx.pipeline().writeAndFlush(message).channel().newPromise();
        return promise;
    }

    public ByteBuf getData() {
        return data;
    }

    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        data = (ByteBuf) msg;
        promise.setSuccess();
    }
}