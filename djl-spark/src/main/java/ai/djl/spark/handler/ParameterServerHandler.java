package ai.djl.spark.handler;

import ai.djl.spark.Util.Grads;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

public class ParameterServerHandler extends ChannelInboundHandlerAdapter {
    /**
     * Invoked when the current {@link Channel} has read a message from the peer.
     *
     * @param ctx current channel handler context
     * @param msg message
     */
    @Override
    public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
        ByteBuf in = (ByteBuf) msg;
        ByteBuf result = Unpooled.wrappedBuffer(Grads.addGrads(in));
        ctx.writeAndFlush(result);
    }

    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // 冲刷所有待审消息到远程节点。关闭通道后，操作完成
        cause.printStackTrace();
        // 关闭通道
        ctx.close();
    }
}
