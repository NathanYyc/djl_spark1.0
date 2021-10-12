package ai.djl.spark.handler;

import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleStateEvent;

import java.util.concurrent.atomic.AtomicInteger;

import static io.netty.handler.timeout.IdleState.*;

public class HeartBeatHandler extends SimpleChannelInboundHandler<String> {

    private AtomicInteger connectionCount;

    int readIdleTimes = 0;

    public HeartBeatHandler(AtomicInteger connectionCount){
        this.connectionCount = connectionCount;
    }

    @Override
    protected void channelRead0(ChannelHandlerContext ctx, String s) throws Exception {
        return;
    }

    @Override
    public void channelActive(ChannelHandlerContext ctx) throws Exception {
        System.err.println("=== " + ctx.channel().remoteAddress() + " is active ===");
    }

    @Override
    public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
        super.channelRegistered(ctx);
        if (connectionCount.incrementAndGet() % 100 == 0) {
            System.out.println("current connected" + connectionCount.get());
        }
    }

    @Override
    public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
        super.channelUnregistered(ctx);
        if(connectionCount.decrementAndGet()%100 == 0){
            System.out.println("current unconnected" + connectionCount.get());
        }
    }
}
