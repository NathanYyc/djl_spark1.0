package ai.djl.spark.initializer;

import ai.djl.spark.handler.HeartBeatHandler;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelPipeline;
import io.netty.handler.codec.string.StringDecoder;
import io.netty.handler.codec.string.StringEncoder;
import io.netty.handler.timeout.IdleStateHandler;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class HeartBeatServerInitializer extends ChannelInitializer<Channel> {

    private AtomicInteger connectionCount;

    public  HeartBeatServerInitializer(AtomicInteger connectionCount){
        super();
        this.connectionCount = connectionCount;
    }

    @Override
    protected void initChannel(Channel channel) throws Exception {
        ChannelPipeline pipeline = channel.pipeline();
        pipeline.addLast("decoder", new StringDecoder());
        pipeline.addLast("encoder", new StringEncoder());
        pipeline.addLast(new IdleStateHandler(2,2,2, TimeUnit.SECONDS));
        pipeline.addLast(new HeartBeatHandler(connectionCount));
    }
}