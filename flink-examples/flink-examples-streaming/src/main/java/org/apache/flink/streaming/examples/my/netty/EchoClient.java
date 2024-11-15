package org.apache.flink.streaming.examples.my.netty;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.SimpleChannelInboundHandler;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringEncoder;

public class EchoClient {

    private final String host;
    private final int port;

    public EchoClient(String host, int port) {
        this.host = host;
        this.port = port;
    }

    public static void main(String[] args) throws Exception {
        new EchoClient("localhost", 28080).start();
    }

    public void start() throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            Bootstrap b = new Bootstrap();
            b.group(group)
                    .channel(NioSocketChannel.class)
                    .remoteAddress(host, port)
                    .handler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {
                                    ch.pipeline()
                                            .addLast(
                                                    new StringDecoder(),
                                                    new StringEncoder(),
                                                    new EchoClientHandler());
                                }
                            });

            ChannelFuture f = b.connect().sync();

            //            BufferedReader reader = new BufferedReader(new
            // InputStreamReader(System.in));
            //            String line;
            //            while ((line = reader.readLine()) != null) {
            //                if ("exit".equalsIgnoreCase(line)) {
            ////                    f.channel().writeAndFlush(line +
            // "\n").addListener(ChannelFutureListener.CLOSE);
            //                    f.channel().close().sync();
            //                    break;
            //                }
            //                if (line.isEmpty() || line.startsWith("#")) {
            //                    continue;
            //                }
            //                f.channel().writeAndFlush(line + "\n");
            //            }
            f.channel().closeFuture().sync();
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    static class EchoClientHandler extends SimpleChannelInboundHandler<String> {
        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
            System.out.println("Client connected");
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            super.channelUnregistered(ctx);
            System.out.println("Client disconnected");
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, String msg) {
            System.out.println("Client received: " + msg);
        }
    }
}
