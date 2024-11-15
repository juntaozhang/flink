package org.apache.flink.streaming.examples.my.netty;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelHandlerContext;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInboundHandlerAdapter;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.EventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringDecoder;
import org.apache.flink.shaded.netty4.io.netty.handler.codec.string.StringEncoder;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class EchoServer {
    public Map<String, EchoServerHandler> clients = new ConcurrentHashMap<>();

    public static void main(String[] args) throws Exception {
        new EchoServer().start(28080);
    }

    public void start(int port) throws Exception {
        EventLoopGroup group = new NioEventLoopGroup();
        try {
            ServerBootstrap b = new ServerBootstrap();
            b.group(group)
                    .channel(NioServerSocketChannel.class)
                    .localAddress(port)
                    .childHandler(
                            new ChannelInitializer<SocketChannel>() {
                                @Override
                                public void initChannel(SocketChannel ch) {
                                    ch.pipeline()
                                            .addLast(
                                                    new StringDecoder(),
                                                    new StringEncoder(),
                                                    new EchoServerHandler());
                                }
                            })
                    .option(ChannelOption.SO_BACKLOG, 128)
                    .childOption(ChannelOption.SO_KEEPALIVE, true);

            ChannelFuture f = b.bind().sync();
            System.out.println(
                    this.getClass().getName()
                            + " started and listen on "
                            + f.channel().localAddress());

            BufferedReader reader = new BufferedReader(new InputStreamReader(System.in));
            String line;
            while ((line = reader.readLine()) != null) {
                if ("exit".equalsIgnoreCase(line)) {
                    f.channel().close().sync();
                    clients.values().forEach(EchoServerHandler::close);
                    break;
                }
                if (line.isEmpty() || line.startsWith("#")) {
                    continue;
                }
                String[] arr = line.split(":");
                if (clients.containsKey(arr[0])) {
                    clients.get(arr[0]).write(arr[1]);
                } else {
                    System.out.println("Client not found: " + arr[0]);
                }

                f.channel().writeAndFlush(line + "\n");
            }
        } finally {
            group.shutdownGracefully().sync();
        }
    }

    class EchoServerHandler extends ChannelInboundHandlerAdapter {
        private String name;
        private ChannelHandlerContext ctx;

        public void write(String msg) {
            ctx.channel().writeAndFlush(msg + "\n");
        }

        public void close() {
            ctx.channel().close();
        }

        @Override
        public void channelRegistered(ChannelHandlerContext ctx) throws Exception {
            super.channelRegistered(ctx);
            name = ctx.channel().remoteAddress().toString().split(":")[1];
            System.out.println("Server register: " + name);
            if (this.ctx == null) {
                this.ctx = ctx;
            }
            clients.put(name, this);
        }

        @Override
        public void channelRead(ChannelHandlerContext ctx, Object msg) {
            System.out.printf("Server received[%s]: %s%n", this.name, msg);
            ctx.write(Thread.currentThread().getName() + ":" + msg);
        }

        @Override
        public void channelUnregistered(ChannelHandlerContext ctx) throws Exception {
            ctx.close();
            super.channelUnregistered(ctx);
            System.out.println("Server unregister: " + name);
            clients.remove(name);
        }

        @Override
        public void channelReadComplete(ChannelHandlerContext ctx) {
            ctx.flush();
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
            cause.printStackTrace();
            ctx.close();
        }
    }
}
