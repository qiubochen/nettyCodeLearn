/*
 * Copyright 2012 The Netty Project
 *
 * The Netty Project licenses this file to you under the Apache License,
 * version 2.0 (the "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at:
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */
package io.netty.example.discard;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;

import java.io.UnsupportedEncodingException;

/**
 * Handles a client-side channel.
 */
public class DiscardClientHandler extends SimpleChannelInboundHandler<Object> {

    private ByteBuf content;
    private ChannelHandlerContext ctx;

    /**
     * 开始的时候运行
     * @param ctx
     */
    @Override
    public void channelActive(ChannelHandlerContext ctx) throws UnsupportedEncodingException {

            this.ctx = ctx;

            byte[] bytes = "test".getBytes();
            // Initialize the message.
            content = ctx.alloc().directBuffer(bytes.length);


            content.writeBytes(bytes);
            // Send the initial messages.
            generateTraffic();

    }

    @Override
    public void channelInactive(ChannelHandlerContext ctx) {
        content.release();
    }

    /**
     * 接收服务端发回来的消息
     * @param ctx           the {@link ChannelHandlerContext} which this {@link SimpleChannelInboundHandler}
     *                      belongs to
     * @param msg           the message to handle
     * @throws Exception
     */
    @Override
    public void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        // Server is supposed to send nothing, but if it sends something, discard it.
    }

    /**
     * 异常的时候使用
     * @param ctx
     * @param cause
     */
    @Override
    public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) {
        // Close the connection when an exception is raised.
        cause.printStackTrace();
        ctx.close();
    }

    long counter;

    /**
     * 递归调用
     */
    private void generateTraffic() {
        // Flush the outbound buffer to the socket.
        // Once flushed, generate the same amount of traffic again.
            ctx.writeAndFlush(content.retainedDuplicate()).addListener(trafficGenerator);
    }

    private final ChannelFutureListener trafficGenerator = new ChannelFutureListener() {
        @Override
        public void operationComplete(ChannelFuture future) {
            if (future.isSuccess()) {
                generateTraffic();
            } else {
                future.cause().printStackTrace();
                future.channel().close();
            }
        }
    };
}
