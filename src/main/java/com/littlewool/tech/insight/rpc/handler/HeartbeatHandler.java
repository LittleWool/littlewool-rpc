package com.littlewool.tech.insight.rpc.handler;

import com.littlewool.tech.insight.rpc.message.HeartbeatRequest;
import com.littlewool.tech.insight.rpc.message.HeartbeatResponse;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.SimpleChannelInboundHandler;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;

/**
 * @ClassName: Handler
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/23 9:50
 * @Version: 1.0
 **/

public class HeartbeatHandler extends SimpleChannelInboundHandler<Object> {


    @Override
    protected void channelRead0(ChannelHandlerContext ctx, Object msg) throws Exception {
        if (msg instanceof HeartbeatRequest){
            HeartbeatRequest request = (HeartbeatRequest)msg;
            ctx.writeAndFlush(new HeartbeatResponse(request.getRequestTime()));
            return;
        }else if (msg instanceof HeartbeatResponse){
            HeartbeatResponse response = (HeartbeatResponse)msg;
            long duration=System.currentTimeMillis()- response.getRequestTime();
            System.out.println("接收到一个心跳响应,延迟："+duration+"毫秒");
            return;
        }
        ctx.fireChannelRead(msg);
    }
    @Override
    public void userEventTriggered(ChannelHandlerContext ctx, Object evt) throws Exception {
        if(evt instanceof IdleStateEvent){
            IdleStateEvent idleStateEvent = (IdleStateEvent)evt;
            IdleState state = idleStateEvent.state();
            if(state==IdleState.READER_IDLE){
                ctx.channel().close();
            }else if (state==IdleState.WRITER_IDLE){
                ctx.writeAndFlush(new HeartbeatRequest());
            }
        }
        ctx.fireUserEventTriggered(evt);
    }


}
