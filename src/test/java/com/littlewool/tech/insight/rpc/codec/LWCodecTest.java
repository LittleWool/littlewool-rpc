package com.littlewool.tech.insight.rpc.codec;

import com.littlewool.tech.insight.rpc.compress.CompressionManager;
import com.littlewool.tech.insight.rpc.message.InvocationType;
import com.littlewool.tech.insight.rpc.message.Request;
import com.littlewool.tech.insight.rpc.serializer.SerizalizerManager;
import io.netty.buffer.ByteBuf;
import io.netty.channel.embedded.EmbeddedChannel;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class LWCodecTest {

    @Test
    public void encodesAndDecodesRequest() {
        EmbeddedChannel outbound = channel();
        Request request = new Request();
        request.markNormalInvoke();
        request.setServiceName("demoService");
        request.setMethodName("hello");
        request.setParamClass(new Class<?>[] {String.class});
        request.setParams(new Object[] {"littlewool"});

        assertTrue(outbound.writeOutbound(request));
        ByteBuf frame = outbound.readOutbound();

        EmbeddedChannel inbound = channel();
        assertTrue(inbound.writeInbound(frame));
        Request decoded = inbound.readInbound();

        assertEquals(request.getRequestId(), decoded.getRequestId());
        assertEquals(InvocationType.NORMAL, decoded.getInvocationType());
        assertEquals("demoService", decoded.getServiceName());
        assertEquals("hello", decoded.getMethodName());
        assertEquals("littlewool", decoded.getParams()[0]);

        outbound.finishAndReleaseAll();
        inbound.finishAndReleaseAll();
    }

    @Test
    public void encodesAndDecodesGenericRequest() {
        EmbeddedChannel outbound = channel();
        Request request = new Request();
        request.markGenericInvoke();
        request.setServiceName("demoService");
        request.setMethodName("merge");
        request.setParamsClassStr(new String[] {"java.lang.String"});
        request.setParams(new Object[] {"littlewool"});

        assertTrue(outbound.writeOutbound(request));
        ByteBuf frame = outbound.readOutbound();

        EmbeddedChannel inbound = channel();
        assertTrue(inbound.writeInbound(frame));
        Request decoded = inbound.readInbound();

        assertEquals(InvocationType.GENERIC, decoded.getInvocationType());
        assertTrue(decoded.isGenericCall());
        assertEquals("demoService", decoded.getServiceName());
        assertEquals("merge", decoded.getMethodName());
        assertEquals("java.lang.String", decoded.getParamsClassStr()[0]);

        outbound.finishAndReleaseAll();
        inbound.finishAndReleaseAll();
    }

    private EmbeddedChannel channel() {
        EmbeddedChannel channel = new EmbeddedChannel(new LWDecoder(), new LWEncoder());
        channel.attr(LWEncoder.SERIALIZE_KEY).set("json");
        channel.attr(LWEncoder.SERIALIZER_MANAGER_KEY).set(new SerizalizerManager());
        channel.attr(LWEncoder.COMPRESS_KEY).set("null");
        channel.attr(LWEncoder.COMPRESS_MANAGER_KEY).set(new CompressionManager());
        return channel;
    }
}
