package com.littlewool.tech.insight.rpc.exception;

public class LimitException extends RpcException {
    public LimitException(String message) {
        super(message);
    }

    @Override
    public boolean retry() {
        return false;
    }
}
