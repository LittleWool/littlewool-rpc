package com.littlewool.tech.insight.rpc.message;

import lombok.Data;

/**
 * @ClassName: Response
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/12 20:03
 * @Version: 1.0
 **/
@Data
public class Response {

    Object result;

    int code;

    String errorMessage;

    private int requestId;
    public static Response fail(String errorMessage,int requestId){
        Response response=new Response();
        response.errorMessage=errorMessage;
        response.code=400;
        response.requestId=requestId;
        return response;
    }

    public static Response success(Object result,int requestId){
        Response response=new Response();
        response.result=result;
        response.code=200;
        response.requestId=requestId;

        return response;
    }
}
