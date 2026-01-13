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

    public static Response fail(String errorMessage){
        Response response=new Response();
        response.errorMessage=errorMessage;
        response.code=400;
        return response;
    }

    public static Response success(Object result){
        Response response=new Response();
        response.result=result;
        response.code=200;
        return response;
    }
}
