package com.littlewool.tech.insight.rpc.provider;

import com.littlewool.tech.insight.rpc.api.Add;

/**
 * @ClassName: AddImpl
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/13 8:25
 * @Version: 1.0
 **/

public class AddImpl implements Add {
    @Override
    public int add(int a, int b) {
        return a+b;
    }

    private int privateAdd(int a, int b) {
        return a-b;
    }
}
