package com.littlewool.tech.insight.rpc.spi;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * @ClassName: Spi
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/2/1 20:28
 * @Version: 1.0
 **/

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Spi {
    String value();
    int code() default -1;
}
