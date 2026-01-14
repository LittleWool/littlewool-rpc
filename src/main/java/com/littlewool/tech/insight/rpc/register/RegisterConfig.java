package com.littlewool.tech.insight.rpc.register;

import lombok.Data;

/**
 * @ClassName: RegisterConfig
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/14 9:43
 * @Version: 1.0
 **/
@Data
public class RegisterConfig {
        private String registerType="zookeeper";

        private String connectString;
}
