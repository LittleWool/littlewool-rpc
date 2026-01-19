package com.littlewool.tech.insight.rpc.version;

/**
 * @ClassName: Version
 * @Description:
 * @Author: LittleWool
 * @Date: 2026/1/19 9:31
 * @Version: 1.0
 **/

public enum Version {
    V1(0);

    private final int versionNum;

    Version(int versionNum) {
        this.versionNum = versionNum;
    }

    public int getVersionNum() {
        return versionNum;
    }
}
