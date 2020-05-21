package com.zto.fire.demo.bean;

import com.zto.fire.common.util.FindClassUtils;
import com.zto.fire.common.util.PropUtils;

public class Local {

    public static void main(String[] args) throws Exception {
        String file = FindClassUtils.findFileInJar("test/LocalTest.properties");
        PropUtils.load(file);
        PropUtils.print();
    }
}
