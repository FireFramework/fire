package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;
import oshi.SystemInfo;
import oshi.hardware.Display;

/**
 * 用于封装显示器相关信息
 * @author ChengLong 2019年9月30日 13:36:16
 */
public class DisplayInfo {
    // 显示器描述信息
    private StringBuilder display;

    public StringBuilder getDisplay() {
        return display;
    }

    private DisplayInfo() {
    }

    /**
     * 获取显示器信息
     */
    public static DisplayInfo getDisplayInfo() {
        SystemInfo systemInfo = new SystemInfo();
        Display[] displays = systemInfo.getHardware().getDisplays();
        DisplayInfo displayInfo = new DisplayInfo();
        if (displays != null && displays.length > 0) {
            for (Display display : displays) {
                displayInfo.display.append(display.toString());
            }
        }
        return displayInfo;
    }

    public static void main(String[] args) {
        System.out.println(JSON.toJSONString(getDisplayInfo()));
    }
}
