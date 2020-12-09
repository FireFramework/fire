package com.zto.fire.common.bean.runtime;

import oshi.SystemInfo;
import oshi.hardware.Display;

/**
 * 用于封装显示器相关信息
 * @author ChengLong 2019年9月30日 13:36:16
 */
public class DisplayInfo {
    // 显示器描述信息
    private String display;

    public String getDisplay() {
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

        StringBuilder sb = new StringBuilder();
        if (displays != null && displays.length > 0) {
            for (Display display : displays) {
                sb.append(display);
            }
        }
        DisplayInfo displayInfo = new DisplayInfo();
        displayInfo.display = sb.toString();

        return displayInfo;
    }
}
