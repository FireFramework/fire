package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;
import oshi.SystemInfo;
import oshi.hardware.ComputerSystem;
import oshi.hardware.HardwareAbstractionLayer;

/**
 * 硬件信息封装类
 *
 * @author ChengLong 2019年9月29日 15:52:50
 */
public class HardwareInfo {
    private static HardwareInfo hardwareInfo;
    // 制造商
    private String manufacturer;
    // 型号
    private String model;
    // 序列号
    private String serialNumber;

    public String getManufacturer() {
        return manufacturer;
    }

    public String getModel() {
        return model;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    private HardwareInfo() {
    }

    /**
     * 获取硬件设备信息
     */
    public static HardwareInfo getHardwareInfo() {
        if (hardwareInfo == null) {
            hardwareInfo = new HardwareInfo();
            SystemInfo systemInfo = new SystemInfo();
            HardwareAbstractionLayer hardware = systemInfo.getHardware();
            ComputerSystem computerSystem = hardware.getComputerSystem();
            hardwareInfo.manufacturer = computerSystem.getManufacturer();
            hardwareInfo.model = computerSystem.getModel();
            hardwareInfo.serialNumber = computerSystem.getSerialNumber();
        }

        return hardwareInfo;
    }

    public static void main(String[] args) {
        System.out.println(JSON.toJSONString(getHardwareInfo()));
    }
}
