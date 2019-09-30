package com.zto.fire.common.bean.runtime;

import com.alibaba.fastjson.JSON;
import oshi.SystemInfo;
import oshi.hardware.UsbDevice;

import java.util.LinkedList;
import java.util.List;

/**
 * 用于封装usb设备信息
 * @author ChengLong 2019年9月30日 13:33:35
 */
public class UsbInfo {
    // usb 设备名称
    private String name;
    // usb设备id
    private String productId;
    // usb设备制造商
    private String vendor;
    // usb设备制造商id
    private String vendorId;
    // usb设备序列号
    private String serialNumber;

    public String getName() {
        return name;
    }

    public String getProductId() {
        return productId;
    }

    public String getVendor() {
        return vendor;
    }

    public String getVendorId() {
        return vendorId;
    }

    public String getSerialNumber() {
        return serialNumber;
    }

    private UsbInfo() {}

    public UsbInfo(String name, String productId, String vendor, String vendorId, String serialNumber) {
        this.name = name;
        this.productId = productId;
        this.vendor = vendor;
        this.vendorId = vendorId;
        this.serialNumber = serialNumber;
    }

    /**
     * 获取usb社保信息
     */
    public static List<UsbInfo> getUsbInfo() {
        SystemInfo systemInfo = new SystemInfo();
        UsbDevice[] usbDevices = systemInfo.getHardware().getUsbDevices(true);
        List<UsbInfo> usbInfoList = new LinkedList<>();
        if (usbDevices != null && usbDevices.length > 0) {
            for (UsbDevice usbDevice : usbDevices) {
                UsbInfo usbInfo = new UsbInfo(usbDevice.getName(), usbDevice.getProductId(), usbDevice.getVendor(), usbDevice.getVendorId(), usbDevice.getSerialNumber());
                usbInfoList.add(usbInfo);
            }
        }
        return usbInfoList;
    }

    public static void main(String[] args) {
        System.out.println(JSON.toJSONString(getUsbInfo()));
    }
}
