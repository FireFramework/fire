package com.zto.bigdata.spark.common.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.poi.xssf.usermodel.XSSFCell;
import org.apache.poi.xssf.usermodel.XSSFRow;
import org.apache.poi.xssf.usermodel.XSSFSheet;
import org.apache.poi.xssf.usermodel.XSSFWorkbook;

import java.io.*;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

/**
 * 文件操作工具类
 *
 * @author ChengLong 2018年8月22日 13:10:03
 */
public class FileUtils {
    private static final String FORMART_STR = "yyyy-MM-dd HH:mm:ss";
    private static DecimalFormat df = new DecimalFormat("#.########");


    /**
     * 写Excel文件
     *
     * @param header
     * @param body
     * @param filePath
     * @param fileName
     * @return
     * @throws Exception
     */
    public static String writeExcel(List<String> header,
                                    List<MyMap<String, String>> body, String filePath, String fileName) throws Exception {
        FileOutputStream fileOutputStream = null;

        try {
            String path = filePath + "/" + fileName + ".xlsx";
            File file = new File(filePath);
            // 如果文件夹不存在则创建
            if (!file.exists() && !file.isDirectory()) {
                file.mkdirs();
            }
            fileOutputStream = new FileOutputStream(path);
            XSSFWorkbook wb = new XSSFWorkbook();
            XSSFSheet childSheet = wb.createSheet();
            XSSFRow row = childSheet.createRow(0);
            for (int i = 0; i < header.size(); i++) {
                row.createCell(i).setCellValue(header.get(i).split("-")[0]);
            }
            for (int j = 0; j < body.size(); j++) {
                XSSFRow bodyRow = childSheet.createRow(j + 1);
                for (int k = 0; k < body.get(j).getMyKeys().size(); k++) {
                    String key = (String) body.get(j).getMyKeys().get(k);
                    String value = (String) body.get(j).get(key);
                    XSSFCell cell = bodyRow.createCell(k);
                    cell.setCellType(XSSFCell.CELL_TYPE_STRING);
                    cell.setCellValue(value);
                }
            }
            wb.write(fileOutputStream);
            fileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            body = null;
            header = null;
            if (fileOutputStream != null) {
                try {
                    fileOutputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            System.gc();
        }
        return fileName;
    }

    /**
     * 写CSV文件
     *
     * @param header
     * @param body
     * @param filePath
     * @param fileName
     * @param isFirst
     * @return
     * @throws Exception
     */
    public static String writeCsv(List<String> header,
                                  List<MyMap<String, String>> body, String filePath, String fileName,
                                  boolean isFirst) throws Exception {
        File csvFile = null;
        BufferedWriter csvFileOutputStream = null;
        FileOutputStream fos = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            // 定义文件名格式并创建
            String path = filePath + "/" + fileName + ".csv";
            csvFile = new File(path);
            // UTF-8使正确读取分隔符","
            fos = new FileOutputStream(csvFile, true);
            csvFileOutputStream = new BufferedWriter(new OutputStreamWriter(fos, "gbk"), 1024);
            // 写入文件头部
            if (isFirst) {
                for (int i = 0, length = header.size(); i < length; i++) {
                    String value = header.get(i).split("-")[0];
                    csvFileOutputStream.write(value);
                    if (i < length - 1) {
                        csvFileOutputStream.write(",");
                    }
                }
            }
            csvFileOutputStream.newLine();
            // 写入文件内容
            for (int j = 0, length = body.size(); j < length; j++) {
                for (int k = 0, lengthKey = body.get(j).getMyKeys().size(); k < lengthKey; k++) {
                    String key = (String) body.get(j).getMyKeys().get(k);
                    String value = (String) body.get(j).get(key);
                    if (value.indexOf(',') >= 0) {
                        value = value.replaceAll(",", "'，'");
                    }
                    csvFileOutputStream.write(value);
                    if (k < lengthKey - 1) {
                        csvFileOutputStream.write(",");
                    }
                }
                if (j < length - 1) {
                    csvFileOutputStream.newLine();
                }
            }
            csvFileOutputStream.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            try {
                if (null != csvFileOutputStream) {
                    csvFileOutputStream.close();
                }
                if (null != fos) {
                    fos.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return fileName;
    }

    /**
     * @param filePath file的路径
     * @param fileName file的名称
     * @param content  文件内容
     * @throws Exception
     */
    public static void writeFileToSvn(String filePath, String fileName,
                                      String content) throws IOException {
        OutputStreamWriter outPut = null;
        File sqlFile = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            // 定义文件名格式并创建
            String path = filePath + "/" + fileName + ".sql";
            sqlFile = new File(path);
            // UTF-8使正确读取分隔符","
            outPut = new OutputStreamWriter(new FileOutputStream(sqlFile),
                    "UTF-8");
            outPut.write(content);
            outPut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outPut != null) {
                outPut.close();
            }
        }
    }

    /**
     * @param filePath file的路径
     * @param fileName file的名称
     * @param content  文件内容
     * @throws Exception
     */
    public static void writeFile(String filePath, String fileName, String suffix, String content) throws IOException {
        OutputStreamWriter outPut = null;
        File sqlFile = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            // 定义文件名格式并创建
            String path = filePath + "/" + fileName + suffix;
            sqlFile = new File(path);
            // UTF-8使正确读取分隔符","
            outPut = new OutputStreamWriter(new FileOutputStream(sqlFile),
                    "UTF-8");
            outPut.write(content);
            outPut.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (outPut != null) {
                outPut.close();
            }
        }
    }

    /**
     * 读取svn文件的内容
     *
     * @param filePath 文件路径
     * @return
     * @throws Exception
     */
    public static String readSvnFile(String filePath) throws IOException {
        StringBuffer sb = new StringBuffer();
        InputStreamReader isr = null;
        try {
            isr = new InputStreamReader(new FileInputStream(filePath), "UTF-8");
            BufferedReader fileReader = new BufferedReader(isr);
            String line = "";
            while ((line = fileReader.readLine()) != null) {
                sb.append(line);
                sb.append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            if (isr != null) {
                isr.close();
            }
        }
        return sb.toString();
    }

    /**
     * @param filePath file的路径
     * @param fileName file的名称
     * @param content  文件内容
     * @throws Exception
     */
    public static void writeJsonFile(String filePath, String fileName,
                                     String content) throws Exception {
        OutputStreamWriter outPut = null;
        File sqlFile = null;
        try {
            File file = new File(filePath);
            if (!file.exists()) {
                file.mkdirs();
            }
            // 定义文件名格式并创建
            String path = filePath + "/" + fileName + ".json";
            sqlFile = new File(path);
            // UTF-8使正确读取分隔符","
            outPut = new OutputStreamWriter(new FileOutputStream(sqlFile),
                    "UTF-8");
            outPut.write(content);
            outPut.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (outPut != null) {
                outPut.close();
            }
        }
    }

    /**
     * 将字符串写入到指定的文件中
     *
     * @param file
     * @param str
     */
    public static void writeStr(File file, String str, boolean append) {
        FileOutputStream out = null;
        try {
            if (file == null) {
                throw new Exception("文件参数不合法");
            }
            if (file.isDirectory()) {
                throw new Exception(file.getAbsolutePath() + " 不能是目录");
            }
            if (!file.exists()) {
                FileUtils.mkDirs(file.getParent());
                file.createNewFile();
            }
            out = new FileOutputStream(file, append);
            StringBuffer sb = new StringBuffer(str);
            out.write(sb.toString().getBytes("utf-8"));
            out.flush();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            if (out != null) {
                try {
                    out.close();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        }
    }

    /**
     * 创建文件夹
     *
     * @param path
     */
    public static void mkDirs(String path) {
        if (org.apache.commons.lang3.StringUtils.isBlank(path)) {
            return;
        }
        FileUtils.mkDirs(new File(path));
    }

    /**
     * 创建文件夹
     *
     * @param path
     */
    public static void mkDirs(File path) {
        if (path == null) {
            return;
        }
        try {
            if (!path.exists()) {
                path.mkdirs();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    /**
     * 判断文件是否存在
     *
     * @param file
     * @return
     */
    public static boolean exists(String file) {
        if (StringUtils.isBlank(file)) {
            return false;
        }
        return new File(file).exists();
    }

    public static void main(String[] args) throws Exception {
        String value = ",abcefg";
        if (value.indexOf(',') > 0) {
            value = value.replaceAll(",", "'，'");
        }
        // List<String> header = new ArrayList<String>();
        // header.add("f1");
        // header.add("f2");
        //
        // List<MyMap<String, String>> body = new ArrayList<MyMap<String,
        // String>>();
        // MyMap<String, String> map = new MyMap<String, String>();
        // map.put("f1", "100");
        // map.put("f2", "中文");
        // body.add(0, map);
        // writeExcel(header, body, "e:/tmp");
        // System.out.println(103%200);
        // System.out.println(102/100);
        // System.out.println(NumberUtils.isNumber("111410303935L"));
        // NumberUtils.createLong("111410303935L");//exception
        // String uuid = UUID.randomUUID().toString();
        // File csvFile = File.createTempFile(uuid, ".csv", new
        // File("E:/V3/xls"));
        // System.out.println("csvFile：" + csvFile + ";uuid:" + uuid);
    }

    static class MyMap<T, A> extends HashMap implements Cloneable {
        private static final long serialVersionUID = 1L;

        private List my_keys = null;

        public List getMyKeys() {
            return my_keys;
        }

        @Override
        public Object put(Object key, Object value) {
            my_keys.add(key);
            return super.put(key, value);
        }

        public MyMap(int columnCount, int initialCapacity) {
            super(initialCapacity);
            my_keys = new ArrayList(columnCount);
        }
    }

    /**
     * 判断resource路径下的文件是否存在
     *
     * @param fileName 配置文件名称
     * @return null: 不存在，否则为存在
     */
    public static InputStream resourceFileExists(String fileName) {
        return FileUtils.class.getClassLoader().getResourceAsStream(fileName);
    }
}
