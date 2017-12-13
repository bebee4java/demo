package com.java.commons.util;

import java.io.File;
import java.io.PrintWriter;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.List;

/**
 * Created by sgr on 2017/12/14.
 */
public class FileUtils {
    public static boolean writeFile(List<Object[]> rows, String fileName, String separator, String charset) {
        // 标记文件生成是否成功
        boolean flag = true;
        try {
            File file = new File(fileName);
            if (file.exists()) { // 如果已存在,删除旧文件
                file.delete();
            }
            file.createNewFile();

            // 格式化浮点数据
            NumberFormat formatter = NumberFormat.getNumberInstance();
            formatter.setMaximumFractionDigits(10); // 设置最大小数位为10

            // 遍历输出每行
            PrintWriter pfp = new PrintWriter(file, charset); //设置输出文件的编码
            for (Object[] rowData : rows) {
                StringBuffer thisLine = new StringBuffer("");
                for (int i = 0; i < rowData.length; i++) {
                    Object obj = rowData[i]; // 当前字段

                    // 格式化数据
                    String field = "";
                    if (null != obj) {
                        if (obj.getClass() == String.class) { // 如果是字符串
                            field = (String) obj;
                        } else if (obj.getClass() == Double.class || obj.getClass() == Float.class) { // 如果是浮点型
                            field = formatter.format(obj); // 格式化浮点数,使浮点数不以科学计数法输出
                        } else if (obj.getClass() == Integer.class || obj.getClass() == Long.class
                                || obj.getClass() == Short.class || obj.getClass() == Byte.class) { // 如果是整形
                            field += obj;
                        }
                    } else {
                        field = " "; // null时给一个空格占位
                    }

                    // 拼接所有字段为一行数据，用separator分隔
                    if (i < rowData.length - 1) { // 不是最后一个元素
                        thisLine.append(field).append(separator);
                    } else { // 是最后一个元素
                        thisLine.append(field);
                    }
                }
                pfp.print(thisLine.toString() + "\n");
            }
            pfp.close();

        } catch (Exception e) {
            flag = false;
            e.printStackTrace();
        }
        return flag;
    }
}
