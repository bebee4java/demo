package com.java.hadoop.mapreduce.fof;

import org.apache.commons.lang3.StringUtils;

/**
 * 格式化数据使（a-b b-a）统一成b-a
 * Created by sgr on 2017/10/18.
 */
public class FOF {
    public static String format(String f1,String f2){
        int c = f1.compareTo(f2);
        if (c < 0){
            return StringUtils.join(f2,"-",f1);
        }
        return StringUtils.join(f1,"-",f2);
    }

    public static void main(String[] args) {
        System.out.println(FOF.format("a","b"));
        System.out.println(FOF.format("b","a"));
    }
}
