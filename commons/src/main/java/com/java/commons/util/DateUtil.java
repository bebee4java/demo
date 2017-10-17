package com.java.commons.util;

import org.apache.commons.lang3.time.FastDateFormat;

import java.util.Date;

/**
 * Created by sgr on 2017/10/17/017.
 */
public class DateUtil {

    public static String getCurrentTime(String pattern){
        FastDateFormat fastDateFormat = FastDateFormat.getInstance(pattern);
        return fastDateFormat.format(new Date());
    }

    public static String getFormattedTime(Date date,String pattern){
        FastDateFormat fastDateFormat = FastDateFormat.getInstance(pattern);
        return fastDateFormat.format(date);
    }

    /**
     * 返回当前时间的yyyy-MM-dd HH:mm:ss格式
     * @return String
     */
    public static String getStandardCurrentTime(){
        FastDateFormat fastDateFormat = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");
        return fastDateFormat.format(new Date());
    }
}
