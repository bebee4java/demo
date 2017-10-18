package com.java.hadoop.mapreduce.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class TQSort extends WritableComparator {

    //注意重写构造方法
    public TQSort(){
        super(WeatherRecord.class,true);
    }

    /**
     * @param a
     * @param b
     */
    @Override
    public int compare(WritableComparable a, WritableComparable b) {
        WeatherRecord wr1 = (WeatherRecord) a;
        WeatherRecord wr2 = (WeatherRecord) b;

        int c1 = Integer.compare(wr1.getYear(),wr2.getYear());
        if (c1 == 0){
            int c2 = Integer.compare(wr1.getMonth(),wr2.getMonth());
            if (c2 == 0){
                return -Double.compare(wr1.getWd(),wr2.getWd());//温度降序排序
            }
            return c2;
        }

        return c1;
    }
}
