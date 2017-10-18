package com.java.hadoop.mapreduce.tq;

import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Created by sgr on 2017/10/18/018.
 */
public class TQGroup extends WritableComparator {
    public TQGroup(){
        super(WeatherRecord.class,true);
    }

    /**
     * 分组需要拿到同一年月的数据
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
            return c2;
        }

        return c1;
    }
}
